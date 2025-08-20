import logging
from collections import defaultdict
from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from knowledge import client, CERTAINTY
from embedding_model.qwen3 import get_embedding

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

router = APIRouter()

# ====================
# 请求体模型 + 校验
# ====================
class MergeQueryRequest(BaseModel):
    user_ids: str
    query_text: str
    limit: int = Field(5, ge=1, le=5000)
    vector: bool = True  # 新增，默认为 True

class ContextQueryRequest(BaseModel):
    user_ids: str
    query_text: str
    limit: int = Field(5, ge=1, le=50)
    context_size: int = Field(1, ge=0, le=5)


# ====================
# where 过滤器构造
# ====================
def build_userid_filter(user_ids: str):
    """构造 where 过滤器：userid"""
    user_id_list = [uid.strip() for uid in user_ids.split(",") if uid.strip()]
    if not user_id_list:
        raise ValueError("user_ids 不能为空")

    if len(user_id_list) == 1:
        return {
            "path": ["userid"],
            "operator": "Equal",
            "valueString": user_id_list[0]
        }
    return {
        "operator": "Or",
        "operands": [
            {
                "path": ["userid"],
                "operator": "Equal",
                "valueString": uid
            }
            for uid in user_id_list
        ]
    }

def build_fileid_filter(file_ids):
    """构造 where 过滤器：file_id"""
    if not file_ids:
        return {"path": ["file_id"], "operator": "Equal", "valueString": "___empty___"}
    return {
        "operator": "Or",
        "operands": [
            {
                "path": ["file_id"],
                "operator": "Equal",
                "valueString": fid
            }
            for fid in file_ids
        ]
    }

def combine_and(a, b):
    """组合 AND 条件"""
    return {"operator": "And", "operands": [a, b]}


# ====================
# 工具方法
# ====================
def safe_get_paragraphs(result):
    """安全获取查询结果中的段落列表"""
    return result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", []) or []

def ordered_unique(seq):
    """按顺序去重"""
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


# ====================
# 全文合并检索（返回 file_id, user_id, context）
# ====================
def search_and_merge_full_file(query_text: str, user_ids: str, limit: int = 5, vector: bool = True):
    logger.info(f"[merge_by_file] vector={vector}, uids='{user_ids}', limit={limit}")
    uid_where = build_userid_filter(user_ids)

    if vector:
        # 原来的向量检索逻辑
        embedding = get_embedding(query_text)
        vector_result = (
            client.query
            .get("KnowledgeParagraph", ["file_id"])
            .with_near_vector({"vector": embedding, "minCertainty": CERTAINTY})
            .with_additional(["certainty"])
            .with_limit(limit)
            .with_where(uid_where)
            .do()
        )
        vector_hits = safe_get_paragraphs(vector_result)
        # 打印 certainty
        for h in vector_hits:
            fid = h.get("file_id")
            c = (h.get("_additional") or {}).get("certainty")
            logger.info(f"[merge_by_file] hit file_id={fid}, certainty={c}")
        file_ids = ordered_unique([h.get("file_id") for h in vector_hits if h.get("file_id")])
    else:
        # 新的非向量模式
        file_ids = list_files_by_users(user_ids, limit)

    if not file_ids:
        logger.info("[merge_by_file] 无匹配文件")
        return []

    # 全文拉取并按 file_id 合并
    where_all = combine_and(uid_where, build_fileid_filter(file_ids))
    full_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "userid", "part_cntt", "part_sort"])
        .with_where(where_all)
        .with_limit(5000)
        .do()
    )
    all_parts = safe_get_paragraphs(full_result)

    grouped = defaultdict(list)
    file_user_map = {}
    for item in all_parts:
        fid = item.get("file_id")
        uid = item.get("userid")
        if fid and uid and fid not in file_user_map:
            file_user_map[fid] = uid
        if item.get("part_sort") is None or item.get("part_cntt") is None:
            continue
        grouped[fid].append({"part_sort": item["part_sort"], "part_cntt": item["part_cntt"]})

    results = []
    for fid in file_ids:
        parts = sorted(grouped.get(fid, []), key=lambda x: x["part_sort"])
        merged_text = '\n'.join(p["part_cntt"] for p in parts)
        results.append({
            "file_id": fid,
            "user_id": file_user_map.get(fid),
            "context": merged_text
        })

    return results

MAX_SCAN = 5000  # 单批扫描最大段落数

def list_files_by_users(user_ids: str, limit: int):
    """按账号列出文件ID列表"""
    uid_where = build_userid_filter(user_ids)
    scan = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_sort"])
        .with_where(uid_where)
        .with_limit(MAX_SCAN)  # 如果可能超量，需要分页
        .do()
    )
    rows = safe_get_paragraphs(scan)
    file_ids = ordered_unique([r.get("file_id") for r in rows if r.get("file_id")])
    return file_ids[:limit]

@router.post("/search_merge_by_file")
async def search_merge_by_file(request: MergeQueryRequest):
    try:
        results = await run_in_threadpool(
            search_and_merge_full_file,
            request.query_text,
            request.user_ids,
            request.limit,
            request.vector  # 新参数
        )
        return results
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception:
        logger.exception("Exception in /search_merge_by_file")
        raise HTTPException(status_code=500, detail="internal error")



# ====================
# 上下文检索（返回 file_id, user_id, context）
# ====================
def handle_contextual_search(query_text: str, user_ids: str, limit: int, context_size: int):
    logger.info(f"[context_search] query_text='{query_text[:50]}...', user_ids='{user_ids}', limit={limit}, context_size={context_size}")
    embedding = get_embedding(query_text)
    uid_where = build_userid_filter(user_ids)

    # 向量检索：拿到 file_id + 命中的 part_sort
    vector_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_sort"])
        .with_near_vector({"vector": embedding, "minCertainty": CERTAINTY})
        .with_additional(["certainty"])
        .with_limit(limit)
        .with_where(uid_where)
        .do()
    )
    hits = safe_get_paragraphs(vector_result)
    if not hits:
        logger.info("[context_search] 无匹配结果")
        return []

    for h in hits:
        fid = h.get("file_id")
        ps = h.get("part_sort")
        c = (h.get("_additional") or {}).get("certainty")
        logger.info(f"[context_search] hit file_id={fid}, part_sort={ps}, certainty={c}")

    match_map = defaultdict(set)
    for item in hits:
        if item.get("file_id") and item.get("part_sort") is not None:
            match_map[item["file_id"]].add(item["part_sort"])

    file_ids = ordered_unique(match_map.keys())

    # 拉取全文（带 user_id 限定），取 userid 用于返回
    where_all = combine_and(uid_where, build_fileid_filter(file_ids))
    full_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "userid", "part_sort", "part_cntt"])
        .with_where(where_all)
        .with_limit(1000)
        .do()
    )
    all_parts = safe_get_paragraphs(full_result)

    file_part_map = defaultdict(list)
    file_user_map = {}
    for item in all_parts:
        fid = item.get("file_id")
        uid = item.get("userid")
        if fid and uid and fid not in file_user_map:
            file_user_map[fid] = uid
        if item.get("part_sort") is None or item.get("part_cntt") is None:
            continue
        file_part_map[fid].append({
            "part_sort": item["part_sort"],
            "part_cntt": item["part_cntt"]
        })

    results = []
    for file_id, matched_sorts in match_map.items():
        parts = sorted(file_part_map.get(file_id, []), key=lambda x: x["part_sort"])
        if not parts:
            continue
        sort_index = {p["part_sort"]: i for i, p in enumerate(parts)}

        # 对每个命中位置输出一个 context（如需去重或合并相邻范围，可再优化）
        for matched_sort in matched_sorts:
            idx = sort_index.get(matched_sort)
            if idx is None:
                continue
            start = max(0, idx - context_size)
            end = min(len(parts), idx + context_size + 1)
            context_parts = parts[start:end]
            context_text = '\n'.join(p["part_cntt"] for p in context_parts)

            results.append({
                "file_id": file_id,
                "user_id": file_user_map.get(file_id),
                "context": context_text
            })

    return results


@router.post("/search_with_context")
async def search_with_context(request: ContextQueryRequest):
    try:
        results = await run_in_threadpool(
            handle_contextual_search,
            request.query_text,
            request.user_ids,
            request.limit,
            request.context_size
        )
        # 仅返回需要的字段
        return results
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))  # ←
    except Exception:
        logger.exception("Exception in /search_with_context")
        raise HTTPException(status_code=500, detail="internal error")
