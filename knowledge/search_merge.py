import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from knowledge import client, CERTAINTY
from embedding_model.qwen3 import get_embedding
from collections import defaultdict

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

router = APIRouter()

# 请求体模型
class MergeQueryRequest(BaseModel):
    user_ids: str
    query_text: str
    limit: int = 5

class ContextQueryRequest(BaseModel):
    user_ids: str
    query_text: str
    limit: int = 5
    context_size: int = 1


# 构造 where 过滤器：用户ID
def build_userid_filter(user_ids: str):
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


# 构造 where 过滤器：file_id
def build_fileid_filter(file_ids):
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


# 安全获取查询结果
def safe_get_paragraphs(result):
    return result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", []) or []


# 查询并拼接全文（按 file_id 分组）
def search_and_merge_full_file(query_text: str, user_ids: str, limit: int = 5):
    logger.info(f"Start merging by file. query_text='{query_text}', user_ids='{user_ids}', limit={limit}")
    embedding = get_embedding(query_text)
    where_filter = build_userid_filter(user_ids)

    logger.debug("Performing vector search...")
    vector_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id"])
        .with_near_vector({"vector": embedding, "certainty": CERTAINTY})
        .with_limit(limit)
        .with_additional(["distance"])
        .with_where(where_filter)
        .do()
    )
    logger.debug(f"vector_result: {vector_result}")
    vector_hits = safe_get_paragraphs(vector_result)
    file_ids = list({hit.get("file_id") for hit in vector_hits if hit.get("file_id")})
    logger.info(f"Matched file_ids: {file_ids}")

    if not file_ids:
        logger.info("No matching file_ids found.")
        return []

    logger.debug("Fetching paragraphs for matched file_ids...")
    full_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_cntt", "part_sort"])
        .with_where(build_fileid_filter(file_ids))
        .with_limit(1000)
        .do()
    )
    logger.debug(f"full_result: {full_result}")
    all_parts = safe_get_paragraphs(full_result)
    logger.info(f"Retrieved {len(all_parts)} paragraph parts.")

    grouped = defaultdict(list)
    for item in all_parts:
        grouped[item["file_id"]].append({
            "part_sort": item["part_sort"],
            "part_cntt": item["part_cntt"]
        })

    results = []
    for file_id in file_ids:
        parts = sorted(grouped[file_id], key=lambda x: x["part_sort"])
        merged_text = ''.join([p["part_cntt"] for p in parts])
        logger.info(f"Merged file_id={file_id}, parts_count={len(parts)}")
        results.append({
            "file_id": file_id,
            "merged_text": merged_text,
            "parts": parts
        })

    return results


@router.post("/search_merge_by_file")
async def search_merge_by_file(request: MergeQueryRequest):
    try:
        merged = search_and_merge_full_file(
            request.query_text,
            request.user_ids,
            request.limit
        )
        return {"file_count": len(merged), "results": merged}
    except Exception as e:
        logger.exception("Exception in /search_merge_by_file")
        raise HTTPException(status_code=500, detail=str(e))


# 查询并拼接上下文（按段落匹配）
def handle_contextual_search(query_text: str, user_ids: str, limit: int, context_size: int):
    logger.info(f"Start contextual search. query_text='{query_text}', user_ids='{user_ids}', limit={limit}, context_size={context_size}")
    embedding = get_embedding(query_text)
    where_filter = build_userid_filter(user_ids)

    logger.debug("Performing vector search for context...")
    vector_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_sort"])
        .with_near_vector({"vector": embedding, "certainty": CERTAINTY})
        .with_limit(limit)
        .with_additional(["distance"])
        .with_where(where_filter)
        .do()
    )
    logger.debug(f"vector_result: {vector_result}")
    hits = safe_get_paragraphs(vector_result)
    if not hits:
        logger.info("No vector matches found.")
        return {"match_count": 0, "results": []}

    match_map = defaultdict(set)
    for item in hits:
        match_map[item["file_id"]].add(item["part_sort"])
    logger.info(f"Matched file_id -> part_sort: {dict(match_map)}")

    file_ids = list(match_map.keys())
    logger.debug("Fetching all paragraphs for matched file_ids...")
    full_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_sort", "part_cntt"])
        .with_where(build_fileid_filter(file_ids))
        .with_limit(1000)
        .do()
    )
    logger.debug(f"full_result: {full_result}")
    all_parts = safe_get_paragraphs(full_result)
    logger.info(f"Retrieved {len(all_parts)} paragraph parts.")

    file_part_map = defaultdict(list)
    for item in all_parts:
        file_part_map[item["file_id"]].append({
            "part_sort": item["part_sort"],
            "part_cntt": item["part_cntt"]
        })

    results = []
    for file_id, matched_sorts in match_map.items():
        parts = sorted(file_part_map[file_id], key=lambda x: x["part_sort"])
        sort_index = {p["part_sort"]: i for i, p in enumerate(parts)}

        for matched_sort in matched_sorts:
            idx = sort_index.get(matched_sort)
            if idx is None:
                continue
            start = max(0, idx - context_size)
            end = min(len(parts), idx + context_size + 1)
            context_parts = parts[start:end]
            context_text = ''.join([p["part_cntt"] for p in context_parts])

            results.append({
                "file_id": file_id,
                "matched_part_sort": matched_sort,
                "context_range": [parts[start]["part_sort"], parts[end - 1]["part_sort"]],
                "context_text": context_text,
                "parts": context_parts
            })
        logger.info(f"Context extracted for file_id={file_id}, matched_sorts={matched_sorts}")

    return {"match_count": len(results), "results": results}


@router.post("/search_with_context")
async def search_with_context(request: ContextQueryRequest):
    try:
        return handle_contextual_search(
            query_text=request.query_text,
            user_ids=request.user_ids,
            limit=request.limit,
            context_size=request.context_size
        )
    except Exception as e:
        logger.exception("Exception in /search_with_context")
        raise HTTPException(status_code=500, detail=str(e))
