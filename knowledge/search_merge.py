from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from knowledge import client
from embedding_model.qwen3 import get_embedding
from collections import defaultdict

router = APIRouter()

class MergeQueryRequest(BaseModel):
    user_ids: str
    query_text: str
    limit: int = 5  # 匹配 file_id 数量限制

class ContextQueryRequest(BaseModel):
    user_ids: str
    query_text: str
    limit: int = 5
    context_size: int = 1  # 匹配段落上下文的窗口大小

def search_and_merge_full_file(query_text: str, user_ids: str, limit: int = 5):
    embedding = get_embedding(query_text)
    user_id_list = [uid.strip() for uid in user_ids.split(",") if uid.strip()]

    # 构造 where 过滤
    if len(user_id_list) == 1:
        where_filter = {
            "path": ["userid"],
            "operator": "Equal",
            "valueString": user_id_list[0]
        }
    else:
        where_filter = {
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

    # 第一步：向量查询匹配的 file_id
    vector_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id"])
        # .with_near_vector({"vector": embedding, "distance": 0.4})  # 越小越近
        .with_near_vector({"vector": embedding, "certainty": 0.75}) # 越高越相似
        .with_limit(limit)
        .with_additional(["distance"])
        .with_where(where_filter)
        .do()
    )
    vector_hits = vector_result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])
    file_ids = list(set(hit["file_id"] for hit in vector_hits))

    if not file_ids:
        return []

    # 第二步：查询这些 file_id 的所有段落（不管是否命中），并按 part_sort 升序
    full_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_cntt", "part_sort"])
        .with_where({
            "path": ["file_id"],
            "operator": "ContainsAny",
            "valueTextArray": file_ids
        })
        .with_limit(1000)  # 每次最多返回1000条
        .do()
    )
    all_parts = full_result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])

    # 根据 file_id 聚合并拼接
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
        return {
            "file_count": len(merged),
            "results": merged
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))




def handle_contextual_search(query_text: str, user_ids: str, limit: int, context_size: int):
    embedding = get_embedding(query_text)
    user_id_list = [uid.strip() for uid in user_ids.split(",") if uid.strip()]

    # 构建 where 条件
    if len(user_id_list) == 1:
        where_filter = {
            "path": ["userid"],
            "operator": "Equal",
            "valueString": user_id_list[0]
        }
    else:
        where_filter = {
            "operator": "Or",
            "operands": [
                {
                    "path": ["userid"],
                    "operator": "Equal",
                    "valueString": uid
                } for uid in user_id_list
            ]
        }

    # 向量匹配
    vector_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_sort"])
        .with_near_vector({"vector": embedding})
        .with_limit(limit)
        .with_additional(["distance"])
        .with_where(where_filter)
        .do()
    )
    hits = vector_result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])
    if not hits:
        return {"match_count": 0, "results": []}

    # 整理匹配 file_id -> set(part_sort)
    match_map = defaultdict(set)
    for item in hits:
        match_map[item["file_id"]].add(item["part_sort"])

    # 查询 file_id 下所有段落（拼接上下文）
    file_ids = list(match_map.keys())
    full_result = (
        client.query
        .get("KnowledgeParagraph", ["file_id", "part_sort", "part_cntt"])
        .with_where({
            "path": ["file_id"],
            "operator": "ContainsAny",
            "valueTextArray": file_ids
        })
        .with_limit(1000)
        .do()
    )
    all_parts = full_result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])

    # 按 file_id 分组
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

    return {
        "match_count": len(results),
        "results": results
    }

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
        raise HTTPException(status_code=500, detail=str(e))