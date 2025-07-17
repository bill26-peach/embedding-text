from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from knowledge import client  # ✅ 引入 Weaviate client（knowledge/__init__.py）
from embedding_model.qwen3 import get_embedding

# 创建 Router
router = APIRouter()

# 请求体模型
class QueryRequest(BaseModel):
    user_ids: str            # 多个 user_id，用逗号分隔
    query_text: str
    limit: int = 3           # 可选，默认返回 3 条结果

# 向量搜索函数
def search_vector_in_db(query_text: str, user_ids: str, limit: int = 3):
    embedding = get_embedding(query_text)

    # 拆分 user_ids 为列表
    user_id_list = [uid.strip() for uid in user_ids.split(",") if uid.strip()]

    # 构建 Weaviate where 过滤条件
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

    # 查询向量数据库
    result = (
        client.query
        .get("KnowledgeParagraph",
             ["part_id", "knowledge_id", "file_id", "userid", "username",
              "digital_human_id", "part_cntt", "part_sort"])
        .with_near_vector({"vector": embedding})
        .with_limit(limit)
        .with_additional(["distance", "id"])
        .with_where(where_filter)
        .do()
    )

    # 处理查询结果
    hits = result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])
    search_results = []
    for item in hits:
        search_results.append({
            "part_id": item.get("part_id"),
            "knowledge_id": item.get("knowledge_id"),
            "file_id": item.get("file_id"),
            "userid": item.get("userid"),
            "username": item.get("username"),
            "digital_human_id": item.get("digital_human_id"),
            "part_cntt": item.get("part_cntt"),
            "part_sort": item.get("part_sort"),
            "distance": round(item["_additional"]["distance"], 4),
            "id": item["_additional"]["id"],
        })

    return search_results

# API 路由
@router.post("/search_vector")
async def search_vector(request: QueryRequest):
    try:
        search_results = search_vector_in_db(
            request.query_text,
            request.user_ids,
            request.limit
        )
        part_num = len(search_results)

        return {
            "user_ids": request.user_ids,
            "part_num": part_num,
            "results": search_results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
