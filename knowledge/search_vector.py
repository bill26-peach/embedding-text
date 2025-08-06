import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from knowledge import client, CERTAINTY
from embedding_model.qwen3 import get_embedding

# ✅ 日志配置（如已在主模块设置过，可省略此配置）
logging.basicConfig(
    level=logging.INFO,  # 可改为 DEBUG 查看更详细信息
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 创建 Router
router = APIRouter()

# 请求体模型
class QueryRequest(BaseModel):
    user_ids: str            # 多个 user_id，用逗号分隔
    query_text: str
    limit: int = 3           # 可选，默认返回 3 条结果


# 向量搜索函数
def search_vector_in_db(query_text: str, user_ids: str, limit: int = 3):
    logger.info(f"Start vector search. query_text='{query_text}', user_ids='{user_ids}', limit={limit}")
    embedding = get_embedding(query_text)
    logger.debug(f"Generated embedding vector of length: {len(embedding)}")

    # 拆分 user_ids 为列表
    user_id_list = [uid.strip() for uid in user_ids.split(",") if uid.strip()]
    logger.debug(f"Parsed user_id_list: {user_id_list}")

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
                }
                for uid in user_id_list
            ]
        }

    logger.debug(f"Constructed where_filter: {where_filter}")

    # 查询向量数据库
    try:
        result = (
            client.query
            .get("KnowledgeParagraph",
                 ["part_id", "knowledge_id", "file_id", "userid", "username",
                  "digital_human_id", "part_cntt", "part_sort"])
            .with_near_vector({"vector": embedding, "certainty": CERTAINTY})
            .with_limit(limit)
            .with_additional(["distance", "id"])
            .with_where(where_filter)
            .do()
        )
    except Exception as e:
        logger.exception("Weaviate query execution failed.")
        raise

    hits = result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])
    logger.info(f"Vector search completed. Retrieved {len(hits)} results.")

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

    logger.debug(f"Final search result: {search_results}")
    return search_results


# API 路由
@router.post("/search_vector")
async def search_vector(request: QueryRequest):
    try:
        logger.info(f"API /search_vector called with: {request}")
        search_results = search_vector_in_db(
            request.query_text,
            request.user_ids,
            request.limit
        )
        part_num = len(search_results)
        logger.info(f"Returning {part_num} result(s) for user_ids='{request.user_ids}'")

        return {
            "user_ids": request.user_ids,
            "part_num": part_num,
            "results": search_results
        }
    except Exception as e:
        logger.exception("Exception occurred in /search_vector endpoint")
        raise HTTPException(status_code=500, detail=str(e))
