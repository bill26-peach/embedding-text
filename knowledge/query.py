from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from knowledge import client  # ✅ 从 knowledge/__init__.py 导入 Weaviate client
from embedding_model.qwen3 import get_embedding

# 创建 Router
router = APIRouter()

# 请求体定义
class QueryRequest(BaseModel):
    user_id: str
    query_text: str

# 向量搜索函数
def search_vector_in_db(query_text: str, user_id: str):
    embedding = get_embedding(query_text)

    # 执行向量检索，查询 Kafka 消费写入的 KnowledgeParagraph 类
    result = (
        client.query
        .get("KnowledgeParagraph",
             ["part_id", "knowledge_id", "file_id", "userid", "username", "digital_human_id", "part_cntt", "part_sort"])
        .with_near_vector({"vector": embedding})
        .with_limit(3)
        .with_additional(["distance", "id"])
        .with_where({
            "path": ["digital_human_id"],
            "operator": "Equal",
            "valueString": user_id
        })
        .do()
    )

    hits = result.get("data", {}).get("Get", {}).get("KnowledgeParagraph", [])
    search_results = []
    for item in hits:
        part_id = item.get("part_id")
        knowledge_id = item.get("knowledge_id")
        file_id = item.get("file_id")
        username = item.get("username")
        digital_human_id = item.get("digital_human_id")
        part_cntt = item.get("part_cntt")
        part_sort = item.get("part_sort")
        distance = item["_additional"]["distance"]
        obj_id = item["_additional"]["id"]

        search_results.append({
            "part_id": part_id,
            "knowledge_id": knowledge_id,
            "file_id": file_id,
            "userid": user_id,
            "username": username,
            "digital_human_id": digital_human_id,
            "part_cntt": part_cntt,
            "part_sort": part_sort,
            "distance": round(distance, 4),
            "id": obj_id,
        })

    return search_results

# API 路由
@router.post("/search_vector")
async def search_vector(request: QueryRequest):
    try:
        search_results = search_vector_in_db(request.query_text, request.user_id)
        part_num = len(search_results)  # ✅ 返回段落条数

        return {
            "user_id": request.user_id,
            "part_num": part_num,
            "results": search_results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
