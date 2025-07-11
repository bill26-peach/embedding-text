import weaviate
from weaviate.auth import AuthApiKey
from qwen3 import get_embedding  # ✅ 从另一个文件导入（qwen3.py）

# ✅ 认证配置
auth_config = AuthApiKey("WVF5YThaHlkYwhGUSmCRgsX3tD5ngdN8pkih")
weaviate.connect.connection.has_grpc = False

client = weaviate.Client(
    url="http://localhost:8080",
    auth_client_secret=auth_config,
    timeout_config=(5, 60),
    startup_period=None
)

if __name__ == "__main__":
    # ✅ 查询文本
    query_text = "ai 是什么？"
    embedding = get_embedding(query_text)

    # ✅ 执行向量检索
    result = (
        client.query
        .get("MyClass2", ["text"])                  # 👈 返回字段
        .with_near_vector({"vector": embedding})    # 👈 用 embedding 查询
        .with_limit(3)                              # 👈 返回最多 3 条
        .with_additional(["distance", "id"])        # 👈 返回距离 & id
        .do()
    )

    # ✅ 格式化打印结果
    print("查询结果 ✅")
    hits = result.get("data", {}).get("Get", {}).get("MyClass2", [])
    for item in hits:
        text = item["text"]
        distance = item["_additional"]["distance"]
        obj_id = item["_additional"]["id"]
        print(f"- 内容: {text}\n  距离: {distance:.4f}\n  ID: {obj_id}\n")
