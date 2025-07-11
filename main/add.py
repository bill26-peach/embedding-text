import weaviate  # type: ignore
from weaviate.auth import AuthApiKey
from qwen3 import get_embedding  # ✅ 从另一个文件导入

if __name__ == "__main__":
    # ✅ 认证配置
    auth_config = AuthApiKey("WVF5YThaHlkYwhGUSmCRgsX3tD5ngdN8pkih")

    # ✅ 禁用 grpc（可选）
    weaviate.connect.connection.has_grpc = False

    # ✅ 创建客户端
    client = weaviate.Client(
        url="http://localhost:8080",
        auth_client_secret=auth_config,
        timeout_config=(5, 60),
        startup_period=None
    )

    # ✅ 配置批量导入
    client.batch.configure(
        batch_size=100,
        dynamic=True,
        timeout_retries=3,
    )

    # ✅ 检查服务是否 ready
    print("Weaviate ready:", client.is_ready())

    # ✅ 定义 schema
    schema = {
        "class": "MyClass2",
        "vectorizer": "none",
        "properties": [
            {
                "name": "text",
                "dataType": ["text"],
            }
        ],
    }

    # ⚠️ 如果已有，先删除
    try:
        client.schema.delete_class("MyClass2")
    except:
        pass

    # ✅ 创建 schema
    client.schema.create_class(schema)
    print("Schema created ✅")

    # ✅ 准备文本
    text_data = [
        "AI will change the world.",
        "Education needs more AI empowerment.",
        "Weaviate is a vector database."
    ]

    # ✅ 生成所有向量
    vector_data = [get_embedding(text) for text in text_data]

    # ✅ 批量写入
    with client.batch as batch:
        for i, text in enumerate(text_data):
            properties = {"text": text}
            batch.add_data_object(
                data_object=properties,
                class_name="MyClass2",
                vector=vector_data[i]
            )

    print("Data inserted ✅")

    # ✅ 查询示例
    query_text = "Find documents about AI education."
    query_vector = get_embedding(query_text)

    result = client.query.get("MyClass2", ["text"]) \
        .with_near_vector({"vector": query_vector}) \
        .with_limit(3) \
        .do()

    print("查询结果 ✅")
    for item in result["data"]["Get"]["MyClass2"]:
        print("-", item["text"])
