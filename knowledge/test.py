from knowledge import client  # ✅ 从 knowledge/__init__.py 导入 Weaviate client

get = client.schema.get()
# client.schema.delete_class("KnowledgeParagraph")
full_result = (client.query.get("KnowledgeParagraph",
                      ["part_id", "knowledge_id", "file_id", "userid", "username", "digital_human_id", "part_cntt",
                       "part_sort"])
                      .with_additional(["distance", "id","vector"]).do())
print(get)
print(full_result)
