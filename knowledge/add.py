from knowledge import client, redis_client, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC
from embedding_model.qwen3 import get_embedding
import weaviate  # type: ignore
from confluent_kafka import Consumer
import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
import threading

cache_lock = threading.Lock()

def md5_text(text: str) -> str:
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def split_text_to_paragraphs(text: str, max_len: int = 500) -> list[str]:
    paragraphs = []
    temp = ""
    for line in text.split("\n"):
        if not line.strip():
            continue
        temp += line.strip() + " "
        if len(temp) >= max_len:
            paragraphs.append(temp.strip())
            temp = ""
    if temp:
        paragraphs.append(temp.strip())
    return paragraphs

def process_and_insert(paragraph: str, meta: dict, part_sort: int):
    # 同一个文本id下的片段不需要再重复写入
    md5_value = md5_text(paragraph)
    file_id = meta.get("file_id")
    cache_key = f"paragraph_md5_cache:{file_id}"

    with cache_lock:
        if redis_client.sismember(cache_key, md5_value):
            print(paragraph)
            print(f"🔁 已存在，file_id={file_id}, 跳过: {md5_value}")
            return
        redis_client.sadd(cache_key, md5_value)

    vector = get_embedding(paragraph)

    properties = {
        "part_id": str(uuid.uuid4()),
        "knowledge_id": meta.get("knowledge_id"),
        "file_id": file_id,
        "userid": meta.get("userid"),
        "username": meta.get("username"),
        "cont_source_chn": meta.get("nickname"),
        "nickname": meta.get("nickname"),
        "digital_human_id": meta.get("digital_human_id"),
        "part_sort": part_sort,
        "part_cntt": paragraph
    }

    try:
        client.batch.add_data_object(
            data_object=properties,
            class_name="KnowledgeParagraph",
            vector=vector,
        )
        print("✅ 已写入:", properties["part_id"])
    except Exception as e:
        print("❌ 写入失败:", e)

def consume_kafka_messages():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([KAFKA_TOPIC])

    executor = ThreadPoolExecutor(max_workers=5)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Kafka 错误:", msg.error())
                continue

            msg_json = json.loads(msg.value().decode('utf-8'))

            meta = {
                # 知识库ID暂时没有
                "knowledge_id": msg_json.get("knowledge_id"),
                "file_id": msg_json.get("id"),
                # 用户id
                "userid": msg_json.get("userid"),
                # 用户名称
                "username": msg_json.get("username"),
                # 昵称 微博、博客、论坛的用户昵称，必填(可用USERNAME填充)；新闻发布者(如果有)
                "nickname": msg_json.get("nickname"),
                # 数字人ID暂时没有
                "digital_human_id": msg_json.get("digital_human_id"),
                # 数据来源
                "cont_source_chn": msg_json.get("cont_source_chn")
            }
            content = msg_json.get("cntt", "")

            paragraphs = split_text_to_paragraphs(content)

            futures = []
            with client.batch as batch:
                for idx, paragraph in enumerate(paragraphs):
                    future = executor.submit(process_and_insert, paragraph, meta, idx + 1)
                    futures.append(future)

                for future in futures:
                    future.result()

            print(f"🔥 消息 file_id={meta.get('file_id')} 处理完成")

    except KeyboardInterrupt:
        print("⛔️ 停止消费")
    finally:
        consumer.close()
        executor.shutdown()

def check_or_create_schema():
    print("Weaviate ready:", client.is_ready())
    existing_schemas = client.schema.get()["classes"]
    class_names = [c["class"] for c in existing_schemas]

    schema = {
        "class": "KnowledgeParagraph",
        "vectorizer": "none",
        "properties": [
            {"name": "part_id", "dataType": ["string"]},
            {"name": "knowledge_id", "dataType": ["string"]},
            {"name": "file_id", "dataType": ["string"]},
            {"name": "userid", "dataType": ["string"]},
            {"name": "username", "dataType": ["string"]},
            {"name": "nickname", "dataType": ["string"]},
            {"name": "cont_source_chn", "dataType": ["string"]},
            {"name": "digital_human_id", "dataType": ["string"]},
            {"name": "part_sort", "dataType": ["int"]},
            {"name": "part_cntt", "dataType": ["text"]},
        ],
    }

    if "KnowledgeParagraph" not in class_names:
        client.schema.create_class(schema)
        print("Schema created ✅")
    else:
        print("Schema 已存在 ✅")

if __name__ == "__main__":
    check_or_create_schema()
    consume_kafka_messages()
