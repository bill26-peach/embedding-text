import weaviate  # type: ignore
from weaviate.auth import AuthApiKey
from confluent_kafka import Consumer
from qwen3 import get_embedding  # ✅ 你的 embedding 方法
import hashlib
import json
import uuid
import redis
from concurrent.futures import ThreadPoolExecutor
import threading
from dotenv import load_dotenv
import os

# ✅ 加载 .env 文件
load_dotenv()

# ✅ 从环境变量获取配置
WEAVIATE_URL = os.getenv("WEAVIATE_URL222")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# ✅ Redis 客户端
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)

cache_lock = threading.Lock()

def md5_text(text: str) -> str:
    """计算文本 MD5"""
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def split_text_to_paragraphs(text: str, max_len: int = 500) -> list[str]:
    """简单切段，可后续改进"""
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

def process_and_insert(client: weaviate.Client, paragraph: str, meta: dict):
    """处理段落并写入 Weaviate"""
    md5_value = md5_text(paragraph)
    file_id = meta.get("file_id")
    cache_key = f"paragraph_md5_cache:{file_id}"

    with cache_lock:
        if redis_client.sismember(cache_key, md5_value):
            print(f"🔁 已存在，file_id={file_id}, 跳过: {md5_value}")
            return
        redis_client.sadd(cache_key, md5_value)

    # 生成向量
    vector = get_embedding(paragraph)

    # 拼接属性
    properties = {
        "part_id": str(uuid.uuid4()),
        "knowledge_id": meta.get("knowledge_id"),
        "file_id": file_id,
        "userid": meta.get("userid"),
        "username": meta.get("username"),
        "digital_human_id": meta.get("digital_human_id"),
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

def consume_kafka_messages(client: weaviate.Client, topic: str, bootstrap_servers: str, group_id: str):
    """消费 Kafka 消息，解析并写入 Weaviate"""
    consumer = Consumer({
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

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
                "knowledge_id": msg_json.get("knowledge_id"),
                "file_id": msg_json.get("file_id"),
                "userid": msg_json.get("userid"),
                "username": msg_json.get("username"),
                "digital_human_id": msg_json.get("digital_human_id")
            }
            content = msg_json.get("content", "")

            paragraphs = split_text_to_paragraphs(content)

            futures = []
            with client.batch as batch:
                for paragraph in paragraphs:
                    future = executor.submit(process_and_insert, client, paragraph, meta)
                    futures.append(future)

                for future in futures:
                    future.result()

    except KeyboardInterrupt:
        print("⛔️ 停止消费")
    finally:
        consumer.close()
        executor.shutdown()

if __name__ == "__main__":
    # ✅ Weaviate 配置
    auth_config = AuthApiKey(WEAVIATE_API_KEY)
    weaviate.connect.connection.has_grpc = False
    client = weaviate.Client(
        url=WEAVIATE_URL,
        auth_client_secret=auth_config,
        timeout_config=(5, 60),
        startup_period=None
    )
    client.batch.configure(batch_size=100, dynamic=True, timeout_retries=3)

    print("Weaviate ready:", client.is_ready())

    # ✅ 定义 schema
    schema = {
        "class": "KnowledgeParagraph",
        "vectorizer": "none",
        "properties": [
            {"name": "part_id", "dataType": ["string"]},
            {"name": "knowledge_id", "dataType": ["string"]},
            {"name": "file_id", "dataType": ["string"]},
            {"name": "userid", "dataType": ["string"]},
            {"name": "username", "dataType": ["string"]},
            {"name": "digital_human_id", "dataType": ["string"]},
            {"name": "part_cntt", "dataType": ["text"]},
        ],
    }

    # ✅ 初始化 schema（若存在则先删除）
    try:
        client.schema.delete_class("KnowledgeParagraph")
    except:
        pass
    client.schema.create_class(schema)
    print("Schema created ✅")

    # ✅ 启动消费
    consume_kafka_messages(client, KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID)
