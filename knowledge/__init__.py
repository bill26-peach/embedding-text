import os
import weaviate
from weaviate.auth import AuthApiKey
from dotenv import load_dotenv
import redis

# ✅ 加载 .env 文件
load_dotenv()
# ==============================
# text len
# ==============================
TEXT_LENGTH = os.getenv("TEXT_LENGTH")
CERTAINTY = os.getenv("CERTAINTY")
# ==============================
# Weaviate client
# ==============================
WEAVIATE_URL = os.getenv("WEAVIATE_URL")
WEAVIATE_API_KEY = os.getenv("WEAVIATE_API_KEY")

auth_config = AuthApiKey(WEAVIATE_API_KEY)
weaviate.connect.connection.has_grpc = False

client = weaviate.Client(
    url=WEAVIATE_URL,
    auth_client_secret=auth_config,
    timeout_config=(5, 60),
    startup_period=None
)
client.batch.configure(batch_size=100, dynamic=True, timeout_retries=3)

print("✅ Weaviate client initialized and ready")

# ==============================
# Redis client
# ==============================
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", 0)

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=True)

print("✅ Redis client initialized")

# ==============================
# Kafka config
# ==============================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

print("✅ Kafka config loaded")
