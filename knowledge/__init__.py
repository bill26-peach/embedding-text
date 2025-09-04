import os
from dotenv import load_dotenv

# ✅ 加载 .env 文件
load_dotenv()
# ==============================
# Kafka config
# ==============================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

print("✅ Kafka config loaded")
