import os
from dotenv import load_dotenv

# ✅ 加载 .env 文件
load_dotenv()

ES_HOSTS = os.getenv("ES_HOSTS")
INDEX_NAME = os.getenv("INDEX_NAME")
USERNAME = os.getenv("ESUSERNAME")
PASSWORD = os.getenv("ESPASSWORD")


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")