from elasticsearch import Elasticsearch, helpers
from confluent_kafka import Producer
import json
from data import ES_HOSTS, INDEX_NAME, PASSWORD, USERNAME, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

# ======================
# 初始化连接
# ======================
# 把字符串转成列表
try:
    ES_HOSTS = json.loads(ES_HOSTS)
except json.JSONDecodeError:
    raise ValueError(f"ES_HOSTS 环境变量格式不正确: {ES_HOSTS}")
es = Elasticsearch(
    ES_HOSTS,
    http_auth=(USERNAME, PASSWORD)
)

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'es-to-kafka-producer'
}
producer = Producer(conf)


# ======================
# Kafka 回调函数
# ======================
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# ======================
# 从ES读取数据并推送到Kafka
# ======================
def es_to_kafka():
    query = {
        "query": {
            "match_all": {}
        }
    }

    # 使用 scan 避免深分页问题
    results = helpers.scan(
        client=es,
        index=INDEX_NAME,
        query=query,
        scroll="2m",
        size=500
    )

    for doc in results:
        data = doc["_source"]  # 取出文档内容
        message = json.dumps(data, ensure_ascii=False)

        # 推送到 Kafka
        producer.produce(
            KAFKA_TOPIC,
            value=message.encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)

    # 刷新消息队列
    producer.flush()


if __name__ == "__main__":
    es_to_kafka()
