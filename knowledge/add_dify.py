import os
import json
import logging
import signal
import time
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor

import requests
from confluent_kafka import Consumer
from dotenv import load_dotenv
from .lru_cache import LRUCache
from knowledge import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC

# =========================
# 配置和初始化
# =========================
load_dotenv()

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

MAX_WORKERS = _get_int("MAX_WORKERS", 5)
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "1.0"))
DIFY_API_URL = os.getenv("DIFY_API_URL", "http://172.23.27.133:8680/v1")
DIFY_API_TOKEN = os.getenv("DIFY_API_TOKEN")
METADATA = os.getenv("METADATA", "account")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

_running = True
document_cache = LRUCache(20)
fragment_queue = Queue()

# =========================
# 信号处理
# =========================
def _signal_handler(signum, frame):
    global _running
    log.info("收到信号 %s，正在优雅退出...", signum)
    _running = False

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

# =========================
# 知识库管理
# =========================
def create_or_get_dataset():
    dataset_name = "贴文信息"
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    response = requests.get(f"{DIFY_API_URL}/datasets", headers=headers)
    if response.status_code != 200:
        log.error("查询知识库失败: %s", response.text)
        return None

    datasets = response.json().get('data', [])
    dataset_id = None
    for dataset in datasets:
        if dataset['name'] == dataset_name:
            dataset_id = dataset['id']
            break

    if not dataset_id:
        response = requests.post(
            f"{DIFY_API_URL}/datasets",
            headers=headers,
            json={"name": dataset_name, "permission": "all_team_members"}
        )
        if response.status_code != 200:
            log.error("创建知识库失败: %s", response.text)
            return None
        dataset_id = response.json().get('id')

        add_metadata_to_dataset(dataset_id)
        update_dataset(dataset_id)  # ✅ 保留完整知识库修改参数

    log.info("使用的知识库 ID: %s", dataset_id)
    return dataset_id

def add_metadata_to_dataset(dataset_id):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}
    metadata_data = {"type": "string", "name": METADATA}
    response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/metadata", headers=headers, json=metadata_data)
    if response.status_code == 201:
        log.info("为知识库 %s 添加元数据 %s 成功", dataset_id, METADATA)
    else:
        log.error("添加元数据失败: %s", response.text)

def update_dataset(dataset_id):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}
    data = {
        "permission": "all_team_members",
        "indexing_technique": "high_quality",
        "retrieval_model": {
            "search_method": "hybrid_search",
            "reranking_mode": "weighted_score",
            "reranking_enable": False,
            "top_k": 6,
            "score_threshold_enabled": True,
            "score_threshold": 0.25,
            "weights": {
                "keyword_setting": {"keyword_weight": 0.5},
                "vector_setting": {
                    "vector_weight": 0.5,
                    "embedding_model_name": "qwen3-embed-4b",
                    "embedding_provider_name": "langgenius/openai_api_compatible/openai_api_compatible"
                }
            }
        }
    }
    response = requests.patch(f"{DIFY_API_URL}/datasets/{dataset_id}", headers=headers, json=data)
    if response.status_code == 200:
        log.info("知识库修改成功: %s", response.json())
        return response.json()
    else:
        log.error("修改知识库失败: %s", response.text)
        return None

dataset_id = create_or_get_dataset()

def get_metadata_id(dataset_id, metadata_name):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}
    response = requests.get(f"{DIFY_API_URL}/datasets/{dataset_id}/metadata", headers=headers)
    if response.status_code != 200:
        log.error("查询元数据失败: %s", response.text)
        return None
    for metadata in response.json().get("doc_metadata", []):
        if metadata['name'] == metadata_name:
            return metadata['id']
    return None

metadata_id = get_metadata_id(dataset_id, METADATA)

# =========================
# 文档管理
# =========================
def get_or_create_document(userid, content):
    cached_document_id = document_cache.get(userid)
    if cached_document_id:
        return cached_document_id

    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}
    response = requests.get(f"{DIFY_API_URL}/datasets/{dataset_id}/documents?keyword={userid}", headers=headers)
    if response.status_code != 200:
        log.error("查询文档失败: %s", response.text)
        return None

    document_data = response.json().get("data", [])
    document_id = None
    for document in document_data:
        if document["name"] == str(userid):
            document_id = document["id"]
            break

    if not document_id:
        document_data = {
            "name": f"{userid}",
            "text": content,
            "indexing_technique": "high_quality",  # 使用高质量索引
            "doc_form": "text_model",  # 设置文档为文本模型
            "process_rule": {"mode": "custom", "rules": {
                "pre_processing_rules": [
                    {
                        "id": "remove_extra_spaces",  # 预处理规则：替换连续空格、换行符、制表符
                        "enabled": True  # 启用该规则
                    },
                    {
                        "id": "remove_urls_emails",  # 预处理规则：删除 URL 和电子邮件地址
                        "enabled": True  # 启用该规则
                    }
                ],
                "segmentation": {
                    "separator": "****",  # 自定义分段标识符，默认为换行符
                    "max_tokens": 1000,  # 最大 token 长度，默认为 1000
                    "chunk_overlap": 100,  # 最大 token 长度，默认为 1000
                    "parent_mode": "full-doc"  # 父分段的召回模式，选择全篇召回
                }
            }}
        }
        response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/document/create-by-text", headers=headers,
                                 json=document_data)
        if response.status_code != 200:
            log.error("创建文档失败: %s", response.text)
            return None
        document_id = response.json().get('document').get('id')
        batch_id = response.json().get('batch')

        # ✅ 等待文档就绪
        while True:
            r = requests.get(f"{DIFY_API_URL}/datasets/{dataset_id}/documents/{batch_id}/indexing-status", headers=headers)
            if r.status_code == 200:
                status = r.json().get("data", [{}])[0].get("indexing_status")
                if status == "completed":
                    log.info(r.json())
                    break
        time.sleep(2)

    document_cache.put(userid, document_id)

    # 更新文档元数据
    if metadata_id:
        update_document_metadata(document_id, metadata_id, METADATA, str(userid))

    return document_id

def update_document_metadata(document_id, metadata_id, metadata_name, metadata_value):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}
    metadata_data = {
        "operation_data": [{
            "document_id": document_id,
            "metadata_list": [{"id": metadata_id, "name": metadata_name, "value": metadata_value}]
        }]
    }
    response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/documents/metadata", headers=headers, json=metadata_data)
    if response.status_code == 200:
        log.info("文档元数据更新成功: %s", document_id)

# =========================
# 异步上传片段
# =========================
def upload_worker():
    while _running or not fragment_queue.empty():
        try:
            document_id, content, userid, cont_source_chn, nickname = fragment_queue.get(timeout=1)
        except Empty:
            continue
        for attempt in range(3):
            status = _upload_fragment(document_id, content, userid, cont_source_chn, nickname)
            if status == 200:
                break
            log.warning("上传片段失败，重试 %s/3", attempt+1)
            time.sleep(2)
        fragment_queue.task_done()

def _upload_fragment(document_id, content, userid, cont_source_chn, nickname):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}
    keywords = [cont_source_chn] if cont_source_chn else []
    if nickname:
        keywords.append(nickname)
    fragment_data = {"segments": [{"content": content, "keywords": keywords, "answer": ""}]}
    response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/documents/{document_id}/segments", headers=headers, json=fragment_data)
    if response.status_code == 200:
        log.info("文档片段已上传: %s", userid)
    else:
        log.error("文档片段上传失败: %s", response.text)
    return response.status_code

# =========================
# Kafka 消费
# =========================
def consume_kafka_messages():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Kafka consumer started. topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    try:
        while _running:
            msg = consumer.poll(POLL_TIMEOUT)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka 错误: %s", msg.error())
                continue

            try:
                msg_json = json.loads(msg.value().decode('utf-8'))
            except Exception as e:
                log.error("JSON 解析失败: %s", e)
                consumer.commit(msg)
                continue

            userid = msg_json.get("userid")
            content = msg_json.get("cntt", "")
            cont_source_chn = msg_json.get("cont_source_chn", "")
            nickname = msg_json.get("nickname", "")

            document_id = get_or_create_document(userid, content)
            if document_id:
                fragment_queue.put((document_id, content, userid, cont_source_chn, nickname))
                consumer.commit(msg)
            else:
                log.error("文档创建失败，跳过消息: %s", msg_json.get("id"))
    finally:
        consumer.close()

# =========================
# 启动
# =========================
def main():
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _ in range(MAX_WORKERS):
            executor.submit(upload_worker)
        consume_kafka_messages()
        fragment_queue.join()  # 等待队列任务完成

if __name__ == "__main__":
    main()
