from knowledge import KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC
from confluent_kafka import Consumer
import json
from concurrent.futures import ThreadPoolExecutor
import signal
import logging
import os
import requests
from .lru_cache import LRUCache
from dotenv import load_dotenv

# ✅ 加载 .env 文件
load_dotenv()


# =========================
# 环境变量 & 配置
# =========================
def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default


# —— 基础参数
MAX_WORKERS = _get_int("MAX_WORKERS", 5)
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT", "1.0"))

# —— 连接 Dify
DIFY_API_URL = os.getenv("DIFY_API_URL", "http://172.23.27.133:8680/v1")
DIFY_API_TOKEN = os.getenv("DIFY_API_TOKEN", "dataset-6uecJ0fySNjtKUkCAFkF0aZ5")
METADATA = os.getenv("METADATA", "account")

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest")

# 运行标志（优雅退出）
_running = True

# 缓存文档 ID（避免重复查询）
document_cache = LRUCache(20)


# =========================
# 创建知识库
# =========================
def create_or_get_dataset():
    dataset_name = "贴文信息"
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    # 查询是否存在知识库
    response = requests.get(f"{DIFY_API_URL}/datasets", headers=headers)

    # 打印响应内容和状态码以调试
    log.info(f"Response Text: {response.text}")

    if response.status_code != 200:
        log.error("查询知识库失败: %s", response.text)
        return None

    try:
        datasets = response.json().get('data', [])
    except ValueError as e:
        log.error("响应内容不是有效的JSON格式: %s", response.text)
        return None

    dataset_id = None
    for dataset in datasets:
        if dataset['name'] == dataset_name:
            dataset_id = dataset['id']
            break

    # 如果没有找到，则创建知识库
    data = {
        "name": dataset_name,
        "permission": "all_team_members"  # 设置权限为团队成员可见
    }
    if not dataset_id:
        response = requests.post(f"{DIFY_API_URL}/datasets", headers=headers, json=data)
        if response.status_code != 200:
            log.error("创建知识库失败: %s", response.text)
            return None
        dataset_id = response.json().get('id')

        # 创建知识库后添加元数据
        add_metadata_to_dataset(dataset_id)
        # 修改知识库
        update_dataset(dataset_id)

    log.info("使用的知识库 ID: %s", dataset_id)
    return dataset_id


# 为知识库添加元数据
def update_dataset(dataset_id):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    data = {
        "indexing_technique": "high_quality",
        "retrieval_model": {
            "search_method": "hybrid_search",  # 使用混合检索
            "reranking_mode": "weighted_score",

            "reranking_enable": False,  # 启用重排序
            "top_k": 6,  # 返回前6条结果
            "score_threshold_enabled": True,  # 启用评分阈值
            "score_threshold": 0.25,
            "weights": {
                "keyword_setting": {
                    "keyword_weight": 0.5
                },
                "vector_setting": {
                    "vector_weight": 0.5,
                    "embedding_model_name": "qwen3-embed-4b",
                    "embedding_provider_name": "langgenius/openai_api_compatible/openai_api_compatible"
                }
            },
        },
    }
    response = requests.patch(f"{DIFY_API_URL}/datasets/{dataset_id}", headers=headers, json=data)

    if response.status_code == 200:
        log.info("知识库修改成功: %s", response.json())
        return response.json()
    else:
        log.error("修改知识库失败: %s", response.text)
        return None


# 为知识库添加元数据
def add_metadata_to_dataset(dataset_id):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    # 为知识库添加元数据
    metadata_data = {
        "type": "string",  # 元数据类型
        "name": METADATA  # 元数据名称
    }

    response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/metadata", headers=headers, json=metadata_data)

    if response.status_code == 201:
        log.info("为知识库 %s 添加元数据 %s 成功", METADATA, dataset_id)
    else:
        log.error("为知识库 %s 添加元数据 %s  失败: %s", METADATA, dataset_id, response.text)


# 获取或创建知识库
dataset_id = create_or_get_dataset()


# 获取元数据 ID
def get_metadata_id(dataset_id, metadata_name):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    # 查询知识库的所有元数据
    response = requests.get(f"{DIFY_API_URL}/datasets/{dataset_id}/metadata", headers=headers)

    if response.status_code != 200:
        log.error("查询元数据失败: %s", response.text)
        return None

    metadata_list = response.json().get("doc_metadata", [])
    for metadata in metadata_list:
        if metadata['name'] == metadata_name:
            return metadata['id']

    log.error("未找到名为 '%s' 的元数据", metadata_name)
    return None


# =========================
# 消费 Kafka 消息
# =========================
def consume_kafka_messages():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # 显式提交
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Kafka consumer started. topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

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
                log.error("JSON 解析失败: partition=%s offset=%s err=%s", msg.partition(), msg.offset(), e)
                consumer.commit(msg)  # 避免阻塞
                continue

            # 获取需要的数据字段
            userid = msg_json.get("userid")
            content = msg_json.get("cntt", "")
            cont_source_chn = msg_json.get("cont_source_chn", "")
            nickname = msg_json.get("nickname", "")  # 获取nickname字段

            # 获取 userid 对应的文档 ID 或创建新文档
            document_id = get_or_create_document(userid, content)

            if document_id:
                # 上传文档片段
                stutas = upload_document_fragment(document_id, content, userid, cont_source_chn, nickname)
                if stutas == 200:
                    # 提交 Kafka 消息的偏移量
                    consumer.commit(msg)
                    log.info("消息处理完成并提交 offset file_id=%s", msg_json.get("id"))
                else:
                    log.error("文档嵌入中，片段插入失败。file_id=%s", msg_json.get("id"))
            else:
                log.error("文档创建失败，跳过此消息处理")

    except Exception as e:
        log.exception("消费循环异常退出: %s", e)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        executor.shutdown(wait=True, cancel_futures=True)
        log.info("Kafka consumer closed. Executor shutdown.")


# =========================
# 获取或创建文档
# =========================
def get_or_create_document(userid, content):
    # 尝试从缓存中获取文档 ID
    cached_document_id = document_cache.get(userid)
    if cached_document_id:
        log.info("%s 账号，使用缓存的文档 ID: %s", userid, cached_document_id)
        return cached_document_id

    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    # 查询是否存在该用户的文档
    response = requests.get(f"{DIFY_API_URL}/datasets/{dataset_id}/documents?keyword={userid}", headers=headers)
    if response.status_code != 200:
        log.error("查询文档失败: %s", response.text)
        return None

    document_data = response.json().get("data", [])
    document_id = None
    if document_data:
        for document in document_data:
            if document["name"] == str(userid):
                document_id = document["id"]
                break
    # 如果文档不存在，则创建新文档
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
                    "separator": "\n",  # 自定义分段标识符，默认为换行符
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

        # 将文档 ID 存入 LRU 缓存
    document_cache.put(userid, document_id)
    log.info("使用的文档 ID: %s", document_id)

    # 获取元数据 ID
    metadata_id = get_metadata_id(dataset_id, METADATA)
    if metadata_id:
        # 更新文档的元数据
        update_document_metadata(document_id, metadata_id, METADATA, str(userid))

    return document_id


# 更新文档元数据
def update_document_metadata(document_id, metadata_id, metadata_name, metadata_value):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    # 更新文档的元数据
    metadata_data = {
        "operation_data": [{
            "document_id": document_id,
            "metadata_list": [{
                "id": metadata_id,  # 使用元数据 ID
                "name": metadata_name,  # 使用元数据名称
                "value": metadata_value  # 更新的元数据值
            }]
        }]
    }

    response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/documents/metadata", headers=headers,
                             json=metadata_data)

    if response.status_code == 200:
        log.info("文档元数据更新成功: %s", document_id)
    else:
        log.error("文档元数据更新失败: %s", response.text)


# =========================
# 上传文档片段到 Dify
# =========================
def upload_document_fragment(document_id, content, userid, cont_source_chn, nickname):
    headers = {"Authorization": f"Bearer {DIFY_API_TOKEN}"}

    # 将 cont_source_chn 和 nickname 作为关键词
    keywords = [cont_source_chn]
    if nickname:
        keywords.append(nickname)

    # 上传文档片段
    fragment_data = {
        "segments": [{
            "content": content,  # 将 cntt 作为片段内容
            "keywords": keywords,  # 将 cont_source_chn 和 nickname 作为关键词
            "answer": "",  # 如果没有问题和答案，可以为空
        }]
    }

    response = requests.post(f"{DIFY_API_URL}/datasets/{dataset_id}/documents/{document_id}/segments", headers=headers,
                             json=fragment_data)

    if response.status_code == 200:
        log.info("文档片段已上传: %s", document_id)
    else:
        log.error("文档片段上传失败: %s", response.text)

    return response.status_code


# =========================
# 启动消费 Kafka 消息
# =========================
def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    consume_kafka_messages()


def _signal_handler(signum, frame):
    global _running
    log.info("收到信号 %s，正在优雅退出...", signum)
    _running = False


if __name__ == "__main__":
    main()
