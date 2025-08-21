# -*- coding: utf-8 -*-
"""
Kafka -> 文本清洗/切分 -> (摘要或原文)向量化 -> Weaviate 批量写入
- Redis 原子去重 + TTL
- 显式提交 Kafka offset（成功后再提交）
- 摘要接口可开关；失败/关闭则回退原文向量化
- 始终把原文写入 part_cntt，不改 schema
"""

from knowledge import client, redis_client, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC, TEXT_LENGTH
from embedding_model.qwen3 import get_embedding
from confluent_kafka import Consumer

import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
import re
import signal
import logging
from typing import List, Optional
import requests
import os

# =========================
# 环境变量 & 配置
# =========================
def _get_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}

def _get_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "").strip() or default)
    except Exception:
        return default

# —— 基础参数
MIN_LENGTH          = _get_int("MIN_LENGTH", 20)
REDIS_TTL_SECONDS   = _get_int("REDIS_TTL_SECONDS", 7*24*60*60)
EMBED_RETRIES       = _get_int("EMBED_RETRIES", 3)
WRITE_RETRIES       = _get_int("WRITE_RETRIES", 3)
BATCH_SIZE          = _get_int("BATCH_SIZE", 64)
MAX_WORKERS         = _get_int("MAX_WORKERS", 5)
POLL_TIMEOUT        = float(os.getenv("POLL_TIMEOUT", "1.0"))

# —— 摘要接口（通过环境变量控制）
USE_SUMMARY_API     = _get_bool("USE_SUMMARY_API", False)
SUMMARY_API_URL     = os.getenv("SUMMARY_API_URL", "http://10.10.25.34:8660/v1/workflows/run")  # 为空则视为不可用
SUMMARY_API_TOKEN   = os.getenv("SUMMARY_API_TOKEN")    # 例如 "Bearer xxxxx"
SUMMARY_TIMEOUT     = _get_float("SUMMARY_TIMEOUT", 5*180)
SUMMARY_MAX_RETRIES = _get_int("SUMMARY_MAX_RETRIES", 2)
SUMMARY_MIN_LEN     = _get_int("SUMMARY_MIN_LEN", 20)


# =========================
# 日志
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest")

# =========================
# 正则
# =========================
URL_RE = re.compile(
    r"""(?xi)
    \b(
        (?:https?://|ftp://)
        [^\s<>"'）)】]+
        |
        www\.[^\s<>"'）)】]+
    )
    """
)
EMAIL_RE = re.compile(r"""(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b""")
SENT_SPLIT_RE = re.compile(r'(?<=[。！？!?.；;])\s*')

# 运行标志（优雅退出）
_running = True

# =========================
# 工具方法
# =========================
def clean_text(text: str) -> str:
    text = URL_RE.sub("", text)
    text = EMAIL_RE.sub("", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def md5_text(text: str) -> str:
    return hashlib.md5(text.encode('utf-8')).hexdigest()

def _split_long_line(line: str, max_len: int) -> List[str]:
    chunks: List[str] = []
    buf = ""
    sentences = list(filter(None, SENT_SPLIT_RE.split(line))) or [line]
    for sent in sentences:
        if len(sent) > max_len:
            if buf:
                chunks.append(buf)
                buf = ""
            for i in range(0, len(sent), max_len):
                chunks.append(sent[i:i + max_len])
        else:
            if buf and len(buf) + 1 + len(sent) > max_len:
                chunks.append(buf)
                buf = sent
            else:
                buf = (buf + " " + sent).strip() if buf else sent
    if buf:
        chunks.append(buf)
    return chunks

def split_text_to_paragraphs(text: str, max_len: int = 500) -> List[str]:
    paragraphs: List[str] = []
    temp = ""
    for raw_line in text.split("\n"):
        if not raw_line.strip():
            continue
        line = clean_text(raw_line.strip())
        if not line:
            continue
        for chunk in _split_long_line(line, max_len):
            if not chunk:
                continue
            if temp and len(temp) + 1 + len(chunk) > max_len:
                if len(temp.strip()) >= MIN_LENGTH:
                    paragraphs.append(temp.strip())
                temp = chunk
            else:
                temp = (temp + " " + chunk).strip() if temp else chunk
            if len(temp) >= max_len:
                if len(temp.strip()) >= MIN_LENGTH:
                    paragraphs.append(temp.strip())
                temp = ""
    if temp and len(temp.strip()) >= MIN_LENGTH:
        paragraphs.append(temp.strip())
    return paragraphs

def was_added_to_set(cache_key: str, md5_value: str) -> bool:
    added = redis_client.sadd(cache_key, md5_value)
    if added:
        try:
            redis_client.expire(cache_key, REDIS_TTL_SECONDS)
        except Exception as e:
            log.warning("设置 Redis TTL 失败: cache_key=%s err=%s", cache_key, e)
    return bool(added)

# =========================
# 摘要接口（失败/关闭即回退原文）
# =========================
def _clean_summary_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def get_summary_from_api(text: str) -> Optional[str]:
    """
    开关开启且 URL 可用时，调用摘要接口。
    成功返回摘要文本；失败/无效/未配置则返回 None（触发回退原文向量）。
    """
    if not USE_SUMMARY_API:
        return None
    if not SUMMARY_API_URL:
        log.warning("USE_SUMMARY_API=True 但未配置 SUMMARY_API_URL，跳过摘要调用")
        return None
    if not text or len(text.strip()) < MIN_LENGTH:
        return None

    headers = {"Content-Type": "application/json"}
    if SUMMARY_API_TOKEN:
        headers["Authorization"] = f"Bearer {SUMMARY_API_TOKEN}"

    payload = {
        "inputs": {"query": text},
        "response_mode": "blocking",
        "user": "admin"
    }

    last_err = None
    for attempt in range(1, SUMMARY_MAX_RETRIES + 1):
        try:
            resp = requests.post(SUMMARY_API_URL, json=payload, headers=headers, timeout=SUMMARY_TIMEOUT)
            if resp.status_code != 200:
                last_err = f"http {resp.status_code} body={resp.text[:180]}"
                log.warning("摘要接口非200(%s/%s): %s", attempt, SUMMARY_MAX_RETRIES, last_err)
                continue
            data = resp.json()
            # 按实际返回字段名调整；默认尝试 'summary' / 'data'
            summary = (data.get("data", {}) or {}).get("outputs", {}).get("result", "")
            summary = _clean_summary_text(summary)
            if len(summary) >= SUMMARY_MIN_LEN:
                return summary
            log.warning("摘要过短/空(%s/%s)，将重试", attempt, SUMMARY_MAX_RETRIES)
        except Exception as e:
            last_err = str(e)
            log.warning("摘要接口异常(%s/%s): %s", attempt, SUMMARY_MAX_RETRIES, e)

    if last_err:
        log.error("摘要接口最终失败: %s", last_err)
    return None

# =========================
# 主处理
# =========================
def process_and_insert(paragraph: str, meta: dict, part_sort: int):
    text_len = len(paragraph.strip())
    if text_len < MIN_LENGTH:
        log.info("段落过短，跳过 file_id=%s part_sort=%s len=%s", meta.get("file_id"), part_sort, text_len)
        return

    md5_value = md5_text(paragraph)
    file_id = meta.get("file_id")
    cache_key = f"paragraph_md5_cache:{file_id}"

    if not was_added_to_set(cache_key, md5_value):
        log.info("重复段落，跳过 file_id=%s md5=%s", file_id, md5_value)
        return

    # —— 选择用于向量化的文本：摘要优先（成功）→ 否则回退原文
    vector_text = paragraph
    vector_src = "original"
    summary = get_summary_from_api(paragraph)
    if summary:
        vector_text = summary
        vector_src = "summary"
        log.info("使用摘要向量化 file_id=%s part_sort=%s summary_len=%s", file_id, part_sort, len(summary))
    else:
        if USE_SUMMARY_API:
            log.info("摘要不可用/失败，回退原文向量化 file_id=%s part_sort=%s", file_id, part_sort)

    # —— 获取向量，带重试
    vector = None
    for attempt in range(EMBED_RETRIES):
        try:
            vector = get_embedding(vector_text)
            break
        except Exception as e:
            if attempt == EMBED_RETRIES - 1:
                log.error("向量生成失败(最终) file_id=%s part_sort=%s err=%s", file_id, part_sort, e)
                return
            else:
                log.warning("向量生成失败，重试中(%s/%s) file_id=%s part_sort=%s err=%s",
                            attempt + 1, EMBED_RETRIES, file_id, part_sort, e)

    properties = {
        "part_id": str(uuid.uuid4()),
        "knowledge_id": meta.get("knowledge_id"),
        "file_id": file_id,
        "userid": meta.get("userid"),
        "username": meta.get("username"),
        "cont_source_chn": meta.get("cont_source_chn"),
        "nickname": meta.get("nickname"),
        "digital_human_id": meta.get("digital_human_id"),
        "part_sort": part_sort,
        "part_cntt": paragraph  # 始终存原文
    }

    for attempt in range(WRITE_RETRIES):
        try:
            client.batch.add_data_object(
                data_object=properties,
                class_name="KnowledgeParagraph",
                vector=vector,  # 由摘要或原文生成
            )
            log.info("已加入批次写入 part_id=%s file_id=%s part_sort=%s vector_src=%s",
                     properties["part_id"], file_id, part_sort, vector_src)
            return
        except Exception as e:
            if attempt == WRITE_RETRIES - 1:
                log.error("写入失败(最终) part_id=%s file_id=%s err=%s", properties["part_id"], file_id, e)
            else:
                log.warning("写入失败，重试中(%s/%s) part_id=%s file_id=%s err=%s",
                            attempt + 1, WRITE_RETRIES, properties["part_id"], file_id, e)

def consume_kafka_messages():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # 显式提交
    })
    consumer.subscribe([KAFKA_TOPIC])
    log.info("Kafka consumer started. topic=%s group=%s", KAFKA_TOPIC, KAFKA_GROUP_ID)

    client.batch.configure(batch_size=BATCH_SIZE, dynamic=True, timeout_retries=3)
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

            meta = {
                "knowledge_id": msg_json.get("knowledge_id"),
                "file_id": msg_json.get("id"),
                "userid": msg_json.get("userid"),
                "username": msg_json.get("username"),
                "nickname": msg_json.get("nickname") or msg_json.get("username"),
                "digital_human_id": msg_json.get("digital_human_id"),
                "cont_source_chn": msg_json.get("cont_source_chn"),
            }
            content = msg_json.get("cntt") or ""

            paragraphs = split_text_to_paragraphs(content, TEXT_LENGTH)

            try:
                futures = []
                with client.batch as batch:
                    for idx, paragraph in enumerate(paragraphs, start=1):
                        futures.append(executor.submit(process_and_insert, paragraph, meta, idx))
                    for f in futures:
                        f.result()
                consumer.commit(msg)
                log.info("消息处理完成并提交 offset file_id=%s paragraphs=%s",
                         meta.get('file_id'), len(paragraphs))
            except Exception as e:
                log.error("消息处理失败，将不提交 offset 以便重试 file_id=%s err=%s",
                          meta.get('file_id'), e)
    except Exception as e:
        log.exception("消费循环异常退出: %s", e)
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        executor.shutdown(wait=True, cancel_futures=True)
        log.info("Kafka consumer closed. Executor shutdown.")

def check_or_create_schema():
    try:
        ready = client.is_ready()
        log.info("Weaviate ready: %s", ready)
    except Exception as e:
        log.warning("Weaviate 健康检查失败: %s", e)

    existing_schemas = client.schema.get().get("classes", [])
    class_names = [c.get("class") for c in existing_schemas]

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
        log.info("Schema created ✅")
    else:
        log.info("Schema 已存在 ✅")

def _signal_handler(signum, frame):
    global _running
    log.info("收到信号 %s，正在优雅退出...", signum)
    _running = False

def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    check_or_create_schema()
    consume_kafka_messages()

if __name__ == "__main__":
    main()
