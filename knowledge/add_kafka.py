from confluent_kafka import Producer
import json
import uuid
from knowledge import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

def delivery_report(err, msg):
    """消息发送回调"""
    if err is not None:
        print(f"❌ 消息发送失败: {err}")
    else:
        print(f"✅ 消息已发送到 {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} key={msg.key()}")

if __name__ == "__main__":
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # 公共字段（除 cntt / id）
    base_data = {
        "cnt_retweet": 4,
        "page_type": "01",
        "cont_source_chn": "Facebook",
        "page_action_type": "07",
        "putm": "2025-06-27 13:50:01",
        "task_id": "9cf8225e230f4051b10f5090cdfc9331",
        "topic_type": "用户分享",
        "userid": "100064602885147",
        "nickname": "TVBS 少康戰情室",
        "fina": r"E:\资料\数字人\zip\success\20250725\im_shared_post_fb\7_7_user_edge_9cf8225e230f4051b10f5090cdfc9331_1751276531621_65087_6_ver001_22807_1.json",
        "topic_guid": "aa37c4d753cda3000f5fcf5b65be5f8b",
        "topic_id": "1136187868544632",
        "if_original": 1,
        "latm": "2025-07-25 12:23:33",
        "urlx": "https://m.facebook.com/story.php?story_fbid=pfbid02PJxnncYwMG2Jgtd5kU2kYaL78fQb3NTWK8A3J2a7wqjLPbhZwPLsT4ToQFkwSWXAl&id=100064602885147",
        "page_user_type_chn": "用户帖文",
        "page_user_type": "1000",
        "imtm": "2025-07-25 12:23:33",
        "if_original_chn": "原创",
        "cont_author": "TVBS 少康戰情室",
        "page_type_chn": "公开",
        "site_name": "Facebook",
        "cont_source": "71",
        "page_action_type_chn": "发贴/发微博/发微信",
        "daso": "digitalman",
        "cnt_agree": 286,
        "catm": "2025-06-30 17:42:11",
        "cnt_comment": 74,
        "username": "2100room"
    }

    # 10 条不同的文本
    cntt_list = [
        "學生被叫「雜質」，市長回應引熱議。",
        "天氣驟變，街頭出現大批遊客避雨。",
        "校園活動火熱展開，師生同樂。",
        "夜市新開張，人潮湧入美食攤位。",
        "地鐵突發延誤，乘客耐心等待。",
        "演唱會現場氣氛熱烈，粉絲高呼偶像名字。",
        "圖書館舉辦閱讀馬拉松，吸引書迷參加。",
        "海灘清潔日，志願者共襄盛舉。",
        "新款手機上市，引發排隊搶購熱潮。",
        "社區舉辦中秋晚會，居民共享團圓時光。"
    ]

    # 逐条发送
    for i, text in enumerate(cntt_list, start=1):
        data = dict(base_data)  # 浅拷贝
        data["cntt"] = text
        data["id"] = str(uuid.uuid4())  # 每条唯一 id

        # 使用 userid 作为 key，能保证同一用户的消息落到同一分区（可选）
        key = data["userid"].encode("utf-8")

        producer.produce(
            topic=KAFKA_TOPIC,
            key=key,
            value=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            callback=delivery_report
        )

        # 触发回调处理，避免本地缓冲过满
        producer.poll(0)

    # 刷新确保全部发送出去
    producer.flush()
    print("🎯 10 条测试数据推送完成")
