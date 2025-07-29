import os
from dotenv import load_dotenv
from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F
import numpy as np

# 加载 .env 文件
load_dotenv()

# ✅ 加载 BGE 模型
model_name = os.getenv("BGE_MODEL_PATH", r"E:\model-al\model\bge-large-zh-v1.5")
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModel.from_pretrained(model_name)

def get_embedding(text):
    # BGE 要求有指令提示（可选），如加句子 "为这个句子生成表示以用于检索相关的句子："
    text = "为这个句子生成表示以用于检索相关的句子：" + text
    batch_dict = tokenizer(text, return_tensors="pt", truncation=True, max_length=512, padding=True)
    with torch.no_grad():
        outputs = model(**batch_dict)
        embeddings = outputs.last_hidden_state[:, 0]  # BGE 默认用 CLS pooling
        embeddings = F.normalize(embeddings, p=2, dim=1)
        return embeddings[0].cpu().numpy()

# ✅ 计算相似度
def cosine_similarity(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))

# ✅ 示例
user_text = "帮我订一张去北京的机票"
intent_text = "订票"

emb1 = get_embedding(user_text)
emb2 = get_embedding(intent_text)
sim = cosine_similarity(emb1, emb2)

print(f"相似度: {sim:.4f}")
