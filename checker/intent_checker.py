# intent_checker.py
import numpy as np
from embedding_model.qwen3 import get_embedding

# ✅ 计算余弦相似度
def cosine_similarity(vec1, vec2):
    vec1 = np.array(vec1)
    vec2 = np.array(vec2)
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))

# ✅ 判断是否属于意图
def is_intent_match(text: str, intent_text: str, threshold: float = 0.75) -> bool:
    emb1 = get_embedding(text)
    emb2 = get_embedding(intent_text)
    sim = cosine_similarity(emb1, emb2)
    print(f"相似度: {sim:.4f}")
    return sim >= threshold

# ✅ 示例用法
if __name__ == "__main__":
    user_text = "Explain gravity"
    # user_text = "What is the capital of China?"
    intent_text = "Gravity is a force that attracts two bodies towards each other. It gives weight to physical objects and is responsible for the movement of planets around the sun."
    # intent_text = "The capital of China is Beijing."

    if is_intent_match(user_text, intent_text, threshold=0.75):
        print("✅ 属于该意图")
    else:
        print("❌ 不属于该意图")
