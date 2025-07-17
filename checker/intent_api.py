from fastapi import APIRouter
from pydantic import BaseModel
from .intent_checker import is_intent_match
from embedding_model.qwen3 import get_embedding
import numpy as np

# 创建 Router
router = APIRouter()

# 请求体模型
class IntentRequest(BaseModel):
    user_text: str
    intent_text: str
    threshold: float = 0.75  # 可选，默认 0.75

# 返回模型
class IntentResponse(BaseModel):
    match: bool
    similarity: float

@router.post("/check_intent", response_model=IntentResponse)
def check_intent(req: IntentRequest):
    emb1 = req.user_text
    emb2 = req.intent_text
    threshold = req.threshold

    # 内部调用相似度匹配
    match = is_intent_match(emb1, emb2, threshold)

    # 重新计算相似度
    v1 = get_embedding(emb1)
    v2 = get_embedding(emb2)
    sim = np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))

    return IntentResponse(match=match, similarity=round(float(sim), 4))
