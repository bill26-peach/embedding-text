from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from .intent_checker import cosine_similarity
from embedding_model.qwen3 import get_embedding
import numpy as np
import time

router = APIRouter()

class IntentRequest(BaseModel):
    user_text: str       # 多个用逗号隔开
    intent_text: str     # 多个用逗号隔开
    threshold: float = 0.75

class IntentMatchResult(BaseModel):
    user_text: str
    intent_text: str
    match: bool
    similarity: float
    duration_ms: float

class IntentResponse(BaseModel):
    total_ms: float
    results: list[IntentMatchResult]

@router.post("/check_intent", response_model=IntentResponse)
def check_intent(req: IntentRequest):
    try:
        user_list = [x.strip() for x in req.user_text.split(",") if x.strip()]
        intent_list = [x.strip() for x in req.intent_text.split(",") if x.strip()]

        if len(user_list) != len(intent_list):
            raise HTTPException(status_code=400, detail="user_text 和 intent_text 数量不一致")

        results = []
        start_total = time.time()

        for u, i in zip(user_list, intent_list):
            start = time.time()

            v1 = get_embedding(u)
            v2 = get_embedding(i)
            sim = cosine_similarity(v1, v2)
            match = sim >= req.threshold

            duration_ms = round((time.time() - start) * 1000, 2)

            results.append(IntentMatchResult(
                user_text=u,
                intent_text=i,
                match=match,
                similarity=round(float(sim), 4),
                duration_ms=duration_ms
            ))

        total_ms = round((time.time() - start_total) * 1000, 2)

        return IntentResponse(total_ms=total_ms, results=results)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
