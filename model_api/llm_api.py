import os
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

router = APIRouter()

# 加载 .env 文件
load_dotenv()

class LLMRequest(BaseModel):
    prompt: str

# 模型初始化示例
# MODEL_PATH = os.getenv("MODEL_PATH", r"E:\model\Qwen3-Embedding-0.6B")
# tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
# model = AutoModelForCausalLM.from_pretrained(MODEL_PATH, trust_remote_code=True).half().cuda()
# model = AutoModelForCausalLM.from_pretrained(MODEL_PATH, trust_remote_code=True)
# model.eval()

# def generate_response(prompt: str) -> str:
#     inputs = tokenizer(prompt, return_tensors="pt").to("cuda")
#     with torch.no_grad():
#         outputs = model.generate(**inputs, max_length=1024, do_sample=True, top_p=0.95, temperature=0.7)
#     result = tokenizer.decode(outputs[0], skip_special_tokens=True)
#     return result

@router.post("/call_local_llm")
async def call_llm(request: LLMRequest):
    try:
        # result = generate_response(request.prompt)
        return {"prompt": request.prompt, "response": ""}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"生成失败: {e}")
