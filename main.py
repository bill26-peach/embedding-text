import threading
from fastapi import FastAPI
from checker.intent_api import router as checker_router
from knowledge.query import router as knowledge_router
from model_api.llm_api import router as llm_router
from knowledge.add import check_or_create_schema, consume_kafka_messages
from contextlib import asynccontextmanager
import uvicorn

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 🚀 启动逻辑
    print("启动时执行 schema 检查...")
    check_or_create_schema()

    t = threading.Thread(target=consume_kafka_messages, daemon=True)
    t.start()
    print("Kafka 消费线程已启动 ✅")

    yield  # 👈 服务运行中

    # 💥 关闭逻辑
    print("服务关闭，做一些清理...")

# ✅ 创建 app，并设置 lifespan
app = FastAPI(lifespan=lifespan)

# ✅ 挂载路由
app.include_router(checker_router, prefix="/checker")
app.include_router(knowledge_router, prefix="/knowledge")
app.include_router(llm_router, prefix="/model")

@app.get("/")
async def root():
    return {"message": "🚀 服务运行中！"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888)
