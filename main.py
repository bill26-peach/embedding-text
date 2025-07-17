import threading
from fastapi import FastAPI
from checker.intent_api import router as checker_router
from knowledge.query import router as knowledge_router
from model_api.llm_api import router as llm_router
from knowledge.add import check_or_create_schema, consume_kafka_messages
from contextlib import asynccontextmanager

app = FastAPI()

# 分模块挂载
app.include_router(checker_router, prefix="/checker")
app.include_router(knowledge_router, prefix="/knowledge")
app.include_router(llm_router, prefix="/model")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 🚀 启动逻辑
    print("启动时执行 schema 检查...")
    check_or_create_schema()

    t = threading.Thread(target=consume_kafka_messages, daemon=True)
    t.start()
    print("Kafka 消费线程已启动 ✅")

    yield  # 👈 注意这里，表示「运行服务中」

    # 💥 关闭逻辑（可选）
    print("服务关闭，做一些清理...")


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def root():
    return {"message": "🚀 服务运行中！"}
