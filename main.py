import signal
import threading
from fastapi import FastAPI
from checker.intent_api import router as checker_router
from knowledge.search_vector import router as knowledge_router
from knowledge.search_merge import router as search_merge_knowledge_router
from model_api.llm_api import router as llm_router
from knowledge.add import check_or_create_schema, consume_kafka_messages, _running
from contextlib import asynccontextmanager
import uvicorn

def _signal_handler(signum, frame):
    global _running
    print(f"收到信号 {signum}，正在请求 Kafka 消费线程退出...")
    _running = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 绑定信号
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # 启动逻辑
    print("启动时执行 schema 检查...")
    check_or_create_schema()

    t = threading.Thread(target=consume_kafka_messages, daemon=True)
    t.start()
    print("Kafka 消费线程已启动 ✅")

    yield  # 服务运行中

    # 关闭逻辑
    print("服务关闭，等待 Kafka 消费线程退出...")
    t.join(timeout=5)
    print("清理完成 ✅")

app = FastAPI(lifespan=lifespan)
app.include_router(checker_router, prefix="/checker")
app.include_router(knowledge_router, prefix="/knowledge")
app.include_router(search_merge_knowledge_router, prefix="/knowledge")
app.include_router(llm_router, prefix="/model")

@app.get("/")
async def root():
    return {"message": "🚀 服务运行中！"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888)
