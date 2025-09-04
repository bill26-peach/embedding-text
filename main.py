import signal
import threading
from fastapi import FastAPI
from knowledge.add_dify import  consume_kafka_messages
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

    t = threading.Thread(target=consume_kafka_messages, daemon=True)
    t.start()
    print("Kafka 消费线程已启动 ✅")

    yield  # 服务运行中

    # 关闭逻辑
    print("服务关闭，等待 Kafka 消费线程退出...")
    t.join(timeout=5)
    print("清理完成 ✅")

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "🚀 服务运行中！"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888)
