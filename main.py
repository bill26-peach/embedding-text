from fastapi import FastAPI
from knowledge.add_dify import consume_kafka_messages
from contextlib import asynccontextmanager
import uvicorn
from concurrent.futures import ThreadPoolExecutor
from knowledge.add_dify import upload_worker, fragment_queue




@asynccontextmanager
async def lifespan(app: FastAPI):
    with ThreadPoolExecutor(max_workers=6) as executor:
        # 启动片段上传线程
        for _ in range(5):
            executor.submit(upload_worker)
        # 启动 Kafka 消费线程
        executor.submit(consume_kafka_messages)

        yield  # 让 FastAPI 可以继续启动服务

        # 优雅关闭时等待队列处理完
        fragment_queue.join()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"message": "🚀 服务运行中！"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888)
