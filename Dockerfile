# Base image: Python 3.12 (slim variant) for UV and FastAPI service:contentReference[oaicite:10]{index=10}
FROM python:3.12-slim

# 1. Install UV package manager (via pip):contentReference[oaicite:11]{index=11}
RUN pip install --no-cache-dir uv

# 2. Set working directory for the app
WORKDIR /app

# 3. Copy Python project files for dependency installation
COPY pyproject.toml uv.lock ./

# 4. Use UV to create venv and install dependencies:contentReference[oaicite:12]{index=12}:contentReference[oaicite:13]{index=13}
RUN uv sync  && rm -rf ~/.cache/uv  \
    && find . -type d -name '__pycache__' -exec rm -rf {} +

# 5. Copy application code and configuration
COPY . .

# 如果有 .env 文件，取消下一行注释
COPY .env .env

EXPOSE 8888

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8888"]