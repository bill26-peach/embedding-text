FROM wallies/python-cuda:3.12-cuda12.2-runtime

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY .env .env

EXPOSE 8888

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8888"]
