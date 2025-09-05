FROM python:3.13-slim

WORKDIR /app

# 无额外系统依赖，保持镜像轻量
RUN apt-get update && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 验证关键依赖（可选）
RUN python -c "import lark_oapi; print('lark-oapi available')"

COPY . .

ENV TMP_ROOT /tmp/fastapi_data_proc

EXPOSE 8080

CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}
