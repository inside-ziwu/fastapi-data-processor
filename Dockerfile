FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron libmagic1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 验证关键依赖
RUN python -c "import lark_oapi; print('lark-oapi available')"

COPY . .

ENV TMP_ROOT /tmp/fastapi_data_proc

EXPOSE 8080

CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}