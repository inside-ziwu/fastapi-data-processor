FROM python:3.13-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libmagic1 \
        libmagic-mgc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 验证关键依赖
RUN python -c "import lark_oapi; print('lark-oapi available')"
RUN python -c "import magic; print('python-magic available')" || echo "python-magic not available, will use fallback"

COPY . .

ENV TMP_ROOT /tmp/fastapi_data_proc

EXPOSE 8080

CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080}
