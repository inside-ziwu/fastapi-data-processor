FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV TMP_ROOT /tmp/fastapi_data_proc

EXPOSE 8080

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "${PORT:-8080}"]