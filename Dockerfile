FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y cron

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV TMP_ROOT /tmp/fastapi_data_proc

EXPOSE 8000

CMD ["/bin/sh", "-c", "echo '--- Probing Environment ---'; echo '--- 1. Listing /usr/bin ---'; ls -l /usr/bin/crontab; echo '--- 2. Printing PATH ---'; echo $PATH; echo '--- 3. Printing current user ---'; whoami; echo '--- 4. Finding crontab executable ---'; find / -name crontab; echo '--- 5. Probe finished, sleeping ---'; sleep 300"]