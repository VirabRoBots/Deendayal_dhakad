FROM python:3.11.7

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        git \
        ffmpeg \
        curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /Deendayal_botz

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip --root-user-action=ignore && \
    pip install --no-cache-dir -r requirements.txt --root-user-action=ignore

COPY . .

CMD ["python3", "bot.py"]
