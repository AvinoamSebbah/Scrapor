FROM python:3.11-slim AS base

ENV TZ="Asia/Jerusalem"

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libbz2-dev libncurses-dev libreadline-dev libffi-dev libssl-dev build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY . .

FROM base AS dev
RUN pip install -r requirements-dev.txt

FROM dev AS testing
CMD python system_tests/main.py && python -m pytest .

FROM base AS data_processing
CMD python main.py

# Serving: api.py
FROM base AS serving
RUN useradd -m -u 1001 -s /bin/bash scraper
USER scraper
CMD uvicorn api:app --host 0.0.0.0 --port 8000 --proxy-headers
