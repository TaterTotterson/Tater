FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app

RUN python -m venv /opt/venv \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    git \
    libpq-dev \
    libolm-dev \
    libffi-dev \
    pkg-config \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN python -m pip install --upgrade pip \
 && python -m pip install -r requirements.txt


FROM python:3.11-slim AS runtime

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    HTMLUI_PORT=8501

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ca-certificates \
    git \
    wget \
    ffmpeg \
    libpq5 \
    libolm3 \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY . .

EXPOSE 8501

CMD ["sh", "run_ui.sh"]
