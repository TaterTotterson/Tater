# Use an official Python runtime as a parent image.
FROM python:3.11-slim

# Prevent some pip noise & keep Python stdout unbuffered
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    TATER_LLAMA_CPP_SERVER_BIN=/opt/llama.cpp/build/bin/llama-server

ARG LLAMA_CPP_REF=master

# Set the working directory in the container.
WORKDIR /app

# Install system dependencies + CA certs (for HTTPS)
# + libolm-dev + libffi-dev + pkg-config to build python-olm (Matrix E2EE)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    git \
    libpq-dev \
    wget \
    ffmpeg \
    libolm-dev \
    libffi-dev \
    cmake \
    pkg-config \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container.
COPY requirements.txt .

# Upgrade pip and install Python dependencies.
RUN python -m pip install --upgrade pip && python -m pip install -r requirements.txt

RUN git clone --depth 1 --branch "${LLAMA_CPP_REF}" https://github.com/ggml-org/llama.cpp.git /opt/llama.cpp \
 && cmake -S /opt/llama.cpp -B /opt/llama.cpp/build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build /opt/llama.cpp/build --config Release --target llama-server -j 4 \
 && "$TATER_LLAMA_CPP_SERVER_BIN" --version

# Copy the rest of your application code into the container.
COPY . .

# Expose HTML UI port.
EXPOSE 8501

# Set environment variables for HTML UI.
ENV HTMLUI_PORT=8501

# Command to run HTML UI.
CMD ["sh", "run_ui.sh"]
