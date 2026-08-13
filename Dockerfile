# Use an official Python runtime as a parent image.
FROM python:3.11-slim

# Prevent some pip noise & keep Python stdout unbuffered
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    TATER_LLAMA_CPP_SERVER_BIN=/opt/llama.cpp/build/bin/llama-server \
    TATER_NATIVE_SATELLITE_CREDENTIALS_PATH=/app/.runtime/native_satellite_credentials.json \
    TATER_RUNTIME_DIR=/app/.runtime \
    TATER_AIRPLAY_CLI_PATH=/usr/local/bin/cliairplay \
    TATER_SHAIRPORT_SYNC_PATH=/usr/local/bin/shairport-sync \
    TATER_FFMPEG_PATH=/usr/bin/ffmpeg

ARG LLAMA_CPP_REF=master
ARG TARGETARCH
ARG AIRPLAY_CLI_VERSION=0.4.12
ARG SHAIRPORT_SYNC_VERSION=5.2.1
ARG SHAIRPORT_SYNC_SOURCE_SHA256=8f97d1a6e045bc3765b10d0cd64abe467eba343af89fa1e158f7fa28b73c4ab6

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
    autoconf \
    automake \
    libtool \
    patch \
    libssl-dev \
    libconfig-dev \
    libpopt-dev \
    libsoxr-dev \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Bake Tater's pinned AirPlay sender into the image so playback does not need
# an internet download after the container starts.
RUN set -eu; \
    image_arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "${image_arch}" in \
      amd64|x86_64) \
        asset="cliairplay-linux-x86_64"; \
        checksum="59490922adb8ac6aa3be8a1110b5472f4147fc429c3c042f986245fdb9e996ca" ;; \
      arm64|aarch64) \
        asset="cliairplay-linux-aarch64"; \
        checksum="91a5d31f0722c2b0497bbb5494f2a386dd6693ddf8ec0d24d5df00a659d7a46d" ;; \
      *) echo "Unsupported AirPlay sender architecture: ${image_arch}" >&2; exit 1 ;; \
    esac; \
    wget -q -O "${TATER_AIRPLAY_CLI_PATH}" \
      "https://github.com/music-assistant/airplay-cli/releases/download/v${AIRPLAY_CLI_VERSION}/${asset}"; \
    echo "${checksum}  ${TATER_AIRPLAY_CLI_PATH}" | sha256sum -c -; \
    chmod 0755 "${TATER_AIRPLAY_CLI_PATH}"; \
    "${TATER_AIRPLAY_CLI_PATH}" --check

# Use the same pinned classic AirPlay/RAOP receiver on Linux and macOS.
# Shairport Sync forwards decoded S16LE PCM to Tater over standard output.
COPY scripts/shairport_sync_configured_port.patch /tmp/shairport-sync-configured-port.patch
RUN set -eu; \
    wget -q -O /tmp/shairport-sync.tar.gz \
      "https://codeload.github.com/mikebrady/shairport-sync/tar.gz/refs/tags/${SHAIRPORT_SYNC_VERSION}"; \
    echo "${SHAIRPORT_SYNC_SOURCE_SHA256}  /tmp/shairport-sync.tar.gz" | sha256sum -c -; \
    mkdir -p /tmp/shairport-sync-source; \
    tar -xzf /tmp/shairport-sync.tar.gz -C /tmp/shairport-sync-source --strip-components=1; \
    patch -d /tmp/shairport-sync-source -p1 < /tmp/shairport-sync-configured-port.patch; \
    cd /tmp/shairport-sync-source; \
    autoreconf -fi; \
    ./configure --prefix=/usr/local --with-ssl=openssl --with-tinysvcmdns \
      --with-soxr --with-stdout --with-metadata --with-metadata-multicast; \
    make -j 4; \
    make install; \
    shairport-sync -V 2>&1 | grep -q "^${SHAIRPORT_SYNC_VERSION}-"; \
    shairport-sync -h 2>&1 | grep -q -- "--service-type"; \
    shairport-sync -h 2>&1 | grep -q -- "stdout"; \
    rm -rf /tmp/shairport-sync.tar.gz /tmp/shairport-sync-source /tmp/shairport-sync-configured-port.patch

# Copy the requirements file into the container.
COPY requirements.txt .

# Upgrade pip and install Python dependencies.
RUN python -m pip install --upgrade pip \
 && python -m pip install -r requirements.txt \
 && python -c "import onnx_asr, onnxruntime as ort; providers=ort.get_available_providers(); assert 'CPUExecutionProvider' in providers, providers; print('onnx-asr ready providers=' + ','.join(providers))"

RUN git clone --depth 1 --branch "${LLAMA_CPP_REF}" https://github.com/ggml-org/llama.cpp.git /opt/llama.cpp \
 && cmake -S /opt/llama.cpp -B /opt/llama.cpp/build -DCMAKE_BUILD_TYPE=Release -DGGML_NATIVE=OFF \
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
