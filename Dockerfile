# Use an official Python runtime as a parent image.
FROM python:3.11-slim

# Prevent some pip noise & keep Python stdout unbuffered
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1

# Set the working directory in the container.
WORKDIR /app

# Install system dependencies + CA certs (for HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    libpq-dev \
    wget \
    ffmpeg \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container.
COPY requirements.txt .

# Upgrade pip and install Python dependencies.
RUN python -m pip install --upgrade pip && python -m pip install -r requirements.txt

# Copy the rest of your application code into the container.
COPY . .

# Expose the port Streamlit will run on.
EXPOSE 8501

# Set environment variables for Streamlit if needed.
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ENABLECORS=false

# Command to run your Streamlit web UI.
CMD ["streamlit", "run", "webui.py"]