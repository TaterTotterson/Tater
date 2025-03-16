# Use an official Python runtime as a parent image.
FROM python:3.11-slim

# Set the working directory in the container.
WORKDIR /app

# Install system dependencies for building and running browsers.
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    wget \
    libglib2.0-0 \
    libnss3 \
    libnspr4 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libxkbcommon0 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container.
COPY requirements.txt .

# Upgrade pip and install Python dependencies.
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright browsers and their dependencies.
RUN playwright install-deps
RUN playwright install

# Copy the rest of your application code into the container.
COPY . .

# Expose the port Streamlit will run on.
EXPOSE 8501

# Set environment variables for Streamlit if needed.
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ENABLECORS=false

# Command to run your Streamlit web UI.
CMD ["streamlit", "run", "webui.py"]