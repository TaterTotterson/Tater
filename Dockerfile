# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables to prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libffi-dev \
    libssl-dev \
    curl \
    git \
    python3-dev \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Poetry for dependency management
RUN pip install --no-cache-dir poetry

# Set the working directory
WORKDIR /app

# Copy only pyproject.toml and poetry.lock if available to leverage Docker cache
COPY pyproject.toml poetry.lock* ./

# Install dependencies without installing the project itself
RUN poetry install --no-root --only main

# Copy the entire project directory to /app
COPY . .

# Command to run the bot
CMD ["poetry", "run", "python", "main.py"]