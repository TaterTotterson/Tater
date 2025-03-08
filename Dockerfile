# Use an official Python runtime as a parent image.
FROM python:3.11-slim

# Set the working directory in the container.
WORKDIR /app

# Install system dependencies (if your project requires any C libraries)
RUN apt-get update && apt-get install -y build-essential libpq-dev wget && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container.
COPY requirements.txt .

# Upgrade pip and install Python dependencies.
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of your application code into the container.
COPY . .

# Expose the port Streamlit will run on.
EXPOSE 8501

# Set environment variables for Streamlit if needed
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ENABLECORS=false

# Command to run your Streamlit web UI.
CMD ["streamlit", "run", "webui.py"]