FROM python:3.10-slim

WORKDIR /app

# Install system dependencies for FAISS
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libopenblas-dev \
    libomp-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY preprocess.py .

CMD ["python", "preprocess.py"]
