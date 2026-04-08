FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

COPY pyproject.toml requirements.txt README.md ./
COPY src ./src
COPY mlruns ./mlruns

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e .

EXPOSE 8080

CMD ["sh", "-c", "uvicorn clothing_mlops.service:app --host 0.0.0.0 --port ${PORT}"]
