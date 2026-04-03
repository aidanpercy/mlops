FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md requirements.txt ./
COPY src ./src

RUN pip install --no-cache-dir --upgrade pip && pip install --no-cache-dir .

ENV HOST=0.0.0.0
ENV PORT=8000

CMD ["uvicorn", "clothing_mlops.service:app", "--host", "0.0.0.0", "--port", "8000"]
