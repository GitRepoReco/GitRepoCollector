FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1

ARG APP_VERSION=dev

ENV APP_VERSION=${APP_VERSION}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .

CMD ["python", "main.py"]
