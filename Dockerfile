FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN mkdir -p /data
ENV PREDICTOR_DB_PATH=/data/satsignal_btc15m.db
ENV PREDICTOR_STORAGE_MOUNT=/data
ENV PREDICTOR_REQUIRE_PERSISTENT_STORAGE=1
ENV HOST=0.0.0.0
ENV PORT=8000
EXPOSE 8000
CMD ["python", "-m", "app.main"]
