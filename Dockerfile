FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY tests ./tests
COPY .env.example ./.env.example
RUN mkdir -p /data
ENV HOST=0.0.0.0
ENV PORT=8000
EXPOSE 8000
CMD ["python", "-m", "app.main"]
