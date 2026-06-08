# Python 3.12 slim — nhỏ gọn, không cần build tools
FROM python:3.12-slim

WORKDIR /app

# Cài dependencies trước (cache layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ source
COPY src/ ./src/
COPY setup/ ./setup/
COPY benchmark.py .

# Thư mục data sẽ được mount từ host (hoặc volume)
RUN mkdir -p data

# SITE_ID được truyền vào lúc run qua environment variable
ENV SITE_ID=0

# Expose port động — docker-compose sẽ override
EXPOSE 5000

# Chạy site server với SITE_ID từ env
CMD ["sh", "-c", "python -m src.site_server $SITE_ID"]
