FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir flask requests gunicorn

# Copy application
COPY app/ /app/

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 5000

# Run with gunicorn for production
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "main:app"]
