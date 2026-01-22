FROM python:3.13-slim

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
# - 4 workers for parallel stream handling
# - 8 threads per worker for concurrent connections
# - Increased timeout for long-running streams
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--threads", "8", "--timeout", "0", "--keep-alive", "65", "main:app"]
