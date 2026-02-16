FROM python:3.13-slim

WORKDIR /src

# Ensure app package is importable regardless of working directory
ENV PYTHONPATH=/src

# Install dependencies
# FastAPI with uvicorn for async support, httpx for async HTTP client, lxml for XML parsing
RUN pip install --no-cache-dir fastapi uvicorn[standard] httpx jinja2 python-multipart lxml rapidfuzz packaging

# Copy application as a proper Python package
COPY app/ /src/app/

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 5000

# Run with uvicorn for async production
# - Single worker (async handles concurrency)
# - Increased timeouts for long-running streams
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5000", "--timeout-keep-alive", "65"]
