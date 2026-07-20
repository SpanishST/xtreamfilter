FROM python:3.13-slim

WORKDIR /src

# Ensure app package is importable regardless of working directory
ENV PYTHONPATH=/src
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8
ENV PYTHONIOENCODING=utf-8

# Install gosu for user switching, ffmpeg for stream remuxing, mkvtoolnix for MKV metadata
RUN apt-get update \
    && apt-get install -y --no-install-recommends gosu ffmpeg mkvtoolnix \
    && rm -rf /var/lib/apt/lists/* \
    && gosu nobody true

# Create default user/group (will be remapped at runtime by entrypoint)
RUN groupadd -g 1000 appuser && useradd -u 1000 -g 1000 -m appuser

# Install dependencies
RUN pip install --no-cache-dir fastapi uvicorn[standard] httpx jinja2 python-multipart lxml rapidfuzz packaging aiosqlite

# Copy application as a proper Python package
COPY app/ /src/app/

# Copy entrypoint script
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose port
EXPOSE 5000

# PUID/PGID defaults (overridable via docker-compose / docker run)
ENV PUID=1000
ENV PGID=1000

ENTRYPOINT ["/entrypoint.sh"]

# Run with uvicorn for async production
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5000", "--timeout-keep-alive", "65"]
