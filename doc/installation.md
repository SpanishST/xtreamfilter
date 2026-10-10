# Installation and Permissions

## Docker Compose

Create a `docker-compose.yml` file:

```yaml
services:
  xtreamfilter:
    image: spanishst/xtreamfilter:latest
    container_name: xtreamfilter
    ports:
      - "8080:5000"
    volumes:
      - ./data:/data
      - ./downloads:/downloads
    restart: unless-stopped
    environment:
      - TZ=Europe/Paris
      - PUID=1000
      - PGID=1000
```

Start the service:

```bash
docker compose up -d
```

Alternatively, run the image directly:

```bash
docker run -d \
  --name xtreamfilter \
  -p 8080:5000 \
  -v ./data:/data \
  -v ./downloads:/downloads \
  --restart unless-stopped \
  -e TZ=Europe/Paris \
  -e PUID=1000 \
  -e PGID=1000 \
  spanishst/xtreamfilter:latest
```

## Build From Source

```bash
git clone https://github.com/SpanishST/xtreamfilter.git
cd xtreamfilter
docker compose up --build -d
```

## First Steps

1. Open `http://localhost:8080`.
2. Add one or more Xtream sources.
3. Configure the dedicated route for each source.
4. Adjust filters for live, VOD, and series as needed.
5. Refresh the cache.
6. Use the displayed Xtream or M3U URLs in your IPTV client.

## User and Group Permissions

The container supports `PUID` and `PGID` environment variables to control file ownership on mounted volumes. Match them to the owner of the host directories:

```bash
id -u   # UID
id -g   # PGID
```

The startup entrypoint remaps the internal `appuser` to the specified UID/GID, creates `/data` and `/downloads` if needed, recursively changes ownership of `/data`, changes ownership of `/downloads` without recursing, checks that `/downloads` is writable, and then drops privileges with `gosu` before starting the application.

The non-recursive `/downloads` ownership change keeps SMB/NFS mounts safe. Existing files inside `/downloads` are not changed. If files are owned by root from a previous installation, fix them manually:

```bash
sudo chown -R 1000:1000 ./downloads
```

If you previously ran the container without `PUID`/`PGID` (or as root), existing files under `/data` will be reassigned to the configured user on startup. For large databases, the first startup after changing ownership may take longer. The `/downloads` directory itself is chowned non-recursively; existing root-owned files inside it may need the manual fix above.

Make sure host directories are writable by the target UID/GID. For example:

```bash
sudo chown -R 1000:1000 ./data ./downloads
```

If `chown` fails (for example on NFS with `root_squash` or a read-only bind mount), the entrypoint checks whether the target user can write to `/data`. If not, the container falls back to running as root so it can start. Check `docker logs` for a warning if you suspect this is happening.

## Development and Tests

Run locally without Docker:

```bash
pip install fastapi uvicorn[standard] httpx jinja2 python-multipart lxml rapidfuzz packaging aiosqlite
uvicorn app.main:app --host 0.0.0.0 --port 5000 --reload
```

The application is then available at `http://localhost:5000`.

Run the test suite:

```bash
uv run pytest tests/ -v
```
