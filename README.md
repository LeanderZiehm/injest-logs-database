# injest-logs-database

### Run from GitHub Container Registry:
```bash
docker run -d -p 9000:9000 \
  -e POSTGRES_HOST=host \
  -e POSTGRES_PORT=5432 \
  -e POSTGRES_DB=logs \
  -e POSTGRES_USER=user \
  -e POSTGRES_PASSWORD=pass \
  --name injest-logs \
  ghcr.io/leanderziehm/injest-logs-database:latest
```
---

### Build from source

```bash
git clone https://github.com/leanderziehm/injest-logs-database.git
cd injest-logs-database
cp .env.example .env
vim .env
docker compose up 
```
