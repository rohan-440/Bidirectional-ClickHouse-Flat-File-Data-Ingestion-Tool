# Bidirectional ClickHouse & Flat File Data Ingestion Tool

This project provides a REST API for ingesting data into and extracting data from a ClickHouse database and flat files. The tool supports schema preview, data preview, and execution of ingestion flows.

## 🚀 Features

- Dockerized ClickHouse setup
- Test ClickHouse connection
- List available tables in ClickHouse
- Preview and inspect schemas
- Preview sample data from ClickHouse and flat files
- Execute data ingestion flows

---

## 🐳 Docker Setup

To run ClickHouse in Docker:

```bash
docker run -d \
  --name clickhouse-server \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_DB=Db \
  -e CLICKHOUSE_USER=myuser \
  -e CLICKHOUSE_PASSWORD=mypassword \
  clickhouse/clickhouse-server
```


End points :/api/integration
1. /clickhouse/test-connection
2. /clickhouse/tables
3. /clickhouse/schema
4. /flatfile/schema
5. /clickhouse/preview
6. /flatfile/preview
7. /execute



