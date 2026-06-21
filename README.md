# Real-time Trending News System

Hệ thống thu thập và xử lý tin tức real-time với kiến trúc bigdata: Kafka → Spark → Elasticsearch → Kibana.

## 🏗️ Architecture

- **Crawlers** — RSS feeds → Kafka (`raw_news`)
- **Kafka + Zookeeper** — Message streaming
- **Spark Streaming** — Đọc Kafka → gọi external embedding API → ghi Elasticsearch
- **Elasticsearch** — Lưu raw + processed articles (có embedding)
- **Kibana** — Dashboard trend / topic / sentiment
- **Airflow** — Orchestration DAG

> Embedding model gọi qua API (OpenAI / Jina). Không CUDA, không ONNX.

## Data Flow

```
Crawlers (VnExpress, Kenh14)
    ↓ RSS
Kafka topic: raw_news
    ↓ Spark Streaming (spark_embedder.py)
Embedding API (OpenAI/Jina/...)
    ↓
Elasticsearch: news_raw (raw)
Elasticsearch: news_processed (có embedding)
    ↑
Kibana Dashboard
```

## Services

| Service | Port | Creds |
|---------|------|-------|
| Elasticsearch | 9200 | — |
| Kibana | 5601 | — |
| Kafka | 9092 | — |
| Spark Master | 8080 | — |
| Spark Worker | 8081 | — |
| Airflow | 8085 | admin/admin |

## Quick Start

```bash
# 1. Config
cp .env.example .env
# Sửa EMBEDDING_API_KEY trong .env

# 2. Run
docker-compose up --build -d

# 3. Airflow UI → trigger complete_news_pipeline_dag
open http://localhost:8085  # admin/admin

# 4. Kibana → create Index Pattern (news_processed) → Dashboard
open http://localhost:5601
```

## .env Config

| Var | Description |
|-----|-------------|
| `ES_HOSTS` | `http://elasticsearch:9200` |
| `ES_RAW_INDEX` | `news_raw` |
| `ES_PROCESSED_INDEX` | `news_processed` |
| `EMBEDDING_API_URL` | VD: `https://api.openai.com/v1/embeddings` |
| `EMBEDDING_API_KEY` | API key của bạn |
| `EMBEDDING_MODEL` | VD: `text-embedding-ada-002` |
| `EMBEDDING_DIM` | 1536 (ada-002) / 1024 (jina) |

## Kibana Dashboard

Sau khi ES có data, Kibana dashboard gợi ý:

1. **Index Pattern:** `news_processed` (Stack Management > Index Patterns)
2. **Dashboard:**
   - Số bài viết theo thời gian (line chart)
   - Top sources (pie chart)
   - Top categories (bar chart)
   - Sentiment phân bố (nếu có)
   - Recent articles table

Access: [http://localhost:5601](http://localhost:5601) — không cần auth.
