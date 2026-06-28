# Real-time Trending News System

Hệ thống thu thập, xử lý và phát hiện xu hướng tin tức real-time với kiến trúc Big Data phân tán: Crawlers → Kafka Cluster → Spark Cluster → Elasticsearch Cluster → Kibana.

## 🏗️ Architecture

```
Crawlers (VnExpress, Kenh14)
    ↓ RSS
Kafka Cluster (3 brokers) — topic: raw_news
    ↓ Spark Streaming (spark_embedder.py)
Embedding API (OpenAI / Jina)
    ↓
Kafka Cluster — topic: processed_data          Elasticsearch Cluster (3 nodes)
    ↓                                            ↑
Spark Batch (trending_clustering.py) ────────────┘
    ↓ KMeans clustering trên embedding vectors
Elasticsearch: news_raw (raw) + news_processed (enriched + topic)
    ↑
Kibana Dashboard — trend / topic / keyword analysis
    ↑
Airflow — Orchestration DAG (1 phút)
```

## 🧱 Components

### Crawlers
- **VnExpress** + **Kenh14** RSS feeds
- ThreadPool (8 workers) crawl song song ~30+ RSS feeds mỗi 60s
- Gửi JSON messages vào Kafka topic `raw_news`
- ISO 8601 datetime, stop-word filtering

### Kafka Cluster (3 brokers)
- Topic: `raw_news` (crawl → spark) + `processed_data` (spark → clustering)
- Replication factor: 3, min.insync.replicas: 2
- Compression: gzip, batching: 32KB, linger: 500ms
- **Schema Registry** (port 8081) cho schema evolution

### Spark Cluster (1 master + 3 workers)
| Worker | RAM | Cores | Port |
|--------|-----|-------|------|
| spark-worker-v4 | 2G | 2 | 8081 |
| spark-worker-v4-2 | 2G | 2 | 8082 |
| spark-worker-v4-3 | 2G | 2 | 8083 |

**Jobs:**
- **spark_embedder.py** (Streaming) — đọc Kafka → gọi embedding API → ghi ES + Kafka
- **trending_clustering.py** (Batch) — đọc articles từ ES → KMeans clustering → gán topic label

### Elasticsearch Cluster (3 nodes)
| Node | Port HTTP | Port Transport | RAM |
|------|-----------|----------------|-----|
| node-1 | 9200 | 9300 | 512MB |
| node-2 | 9201 | 9301 | 256MB |
| node-3 | 9202 | 9302 | 256MB |

**Indices:**
- `news_raw` — raw article data (3 shards)
- `news_processed` — enriched: embedding (dense_vector, cosine), top_keywords (nested), topic_id, topic_label

### Trending Detection Pipeline
1. **Keyword Extraction** — trong `spark_embedder.py`, trích top-10 keywords từ title + content (TF-based, filter stop words)
2. **Embedding** — gọi OpenAI/Jina API, lưu `dense_vector` vào ES
3. **Clustering** — Spark Batch KMeans (MLlib) trên embedding vectors, gom articles thành 20 topics
4. **Topic Labeling** — aggregate keywords per cluster → label tự động

## 📊 Data Flow

```
Crawlers (VnExpress, Kenh14)
    ↓ RSS feeds
    ↓ ThreadPoolExecutor(8)
Kafka topic: raw_news (RF=3)
    ↓ Spark Streaming (micro-batch 30s, max 20 records/trigger)
spark_embedder.py
    ├── 1. Parse JSON → filter null/duplicate
    ├── 2. Extract keywords (TF-based, top-10)
    ├── 3. Call embedding API (OpenAI/Jina, 3 retries)
    └── 4. Write (parallel 5 threads)
            ├── ES: news_raw (raw document)
            ├── ES: news_processed (enriched + embedding + keywords)
            └── Kafka: processed_data
                        ↓
    trending_clustering.py (Spark Batch, mỗi 60s)
        ├── 1. Fetch articles without topic_id from ES
        ├── 2. KMeans (K=20, Spark MLlib)
        ├── 3. Analyze cluster → topic_label + topic_keywords
        └── 4. Update ES documents
                    ↑
            Kibana Dashboard
```

## 🔌 Services

| Service | Hostname | Port | Creds |
|---------|----------|------|-------|
| **Zookeeper** | zookeeper-v4 | 2181 | — |
| **Kafka Broker 1** | kafka-v4 | 9092 (ext) / 29092 (int) | — |
| **Kafka Broker 2** | kafka-v4-2 | 9093 (ext) / 29093 (int) | — |
| **Kafka Broker 3** | kafka-v4-3 | 9094 (ext) / 29094 (int) | — |
| **Schema Registry** | schema-registry-v4 | 8081 | — |
| **Elasticsearch Node 1** | elasticsearch-v4 | 9200 | — |
| **Elasticsearch Node 2** | elasticsearch-v4-2 | 9201 | — |
| **Elasticsearch Node 3** | elasticsearch-v4-3 | 9202 | — |
| **Kibana** | kibana-v4 | 5601 | — |
| **Spark Master** | spark-master-v4 | 8080 / 7077 | — |
| **Spark Worker 1** | spark-worker-v4 | 8081 | — |
| **Spark Worker 2** | spark-worker-v4-2 | 8082 | — |
| **Spark Worker 3** | spark-worker-v4-3 | 8083 | — |
| **Airflow Webserver** | airflow-webserver-v4 | 8085 | admin/admin |
| **Airflow Postgres** | airflow-postgres-v4 | 5435 | airflow/airflow |

## 🚀 Quick Start

```bash
# 1. Config
cp .env.example .env
# Sửa EMBEDDING_API_KEY trong .env

# 2. Download Spark-Kafka connector jars (nếu không có internet ở runtime)
bash jars/download-jars.sh

# 3. Run
docker-compose up --build -d

# 4. Airflow UI → trigger complete_news_pipeline_dag
open http://localhost:8085  # admin/admin

# 5. Kibana → create Index Pattern (news_processed) → Dashboard
open http://localhost:5601
```

## ⚙️ .env Config

| Var | Default | Description |
|-----|---------|-------------|
| `ES_HOSTS` | `http://elasticsearch:9200` | Elasticsearch HTTP endpoint |
| `ES_RAW_INDEX` | `news_raw` | Raw article index |
| `ES_PROCESSED_INDEX` | `news_processed` | Enriched index (embedding + keywords) |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka-v4:29092,...` | 3-broker Kafka cluster |
| `KAFKA_RAW_TOPIC` | `raw_news` | Crawler → Spark topic |
| `KAFKA_PROCESSED_TOPIC` | `processed_data` | Spark output topic |
| `EMBEDDING_API_URL` | — | OpenAI / Jina endpoint |
| `EMBEDDING_API_KEY` | — | API key |
| `EMBEDDING_MODEL` | `text-embedding-ada-002` | Model name |
| `EMBEDDING_DIM` | `1536` | Vector dimension (1536 ada-002 / 1024 jina) |
| `NUM_TOPICS` | `20` | Số clusters cho KMeans |
| `MIN_CLUSTER_SIZE` | `3` | Cluster tối thiểu để gán topic |
| `CLUSTERING_BATCH_SIZE` | `500` | Số articles tối đa mỗi batch cluster |

## 📈 Kibana Dashboard

Sau khi ES có data, Kibana dashboard gợi ý:

1. **Index Pattern:** `news_processed` (Stack Management > Index Patterns)
2. **Dashboard:**
   - Số bài viết theo thời gian (line chart, field: `processed_at`)
   - Top keywords (tag cloud / term plot, field: `top_keywords.word`)
   - Topics phân bố (pie chart, field: `topic_label`)
   - Top sources (pie chart, field: `source`)
   - Top categories (bar chart, field: `category`)
   - Recent articles table

Access: [http://localhost:5601](http://localhost:5601) — không cần auth.

## 📁 Project Structure

```
├── crawler/
│   ├── Vnexpress_Crawler.py    # VnExpress RSS → Kafka
│   └── Kenh14_Crawler.py       # Kenh14 RSS → Kafka
├── processor/
│   ├── spark_embedder.py       # Spark Streaming: Kafka → embedding API → ES + Kafka
│   └── trending_clustering.py  # Spark Batch: KMeans clustering → topic labeling
├── dags/
│   └── Complete_News_Pipeline_DAG.py  # Airflow orchestration
├── jars/
│   └── download-jars.sh        # Download Spark-Kafka connector jars
├── docker-compose.yml          # 18 services: ZK, Kafka×3, SR, ES×3, Kibana, Spark×4, Airflow×3, Postgres
├── spark.Dockerfile            # Spark Python image
└── Dockerfile                  # Airflow image
```
