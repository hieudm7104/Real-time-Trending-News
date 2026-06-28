"""
Spark Streaming Embedder
Reads raw_news from Kafka → calls external embedding API → writes to Elasticsearch + Kafka
No ONNX, no CUDA, no local model.
"""

import json
import logging
import os
import time
from datetime import datetime
from threading import Lock
from kafka import KafkaProducer
from elasticsearch import Elasticsearch
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== Config ====================
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-v4:29092")
RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "raw_news")
OUTPUT_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "processed_data")
ES_HOSTS = os.getenv("ES_HOSTS", "http://elasticsearch:9200")
ES_RAW_INDEX = os.getenv("ES_RAW_INDEX", "news_raw")
ES_PROCESSED_INDEX = os.getenv("ES_PROCESSED_INDEX", "news_processed")
EMBEDDING_API_URL = os.getenv("EMBEDDING_API_URL", "")
EMBEDDING_API_KEY = os.getenv("EMBEDDING_API_KEY", "")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-ada-002")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "1536"))

# Deduplication cache
_seen_urls = {}
_seen_urls_lock = Lock()
DEDUP_TTL_SECS = 3600  # 1 hour — URLs older than this can be re-processed

# Lazy-init clients (module-level singleton pattern)
_producer = None
_producer_lock = Lock()
_es_client = None
_es_client_lock = Lock()


# ==================== Lazy-init Clients ====================

def get_kafka_producer():
    """Return a singleton KafkaProducer, created once."""
    global _producer
    if _producer is None:
        with _producer_lock:
            if _producer is None:
                logger.info("Creating KafkaProducer (singleton)...")
                _producer = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    compression_type="gzip",
                    linger_ms=500,
                    batch_size=32768,
                    request_timeout_ms=30000,
                    retries=3,
                )
    return _producer


def get_es_client():
    """Return a singleton Elasticsearch client, created once + ensures index mappings."""
    global _es_client
    if _es_client is None:
        with _es_client_lock:
            if _es_client is None:
                logger.info("Creating Elasticsearch client (singleton)...")
                _es_client = Elasticsearch(hosts=[ES_HOSTS], request_timeout=30, retry_on_timeout=True, max_retries=3)
                _ensure_es_indices(_es_client)
    return _es_client


def _ensure_es_indices(es):
    """Create indices with proper mappings if they don't exist."""
    # news_raw — lightweight mapping, dynamic for flexibility
    if not es.indices.exists(index=ES_RAW_INDEX):
        logger.info(f"Creating index '{ES_RAW_INDEX}'...")
        es.indices.create(index=ES_RAW_INDEX, body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "dynamic": True,
                "properties": {
                    "published_at": {"type": "date"},
                    "collected_at": {"type": "date"},
                }
            }
        })

    # news_processed — needs explicit mapping for dense_vector (embedding) + date fields
    if not es.indices.exists(index=ES_PROCESSED_INDEX):
        logger.info(f"Creating index '{ES_PROCESSED_INDEX}' with mapping...")
        es.indices.create(index=ES_PROCESSED_INDEX, body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "dynamic": True,
                "properties": {
                    "published_at": {"type": "date"},
                    "collected_at": {"type": "date"},
                    "processed_at": {"type": "date"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": EMBEDDING_DIM,
                        "index": True,
                        "similarity": "cosine",
                    },
                    "embedding_dim": {"type": "integer"},
                }
            }
        })
    else:
        logger.info(f"Index '{ES_PROCESSED_INDEX}' already exists.")


# ==================== Deduplication ====================

def is_duplicate(url):
    """Check if a URL was already processed recently (thread-safe)."""
    if not url:
        return False
    now = time.time()
    with _seen_urls_lock:
        ts = _seen_urls.get(url)
        if ts is not None and (now - ts) < DEDUP_TTL_SECS:
            return True
        _seen_urls[url] = now

    # Periodically evict stale entries (probabilistic, every 1000 inserts)
    if len(_seen_urls) > 10000:
        with _seen_urls_lock:
            cutoff = now - DEDUP_TTL_SECS
            stale = [k for k, v in _seen_urls.items() if (now - v) >= cutoff]
            for k in stale:
                del _seen_urls[k]
            logger.info(f"Evicted {len(stale)} stale dedup entries, cache now {len(_seen_urls)}")

    return False


# ==================== Embedding API ====================

def call_embedding_api(texts):
    """Call external embedding API. Compatible with OpenAI, Jina, etc."""
    if not EMBEDDING_API_KEY:
        logger.warning("No API key, using dummy embeddings")
        return [[0.0] * EMBEDDING_DIM for _ in texts]

    headers = {
        "Authorization": f"Bearer {EMBEDDING_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {"input": texts, "model": EMBEDDING_MODEL}

    for attempt in range(3):
        try:
            resp = requests.post(EMBEDDING_API_URL, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "data" in data:  # OpenAI format
                items = sorted(data["data"], key=lambda x: x["index"])
                return [item["embedding"] for item in items]
            elif "embeddings" in data:  # Jina format
                return [item["embedding"] for item in data["embeddings"]]
            else:
                logger.error(f"Unknown API response keys: {list(data.keys())}")
                return [[0.0] * EMBEDDING_DIM for _ in texts]
        except Exception as e:
            logger.warning(f"API attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)

    logger.error("API failed after retries")
    return [[0.0] * EMBEDDING_DIM for _ in texts]


# ==================== Batch Processing ====================

def process_batch(df, batch_id):
    """Process each Spark micro-batch."""
    count = df.count()
    if count == 0:
        return

    logger.info(f"Batch {batch_id}: {count} articles")

    # Collect to driver for API calls (batches are small — controlled by maxOffsetsPerTrigger)
    rows = df.collect()

    texts = []
    articles = []
    for row in rows:
        # Skip null rows (from_json parse failures)
        if row.url is None:
            logger.warning("Skipping null row (JSON parse failure or missing fields)")
            continue

        # Deduplicate by URL
        if is_duplicate(row.url):
            logger.debug(f"Duplicate skipped: {row.url}")
            continue

        title = row.title or ""
        content = row.content or ""
        # Use title + description (content) for embedding
        text = f"{title} {content}".strip()
        if not text:
            logger.warning(f"Skipping empty article: {row.url}")
            continue

        texts.append(text)
        articles.append(row.asDict())

    if not texts:
        logger.info(f"Batch {batch_id}: all articles skipped (duplicates or empty)")
        return

    logger.info(f"Batch {batch_id}: {len(texts)} new articles to embed")

    # Call embedding API
    embeddings = call_embedding_api(texts)

    # Get singleton clients
    producer = get_kafka_producer()
    es = get_es_client()

    for i, article in enumerate(articles):
        enriched = {
            "title": article.get("title"),
            "content": article.get("content"),
            "url": article.get("url"),
            "source": article.get("source"),
            "category": article.get("category"),
            "published_at": article.get("published_at"),
            "collected_at": article.get("collected_at"),
            "processed_at": datetime.now().isoformat(),
            "embedding": embeddings[i] if i < len(embeddings) else [0.0] * EMBEDDING_DIM,
            "embedding_model": EMBEDDING_MODEL,
            "embedding_dim": EMBEDDING_DIM,
        }

        # Write to Elasticsearch (raw + processed)
        try:
            es.index(index=ES_RAW_INDEX, document=article)
            es.index(index=ES_PROCESSED_INDEX, document=enriched)
            logger.info(f"Saved to ES: {enriched['title']}")
        except Exception as e:
            logger.error(f"ES error for '{enriched['title']}': {e}")

        # Write to Kafka
        try:
            producer.send(OUTPUT_TOPIC, value=enriched)
        except Exception as e:
            logger.error(f"Kafka send error for '{enriched['title']}': {e}")

    producer.flush()
    logger.info(f"Batch {batch_id} done. Processed {len(articles)} articles.")


# ==================== Spark Session ====================

def spark_session():
    return SparkSession.builder \
        .appName("SparkEmbedder") \
        .config("spark.jars",
                "/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
                "/opt/spark/work-dir/jars/kafka-clients-3.5.0.jar,"
                "/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
                "/opt/spark/work-dir/jars/commons-pool2-2.11.1.jar") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


# ==================== Main ====================

def main():
    logger.info("Starting Spark Embedder...")

    spark = spark_session()

    schema = StructType([
        StructField("title", StringType(), True),
        StructField("content", StringType(), True),
        StructField("description", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("published_at", StringType(), True),
        StructField("collected_at", StringType(), True),
        StructField("category", StringType(), True),
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", RAW_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "20") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON, filtering out null rows (parse failures)
    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Filter out rows where JSON parsing produced all-null (malformed messages)
    parsed = parsed.filter(col("url").isNotNull())

    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark-embedder-checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
