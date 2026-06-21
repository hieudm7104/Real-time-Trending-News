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

# Config
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
                logger.error(f"Unknown API response: {list(data.keys())}")
                return [[0.0] * EMBEDDING_DIM for _ in texts]
        except Exception as e:
            logger.warning(f"API attempt {attempt+1} failed: {e}")
            time.sleep(2 ** attempt)

    logger.error("API failed after retries")
    return [[0.0] * EMBEDDING_DIM for _ in texts]


def process_batch(df, batch_id):
    """Process each Spark micro-batch."""
    count = df.count()
    if count == 0:
        return

    logger.info(f"Batch {batch_id}: {count} articles")

    # Collect to driver for API calls (batches are small - controlled by maxOffsetsPerTrigger)
    rows = df.collect()

    texts = []
    articles = []
    for row in rows:
        title = row.title or ""
        content = row.content or ""
        text = f"{title} {content}".strip()
        texts.append(text)
        articles.append(row.asDict())

    # Call embedding API
    embeddings = call_embedding_api(texts)

    # Produce to Kafka and Elasticsearch
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        compression_type="gzip",
    )
    es = Elasticsearch(hosts=[ES_HOSTS])

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
            logger.error(f"ES error: {e}")

        # Write to Kafka
        try:
            producer.send(OUTPUT_TOPIC, value=enriched)
        except Exception as e:
            logger.error(f"Kafka send error: {e}")

    producer.flush()
    producer.close()
    logger.info(f"Batch {batch_id} done")


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

    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    query = parsed.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/spark-embedder-checkpoint") \
        .trigger(processingTime="30 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
