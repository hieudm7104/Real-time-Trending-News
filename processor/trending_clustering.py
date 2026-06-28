"""
Trending & Topic Clustering
Batch Spark job: reads articles from ES, clusters by embedding vector,
assigns topic labels, and writes results back to ES.

Uses Spark MLlib KMeans (built-in, no extra deps).
Run periodically via Airflow DAG to detect emerging topics.
"""

import os
import logging
from datetime import datetime
from collections import Counter

from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==================== Config ====================
ES_HOSTS = os.getenv("ES_HOSTS", "http://elasticsearch:9200")
ES_PROCESSED_INDEX = os.getenv("ES_PROCESSED_INDEX", "news_processed")
NUM_TOPICS = int(os.getenv("NUM_TOPICS", "20"))
MIN_CLUSTER_SIZE = int(os.getenv("MIN_CLUSTER_SIZE", "3"))
MAX_DOCS = int(os.getenv("CLUSTERING_BATCH_SIZE", "500"))


# ==================== ES Helpers ====================

def fetch_unclustered_articles(es, max_docs=MAX_DOCS):
    """Fetch articles from ES that have embedding but no topic_id."""
    query = {
        "query": {
            "bool": {
                "must": [{"exists": {"field": "embedding"}}],
                "must_not": [{"exists": {"field": "topic_id"}}],
            }
        },
        "_source": ["title", "url", "category", "embedding", "top_keywords"],
        "size": max_docs,
    }

    resp = es.search(index=ES_PROCESSED_INDEX, body=query, scroll="2m")
    scroll_id = resp["_scroll_id"]
    hits = resp["hits"]["hits"]

    articles = []
    while hits:
        for hit in hits:
            src = hit["_source"]
            articles.append({
                "_id": hit["_id"],
                "title": src.get("title", ""),
                "url": src.get("url", ""),
                "category": src.get("category", ""),
                "embedding": src.get("embedding", []),
                "top_keywords": src.get("top_keywords", []),
            })

        resp = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = resp["_scroll_id"]
        hits = resp["hits"]["hits"]

    es.clear_scroll(scroll_id=scroll_id)
    logger.info(f"Fetched {len(articles)} unclustered articles from ES")
    return articles


# ==================== Cluster Analysis ====================

def analyze_cluster(articles_in_cluster, cluster_idx):
    """Analyze a cluster to find representative keywords and generate a label."""
    keyword_counter = Counter()

    for a in articles_in_cluster:
        # Aggregate top_keywords
        for kw in a.get("top_keywords", []):
            word = kw.get("word", "")
            count = kw.get("count", 1)
            if word:
                keyword_counter[word] += count

        # Also count significant words from title as fallback
        title = a.get("title", "")
        for t in title.lower().split():
            t = t.strip(".,!?;:\"'()[]{}")
            if len(t) >= 4 and not t.isdigit():
                keyword_counter[t] += 1

    top_keywords = [w for w, _ in keyword_counter.most_common(15)]
    label = " / ".join(top_keywords[:3]) if top_keywords else f"Topic-{cluster_idx}"

    return {
        "topic_id": int(cluster_idx),
        "topic_label": label,
        "topic_size": len(articles_in_cluster),
        "topic_keywords": top_keywords,
    }


def write_clusters_to_es(es, articles_with_topics):
    """Update ES documents with topic_id, topic_label, topic_keywords."""
    updated = 0
    for item in articles_with_topics:
        doc_id = item["_id"]
        topic = item["topic"]
        try:
            es.update(
                index=ES_PROCESSED_INDEX,
                id=doc_id,
                body={
                    "doc": {
                        "topic_id": topic["topic_id"],
                        "topic_label": topic["topic_label"],
                        "topic_keywords": topic["topic_keywords"],
                        "clustered_at": datetime.now().isoformat(),
                    }
                },
            )
            updated += 1
        except Exception as e:
            logger.error(f"ES update error for {doc_id}: {e}")

    logger.info(f"Updated {updated}/{len(articles_with_topics)} articles with topic info")


# ==================== Main ====================

def main():
    logger.info("Starting Trending Clustering job...")

    spark = SparkSession.builder \
        .appName("TrendingClustering") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    es = Elasticsearch(hosts=[ES_HOSTS], request_timeout=30)

    # 1. Fetch articles without topic_id
    articles = fetch_unclustered_articles(es)
    if not articles:
        logger.info("No unclustered articles found. Skipping.")
        spark.stop()
        return

    # 2. Prepare data: filter articles with valid embeddings
    data = []
    for a in articles:
        emb = a.get("embedding", [])
        if not emb or len(emb) == 0:
            continue
        data.append((a["_id"], Vectors.dense(emb)))

    if len(data) < NUM_TOPICS * 2:
        logger.info(
            f"Only {len(data)} articles with valid embeddings "
            f"(need at least {NUM_TOPICS * 2} for meaningful clusters). Skipping."
        )
        spark.stop()
        return

    # 3. Create Spark DataFrame and run KMeans
    df = spark.createDataFrame(data, ["doc_id", "features"])

    k = min(NUM_TOPICS, len(data))
    kmeans = KMeans() \
        .setK(k) \
        .setSeed(42) \
        .setMaxIter(20) \
        .setFeaturesCol("features") \
        .setPredictionCol("prediction")

    model = kmeans.fit(df)
    predictions = model.transform(df)

    # 4. Collect results and group by cluster
    rows = predictions.select("doc_id", "prediction").collect()

    cluster_groups = {}
    for row in rows:
        cid = int(row["prediction"])
        if cid not in cluster_groups:
            cluster_groups[cid] = []
        cluster_groups[cid].append(row["doc_id"])

    logger.info(f"KMeans produced {len(cluster_groups)} clusters")
    for cid in sorted(cluster_groups):
        logger.info(f"  Cluster {cid}: {len(cluster_groups[cid])} articles")

    # 5. Analyze each cluster
    articles_by_id = {a["_id"]: a for a in articles}
    articles_with_topics = []

    for cluster_id, doc_ids in cluster_groups.items():
        cluster_articles = [
            articles_by_id[did] for did in doc_ids if did in articles_by_id
        ]

        if len(cluster_articles) < MIN_CLUSTER_SIZE:
            logger.info(
                f"Cluster {cluster_id} too small "
                f"({len(cluster_articles)} < {MIN_CLUSTER_SIZE}), skipping"
            )
            continue

        topic_info = analyze_cluster(cluster_articles, cluster_id)

        for doc_id in doc_ids:
            if doc_id in articles_by_id:
                articles_with_topics.append({
                    "_id": doc_id,
                    "topic": topic_info,
                })

    if not articles_with_topics:
        logger.info("No valid clusters to write.")
        spark.stop()
        return

    # 6. Write topic assignments back to ES
    write_clusters_to_es(es, articles_with_topics)

    # Summary
    topic_summary = {}
    for item in articles_with_topics:
        tid = item["topic"]["topic_id"]
        if tid not in topic_summary:
            topic_summary[tid] = {
                "label": item["topic"]["topic_label"],
                "size": 0,
            }
        topic_summary[tid]["size"] += 1

    logger.info("=== Topic Summary ===")
    for tid in sorted(topic_summary):
        info = topic_summary[tid]
        logger.info(f"  Topic {tid}: {info['label']} ({info['size']} articles)")

    logger.info("Trending Clustering job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
