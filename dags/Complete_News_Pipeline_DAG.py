"""
Complete News Pipeline DAG
Crawlers → Kafka → Spark (embedding API) → Elasticsearch
"""

import subprocess
import logging
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# ==================== Infra Checks ====================

def check_kafka():
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=['kafka-v4:29092'],
        value_serializer=lambda x: x.encode('utf-8'),
        request_timeout_ms=10000
    )
    producer.close()
    logger.info("Kafka OK")

def check_infra():
    check_kafka()
    logger.info("Infra OK")

# ==================== Spark Services ====================

def ensure_spark_containers():
    for name in ["spark-master-v4", "spark-worker-v4"]:
        check = subprocess.run(
            ["docker", "ps", "-q", "-f", f"name={name}"],
            capture_output=True, text=True, timeout=10
        )
        if not check.stdout.strip():
            logger.info(f"Starting {name}...")
            result = subprocess.run(
                ["docker", "start", name],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                raise Exception(f"Failed to start {name}: {result.stderr}")
            time.sleep(5)
        else:
            logger.info(f"{name} running")
    logger.info("Spark containers OK")

def ensure_spark_embedder():
    check = subprocess.run(
        ["docker", "exec", "spark-master-v4", "bash", "-lc",
         "ps aux | grep -E 'spark_embedder.py' | grep -v grep | wc -l"],
        capture_output=True, text=True, timeout=10
    )
    if check.stdout.strip() and int(check.stdout.strip()) > 0:
        logger.info("Spark embedder already running")
        return True

    logger.info("Starting Spark embedder...")
    jars = "/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar," \
           "/opt/spark/work-dir/jars/kafka-clients-3.5.0.jar," \
           "/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar," \
           "/opt/spark/work-dir/jars/commons-pool2-2.11.1.jar"

    subprocess.Popen([
        "docker", "exec", "-d", "spark-master-v4",
        "bash", "-lc",
        f"nohup /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
        f"--conf spark.jars={jars} "
        f"/opt/spark/work-dir/processor/spark_embedder.py "
        f"> /tmp/spark_embedder.out 2>&1 &"
    ])

    time.sleep(10)
    verify = subprocess.run(
        ["docker", "exec", "spark-master-v4", "bash", "-lc",
         "ps aux | grep -E 'spark_embedder.py' | grep -v grep | wc -l"],
        capture_output=True, text=True, timeout=10
    )
    if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
        raise Exception("Spark embedder failed to start")
    logger.info("Spark embedder started")

# ==================== Crawlers ====================

def ensure_crawler(name, filename):
    check = subprocess.run(
        ["docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
         f"ps aux | grep -E '{filename}' | grep -v grep | wc -l"],
        capture_output=True, text=True, timeout=10
    )
    if check.stdout.strip() and int(check.stdout.strip()) > 0:
        logger.info(f"{name} already running")
        return True

    logger.info(f"Starting {name}...")
    subprocess.Popen([
        "docker", "exec", "-d", "airflow-scheduler-v4",
        "bash", "-lc",
        f"cd /opt/airflow && nohup python3 crawler/{filename} > /tmp/{name}_crawler.out 2>&1 &"
    ])
    time.sleep(5)
    verify = subprocess.run(
        ["docker", "exec", "airflow-scheduler-v4", "bash", "-lc",
         f"ps aux | grep -E '{filename}' | grep -v grep | wc -l"],
        capture_output=True, text=True, timeout=10
    )
    if not verify.stdout.strip() or int(verify.stdout.strip()) == 0:
        raise Exception(f"{name} failed to start")
    logger.info(f"{name} started")

def ensure_vnexpress():
    return ensure_crawler("VnExpress", "Vnexpress_Crawler.py")

def ensure_kenh14():
    return ensure_crawler("Kenh14", "Kenh14_Crawler.py")

# ==================== Trending Clustering ====================

def run_trending_clustering():
    """Run batch Spark job to cluster articles by embedding and assign topic labels."""
    logger.info("Running trending clustering job...")
    result = subprocess.run(
        [
            "docker", "exec", "spark-master-v4",
            "bash", "-lc",
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.sql.adaptive.enabled=true "
            "/opt/spark/work-dir/processor/trending_clustering.py "
            "2>&1",
        ],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        logger.error(f"Clustering failed:\n{result.stderr}")
        raise Exception(f"Trending clustering failed: {result.stderr[:500]}")
    logger.info(f"Clustering output:\n{result.stdout[-1000:]}")
    logger.info("Trending clustering completed")

# ==================== Monitoring ====================

def monitor():
    from elasticsearch import Elasticsearch
    es = Elasticsearch(["http://elasticsearch:9200"])
    try:
        raw = es.count(index="news_raw")["count"]
        processed = es.count(index="news_processed")["count"]
        logger.info(f"Raw: {raw} | Processed: {processed}")
        if raw > 0:
            logger.info(f"Rate: {processed/raw*100:.1f}%")
    except Exception as e:
        logger.error(f"ES monitor error: {e}")

# ==================== DAG ====================

with DAG(
    dag_id="complete_news_pipeline_dag",
    default_args=default_args,
    description="News pipeline: Crawlers → Kafka → Spark(API) → Elasticsearch → Kibana",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["news", "spark", "lean"],
) as dag:

    start = DummyOperator(task_id="start")

    check_infra = PythonOperator(
        task_id="check_infra",
        python_callable=check_infra,
    )

    ensure_spark = PythonOperator(
        task_id="ensure_spark_containers",
        python_callable=ensure_spark_containers,
    )

    ensure_embedder = PythonOperator(
        task_id="ensure_spark_embedder",
        python_callable=ensure_spark_embedder,
    )

    crawler_vne = PythonOperator(
        task_id="ensure_vnexpress",
        python_callable=ensure_vnexpress,
    )

    crawler_k14 = PythonOperator(
        task_id="ensure_kenh14",
        python_callable=ensure_kenh14,
    )

    monitor_task = PythonOperator(
        task_id="monitor",
        python_callable=monitor,
    )

    trend_cluster = PythonOperator(
        task_id="trending_clustering",
        python_callable=run_trending_clustering,
        execution_timeout=timedelta(seconds=120),
    )

    end = DummyOperator(task_id="end")

    start >> check_infra >> ensure_spark >> ensure_embedder >> [crawler_vne, crawler_k14] >> monitor_task >> trend_cluster >> end
