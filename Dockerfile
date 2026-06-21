FROM apache/airflow:2.6.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.7 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    feedparser==6.0.10 \
    beautifulsoup4==4.12.2 \
    python-dotenv==1.0.0

WORKDIR /opt/airflow
