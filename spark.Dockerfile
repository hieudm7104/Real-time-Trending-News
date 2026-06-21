FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y \
    python3-pip python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip && pip3 install --no-cache-dir \
    numpy==1.24.3 \
    pandas==2.0.3 \
    elasticsearch==8.15.0 \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    python-dotenv==1.0.0

USER spark
WORKDIR /opt/spark/work-dir

COPY processor/ /opt/spark/work-dir/processor/
COPY jars/ /opt/spark/work-dir/jars/
