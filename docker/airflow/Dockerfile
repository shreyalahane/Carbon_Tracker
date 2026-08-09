FROM apache/airflow:2.7.0

# The nightly DAG runs the Spark batch job via `docker exec pyspark_carbon`,
# which needs the Docker CLI plus the mounted docker socket.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    kafka-python==3.0.10 \
    pymysql==1.1.2 \
    pymongo==4.10.1 \
    requests==2.31.0 \
    python-dotenv==1.0.0 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    scipy==1.10.1 \
    scikit-learn==1.3.2 \
    xgboost==2.1.4 \
    mlflow==2.9.0 \
    shap==0.44.1
