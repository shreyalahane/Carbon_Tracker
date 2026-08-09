FROM jupyter/pyspark-notebook:spark-3.5.0

# The Spark batch/stream consumers need these Python libraries, which are
# not part of the base image. Baking them in avoids installing packages at
# runtime (fragile in a deployment).
USER root
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    kafka-python==2.0.2 \
    pymysql==1.1.0 \
    pymongo==4.6.0 \
    requests==2.31.0 \
    python-dotenv==1.0.0
USER jovyan
