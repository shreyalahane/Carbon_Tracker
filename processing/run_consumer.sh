#!/bin/bash
cd /app
pip install kafka-python pymysql python-dotenv requests
python processing/pyspark_consumer.py