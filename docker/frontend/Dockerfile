FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit==1.28.0 \
    requests==2.31.0 \
    pandas==2.1.0 \
    plotly==5.17.0 \
    python-dotenv==1.0.0

COPY frontend ./frontend

EXPOSE 8501

CMD ["streamlit", "run", "frontend/app.py", \
     "--server.address=0.0.0.0", "--server.port=8501"]
