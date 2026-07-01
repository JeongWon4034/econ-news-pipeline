FROM apache/airflow:2.9.3-python3.11

# gold 배치를 spark 컨테이너에 위임하기 위한 docker CLI
USER root
RUN apt-get update && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# RAG / 멀티에이전트 / PyIceberg 의존성 설치
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
