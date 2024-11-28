# FROM airflow-2.7.1-python3.9-x86_64:latest
FROM apache/airflow:2.7.1-python3.9

USER root

COPY requirements.txt /opt/airflow/

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \              
    python3-dev \              
    gcc \                      
    libpq-dev \                
    curl \                    
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
