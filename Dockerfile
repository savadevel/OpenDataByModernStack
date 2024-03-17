FROM apache/airflow:2.8.3

USER root
RUN apt-get update && \
    apt-get -y install git && \
    apt-get clean

USER airflow    
