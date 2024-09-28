FROM apache/airflow:2.10.1-python3.12

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev wget && \
    wget https://download.java.net/java/GA/jdk11/openjdk-11_linux-x64_bin.tar.gz && \
    tar -xvf openjdk-11_linux-x64_bin.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    mv jdk-11 /usr/lib/jvm/java-11-openjdk-amd64 && \
    update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-11-openjdk-amd64/bin/java 1 && \
    update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-11-openjdk-amd64/bin/javac 1 && \
    rm openjdk-11_linux-x64_bin.tar.gz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


USER airflow
RUN pip install apache-airflow==2.10.1 apache-airflow-providers-openlineage==1.11.0 apache-airflow-providers-apache-spark==4.10.0 pyspark
#
USER root
#RUN apt-get install -y procps

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys A8D3785C
RUN mkdir -p /opt/spark/jars
#COPY ./postgresql-42.2.18.jar /opt/spark/jars/

USER airflow
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt
USER root
RUN apt-get update && \
    wget http://archive.apache.org/dist/hadoop/core/hadoop-3.2.2/hadoop-3.2.2.tar.gz && \
    tar -xvf hadoop-3.2.2.tar.gz && \
    rm hadoop-3.2.2.tar.gz && \
    mv hadoop-3.2.2 /usr/local/hadoop && \
    chown -R root:root /usr/local/hadoop && \
    export HADOOP_HOME=/usr/local/hadoop && \
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop && \
    export PATH=$PATH:$HADOOP_HOME/bin