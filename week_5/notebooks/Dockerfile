FROM jupyter/all-spark-notebook:python-3.8

USER root

RUN set -e; \
    wget -q -O spark.tgz https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz && \
    tar -xzf spark.tgz && \
    mv spark-3.3.2-bin-hadoop3 /usr/local/ && \
    rm spark.tgz && \
    rm -rf /usr/local/spark /usr/local/spark-3.3.0-bin-hadoop3 && \
    ln -s /usr/local/spark-3.3.2-bin-hadoop3 /usr/local/spark

RUN wget -q -O s3-2.18.41.jar https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar && \
    wget -q -O aws-java-sdk-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar && \
    wget -q -O aws-java-sdk-bundle-1.11.1026.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    wget -q -O delta-core_2.12-2.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar && \
    wget -q -O delta-storage-2.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar && \
    wget -q -O hadoop-aws-3.3.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    mv s3-2.18.41.jar aws-java-sdk-1.12.367.jar aws-java-sdk-bundle-1.11.1026.jar delta-core_2.12-2.2.0.jar delta-storage-2.2.0.jar hadoop-aws-3.3.2.jar /usr/local/spark/jars/
