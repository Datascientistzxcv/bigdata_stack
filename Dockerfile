FROM openjdk:8-jre-slim

ARG BUILD_DATE
#ARG SPARK_VERSION=3.1.1
ARG SPARK_VERSION=3.0.0


# Airflow
ARG AIRFLOW_VERSION=2.0.1
ARG AIRFLOW_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS="s3,password"
ARG PYTHON_DEPS="s3 aws-requests-auth flatten_json"
ENV AIRFLOW_GPL_UNIDECODE yes
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

LABEL org.label-schema.name="Apache PySpark $SPARK_VERSION" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.version=$SPARK_VERSION

ENV PATH="/opt/miniconda3/bin:${PATH}"
ENV PYSPARK_PYTHON="/opt/miniconda3/bin/python"

RUN set -ex && \
        apt-get update && \
        apt-get install gcc g++ -y && \
        apt-get install default-libmysqlclient-dev -y && \
        apt-get install postgresql-client -y && \
        apt-get install libsasl2-dev && \
        useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow  && \
    # apt-get install python3-pip  -y && \
    apt-get install -y curl bzip2 --no-install-recommends && \
    curl -s -L --url "https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh" --output /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -f -p "/opt/miniconda3" && \
    rm /tmp/miniconda.sh && \
    conda config --set auto_update_conda true && \
    conda config --set channel_priority false && \
    conda update conda -y --force-reinstall && \
    conda install -c conda-forge pyahocorasick && \
    #pip install pandas fastavro && \
    conda install pip && \
    conda clean -tipy && \
    echo "PATH=/opt/miniconda3/bin:\${PATH}" > /etc/profile.d/miniconda.sh && \
    pip3 install --no-cache pyspark==${SPARK_VERSION} && \
    pip3 install pip install apache-airflow[crypto,celery,postgres,hive,jdbc,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} && \
    pip3 install marshmallow==2.21.0 WTForms==2.3.3 && \
    pip3 install sqlalchemy==1.3.20 && \
    SPARK_HOME=$(python /opt/miniconda3/bin/find_spark_home.py) && \
    echo "export SPARK_HOME=(python /opt/miniconda3/bin/find_spark_home.py)" > /etc/profile.d/spark.sh && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.0.0/spark-sql-kafka-0-10_2.12-3.0.0.jar" --output $SPARK_HOME/jars/spark-sql-kafka-0-10_2.12-3.0.0.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" --output $SPARK_HOME/jars/commons-pool2-2.11.1.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.0.0/kafka-clients-3.0.0.jar" --output $SPARK_HOME/jars/kafka-clients-3.0.0.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.0.0/spark-token-provider-kafka-0-10_2.12-3.0.0.jar" --output $SPARK_HOME/jars/spark-token-provider-kafka-0-10_2.12-3.0.0.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/spark/spark-tags_2.12/3.0.0/spark-tags_2.12-3.0.0.jar" --output $SPARK_HOME/jars/spark-tags_2.12-3.0.0.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar" --output $SPARK_HOME/jars/aws-java-sdk-1.7.4.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar" --output $SPARK_HOME/jars/hadoop-aws-2.7.3.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.3.3/elasticsearch-spark-30_2.12-8.3.3.jar" --output $SPARK_HOME/jars/elasticsearch-spark-30_2.12-8.3.2.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/commons/commons-collections4/4.1/commons-collections4-4.1.jar" --output $SPARK_HOME/jars/commons-collections4-4.1.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/xmlbeans/xmlbeans/3.1.0/xmlbeans-3.1.0.jar" --output $SPARK_HOME/jars/xmlbeans-3.1.0.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/org/apache/poi/ooxml-schemas/1.4/ooxml-schemas-1.4.jar" --output $SPARK_HOME/jars/ooxml-schemas-1.4.jar && \
    curl -s -L --url "https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/9.2.1.jre8/mssql-jdbc-9.2.1.jre8.jar" --output $SPARK_HOME/jars/mssql-jdbc-9.2.1.jre8.jar && \
    mkdir -p $SPARK_HOME/conf && \
    apt-get remove -y curl bzip2 && \
    apt-get autoremove -y && \
    apt-get clean
# #COPY ./entrypoint.sh /opt
# WORKDIR /opt/spark/work-dir
# #COPY ./parquet_s3.py /opt/spark/work-dir
# # RUN set -ex && pip install -r requirements.txt
# #RUN chmod 777 /opt/entrypoint.sh
# #ENTRYPOINT ["/opt/entrypoint.sh" ]
# #ENTRYPOINT ["spark-submit"]
# CMD ["/bin/sh"]

COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
# Mike Added
COPY dags ${AIRFLOW_HOME}/dags
COPY config ${AIRFLOW_HOME}/config
COPY etl ${AIRFLOW_HOME}/etl

RUN chown -R airflow: ${AIRFLOW_HOME}

EXPOSE 8080 5555 8793
USER root
RUN chmod +x entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}


ENTRYPOINT ["/entrypoint.sh"]
#ENTRYPOINT ["/bin/bash"]
CMD ["webserver"] # set default arg for entrypoint
