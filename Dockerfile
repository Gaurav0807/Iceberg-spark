FROM apache/spark:3.5.1

USER root

ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.2/iceberg-spark-runtime-3.5_2.12-1.9.2.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.9.2.jar

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
