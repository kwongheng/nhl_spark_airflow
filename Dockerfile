FROM astrocrpublic.azurecr.io/runtime:3.4-1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER astro

# Pre-download the Delta Lake jars into the image so tasks never download at runtime
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; b = SparkSession.builder.master('local[1]').config('spark.ui.enabled', 'false'); s = configure_spark_with_delta_pip(b).getOrCreate(); s.stop()"