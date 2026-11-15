#! /bin/sh

python prepare_medallion.py
SPARK_REMOTE=sc://localhost:15002 python -m pyspark.pipelines.cli dry-run --spec pipeline/spark-pipeline.yml
SPARK_REMOTE=sc://localhost:15002 python -m pyspark.pipelines.cli run --spec pipeline/spark-pipeline.yml