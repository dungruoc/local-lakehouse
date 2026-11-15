#! /bin/sh
SPARK_REMOTE=sc://localhost:15002 python -m pyspark.pipelines.cli dry-run --spec non_uc_pipeline/spark-pipeline.yml
SPARK_REMOTE=sc://localhost:15002 python -m pyspark.pipelines.cli run --spec non_uc_pipeline/spark-pipeline.yml