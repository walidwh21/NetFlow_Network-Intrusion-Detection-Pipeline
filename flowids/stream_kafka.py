"""Score NetFlow records arriving on a Kafka topic (Structured Streaming).

Messages are JSON objects that contain `src_ip`, `dst_ip` plus the lower-case
feature names (see flowids/features.py). Missing features default to 0.
Run with the Kafka connector on the classpath, e.g.

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        flowids/stream_kafka.py
"""
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from flowids import storage
from flowids.config import settings
from flowids.features import FEATURE_COLUMNS
from flowids.spark_utils import build_session


def message_schema() -> StructType:
    fields = [StructField("src_ip", StringType()), StructField("dst_ip", StringType())]
    fields += [StructField(name.lower(), DoubleType()) for name in FEATURE_COLUMNS]
    return StructType(fields)


def main():
    spark = build_session("flowids-stream-kafka")
    model = PipelineModel.load(str(settings.model_path))

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", settings.kafka_brokers)
           .option("subscribe", settings.kafka_topic)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.select(F.from_json(F.col("value").cast("string"), message_schema()).alias("m"))
                 .select("m.*"))
    # The trained pipeline expects the original upper-case column names.
    for name in FEATURE_COLUMNS:
        parsed = parsed.withColumn(name, F.coalesce(F.col(name.lower()), F.lit(0.0)))

    def score_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        scored = model.transform(batch_df)
        out = scored.select(
            "src_ip", "dst_ip",
            F.col("L4_SRC_PORT").cast("int").alias("src_port"),
            F.col("L4_DST_PORT").cast("int").alias("dst_port"),
            F.col("PROTOCOL").cast("int").alias("protocol"),
            F.col("prediction").cast("int").alias("is_anomaly"),
            (vector_to_array("probability")[1] * 100).alias("anomaly_score"),
            F.current_timestamp().alias("ingestion_time"),
        )
        storage.append(out, settings.predictions_table)

    (parsed.writeStream.foreachBatch(score_batch)
           .option("checkpointLocation", "/tmp/flowids-checkpoints")
           .start().awaitTermination())


if __name__ == "__main__":
    main()
