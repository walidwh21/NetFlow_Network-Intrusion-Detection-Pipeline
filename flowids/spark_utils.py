"""Spark session factory shared by every job."""
from pyspark.sql import SparkSession

from flowids.config import settings


def build_session(app_name: str, *, cassandra: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.driver_memory)
        .config("spark.sql.adaptive.enabled", "true")
    )
    if cassandra:
        builder = (
            builder.config("spark.cassandra.connection.host", settings.cassandra_host)
            .config("spark.cassandra.connection.port", settings.cassandra_port)
        )
        if settings.cassandra_user:
            builder = (
                builder.config("spark.cassandra.auth.username", settings.cassandra_user)
                .config("spark.cassandra.auth.password", settings.cassandra_password)
            )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
