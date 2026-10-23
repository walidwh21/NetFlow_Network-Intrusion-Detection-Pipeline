"""Thin Cassandra read/write helpers on top of the Spark connector."""
from pyspark.sql import DataFrame, SparkSession

from flowids.config import settings

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"


def append(df: DataFrame, table: str) -> None:
    (df.write.format(CASSANDRA_FORMAT)
       .options(table=table, keyspace=settings.keyspace)
       .mode("append").save())


def read(spark: SparkSession, table: str) -> DataFrame:
    return (spark.read.format(CASSANDRA_FORMAT)
            .options(table=table, keyspace=settings.keyspace).load())
