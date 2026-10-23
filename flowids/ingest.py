"""Load a NetFlow CSV into the Cassandra `flows` table.

    spark-submit flowids/ingest.py                       # uses DATASET_CSV
    spark-submit flowids/ingest.py --csv /app/data/my_flows.csv
"""
import argparse

from pyspark.sql import functions as F

from flowids import storage
from flowids.config import settings
from flowids.features import ADDRESS_COLUMNS, ATTACK_COLUMN, LABEL_COLUMN
from flowids.spark_utils import build_session

# CSV column -> (Cassandra column, Spark type)
COLUMN_MAP = {
    "L4_SRC_PORT": ("src_port", "int"), "L4_DST_PORT": ("dst_port", "int"),
    "PROTOCOL": ("protocol", "int"), "L7_PROTO": ("l7_proto", "double"),
    "IN_BYTES": ("in_bytes", "long"), "OUT_BYTES": ("out_bytes", "long"),
    "IN_PKTS": ("in_pkts", "long"), "OUT_PKTS": ("out_pkts", "long"),
    "FLOW_DURATION_MILLISECONDS": ("duration_ms", "long"),
    "TCP_FLAGS": ("tcp_flags", "int"), "CLIENT_TCP_FLAGS": ("client_tcp_flags", "int"),
    "SERVER_TCP_FLAGS": ("server_tcp_flags", "int"),
    "SRC_TO_DST_AVG_THROUGHPUT": ("src_to_dst_avg_throughput", "double"),
    "DST_TO_SRC_AVG_THROUGHPUT": ("dst_to_src_avg_throughput", "double"),
    "NUM_PKTS_UP_TO_128_BYTES": ("num_pkts_up_to_128_bytes", "long"),
    "NUM_PKTS_128_TO_256_BYTES": ("num_pkts_128_to_256_bytes", "long"),
    "NUM_PKTS_256_TO_512_BYTES": ("num_pkts_256_to_512_bytes", "long"),
    "NUM_PKTS_512_TO_1024_BYTES": ("num_pkts_512_to_1024_bytes", "long"),
    "NUM_PKTS_1024_TO_1514_BYTES": ("num_pkts_1024_to_1514_bytes", "long"),
}


def ipv4_to_int_expr(column: str):
    """IPv4 string -> signed 32-bit int using only built-in Spark functions."""
    pattern = r"^(\d+)\.(\d+)\.(\d+)\.(\d+)$"

    def octet(i):
        return F.regexp_extract(F.col(column), pattern, i).cast("long")

    unsigned = octet(1) * 16777216 + octet(2) * 65536 + octet(3) * 256 + octet(4)
    signed = F.when(unsigned >= 2147483648, unsigned - 4294967296).otherwise(unsigned)
    return F.coalesce(signed, F.lit(0)).cast("int")


def run(csv_path: str) -> None:
    spark = build_session("flowids-ingest")
    raw = spark.read.option("header", "true").csv(csv_path)
    src, dst = ADDRESS_COLUMNS
    df = raw.dropna(subset=[src, dst])

    df = (df.withColumn("src_ip", F.trim(F.col(src)))
            .withColumn("dst_ip", F.trim(F.col(dst))))
    df = (df.withColumn("src_ip_int", ipv4_to_int_expr("src_ip"))
            .withColumn("dst_ip_int", ipv4_to_int_expr("dst_ip")))

    for source, (target, dtype) in COLUMN_MAP.items():
        df = df.withColumn(target, F.col(source).cast(dtype))

    # `Label` is numeric (0/1) in the NF-v2 CSVs.
    df = (df.withColumn("label", F.col(LABEL_COLUMN).cast("int"))
            .withColumn("attack_type", F.col(ATTACK_COLUMN))
            .withColumn("flow_start_time", F.current_timestamp())
            .withColumn("flow_id", F.monotonically_increasing_id()))

    keep = (["flow_id", "src_ip", "dst_ip", "src_ip_int", "dst_ip_int"]
            + [t for t, _ in COLUMN_MAP.values()]
            + ["flow_start_time", "label", "attack_type"])
    out = df.select(*keep)
    print(f"Writing {out.count():,} rows to {settings.keyspace}.{settings.flows_table}")
    storage.append(out, settings.flows_table)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default=str(settings.dataset_csv))
    run(parser.parse_args().csv)
