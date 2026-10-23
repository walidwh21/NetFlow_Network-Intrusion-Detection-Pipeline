"""Sniff live packets, score them in micro-batches, store results in Cassandra.

Needs raw-socket rights (the Docker service runs as root) and `scapy`.

    spark-submit flowids/detect_live.py --iface eth0 --batch-size 20
"""
import argparse
import signal
import sys
from datetime import datetime

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from flowids import storage
from flowids.config import settings
from flowids.features import FEATURE_COLUMNS, approximate_flow_features
from flowids.spark_utils import build_session


def packet_to_record(pkt, ignore_ports):
    """Return (meta, feature_row) for an IP packet, or None to skip it."""
    from scapy.all import ICMP, IP, TCP, UDP  # imported lazily: scapy is optional at import time

    if not pkt.haslayer(IP):
        return None
    ip = pkt[IP]
    src_port = dst_port = tcp_flags = 0
    proto = int(ip.proto)
    if pkt.haslayer(TCP):
        src_port, dst_port, tcp_flags = pkt[TCP].sport, pkt[TCP].dport, int(pkt[TCP].flags)
    elif pkt.haslayer(UDP):
        src_port, dst_port = pkt[UDP].sport, pkt[UDP].dport
    elif pkt.haslayer(ICMP):
        proto = 1
    if src_port in ignore_ports or dst_port in ignore_ports:
        return None

    meta = {"src_ip": ip.src, "dst_ip": ip.dst, "src_port": int(src_port),
            "dst_port": int(dst_port), "protocol": proto}
    features = approximate_flow_features(
        src_port=src_port, dst_port=dst_port, proto=proto,
        length=len(pkt), ttl=int(ip.ttl), tcp_flags=tcp_flags)
    return meta, features


class MicroBatcher:
    """Collects packets and scores them with one Spark job per batch."""

    def __init__(self, spark, model, batch_size, threshold):
        self.spark, self.model = spark, model
        self.batch_size, self.threshold = batch_size, threshold
        self.buffer = []

    def add(self, record):
        self.buffer.append(record)
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.buffer:
            return
        rows = [{**meta, **feats} for meta, feats in self.buffer]
        count, self.buffer = len(rows), []
        df = self.spark.createDataFrame(rows)
        for name in FEATURE_COLUMNS:
            df = df.withColumn(name, F.col(name).cast("double"))
        scored = self.model.transform(df)
        out = scored.select(
            "src_ip", "dst_ip", "src_port", "dst_port", "protocol",
            F.col("prediction").cast("int").alias("is_anomaly"),
            (vector_to_array("probability")[1] * 100).alias("anomaly_score"),
            F.current_timestamp().alias("ingestion_time"),
        )
        storage.append(out, settings.predictions_table)
        alerts = out.filter(F.col("anomaly_score") >= self.threshold * 100).count()
        print(f"[{datetime.now():%H:%M:%S}] scored {count} packets, {alerts} above threshold")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iface", default=settings.interface)
    parser.add_argument("--batch-size", type=int, default=settings.batch_size)
    parser.add_argument("--threshold", type=float, default=settings.alert_threshold,
                        help="probability (0-1) at which a packet is reported as an alert")
    args = parser.parse_args()

    spark = build_session("flowids-detect-live")
    model = PipelineModel.load(str(settings.model_path))
    batcher = MicroBatcher(spark, model, args.batch_size, args.threshold)

    def shutdown(*_):
        batcher.flush()
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    print(f"Sniffing on {args.iface} (ignoring ports {sorted(settings.ignore_ports)}) - Ctrl+C to stop")
    from scapy.all import sniff

    try:
        sniff(iface=args.iface, store=False,
              prn=lambda p: (lambda r: r and batcher.add(r))(packet_to_record(p, settings.ignore_ports)))
    except KeyboardInterrupt:
        pass
    finally:
        shutdown()


if __name__ == "__main__":
    main()
