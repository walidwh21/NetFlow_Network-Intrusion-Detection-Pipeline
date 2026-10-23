"""Read an nfdump CSV export and publish each flow to Kafka as JSON.

The record keys are the lower-case feature names expected by
flowids/stream_kafka.py. Only the fields nfdump provides are filled; the
rest are omitted and default to 0 downstream.
"""
import argparse
import json

import pandas as pd
from kafka import KafkaProducer

PROTO = {"TCP": 6, "UDP": 17, "ICMP": 1}


def pick(row, *names, default=0):
    for name in names:
        if name in row and pd.notna(row[name]):
            return row[name]
    return default


def to_record(row) -> dict:
    proto_raw = str(pick(row, "pr", "Prot", "proto", default=0)).upper()
    return {
        "src_ip": str(pick(row, "sa", "Src Addr", "SrcAddr", default="")).strip(),
        "dst_ip": str(pick(row, "da", "Dst Addr", "DstAddr", default="")).strip(),
        "l4_src_port": float(pick(row, "sp", "Src Port", "SrcPort")),
        "l4_dst_port": float(pick(row, "dp", "Dst Port", "DstPort")),
        "protocol": float(PROTO.get(proto_raw, int(proto_raw) if proto_raw.isdigit() else 0)),
        "in_bytes": float(pick(row, "ibyt", "Bytes")),
        "in_pkts": float(pick(row, "ipkt", "Packets")),
        "out_bytes": float(pick(row, "obyt")),
        "out_pkts": float(pick(row, "opkt")),
        "flow_duration_milliseconds": float(pick(row, "td", "Dur", "Duration")) * 1000,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--kafka", default="kafka:9092")
    ap.add_argument("--topic", default="netflow-data")
    args = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.kafka, retries=10, linger_ms=10,
                             value_serializer=lambda v: json.dumps(v).encode())
    flows = pd.read_csv(args.csv)
    for _, row in flows.iterrows():
        producer.send(args.topic, to_record(row))
    producer.flush()
    print(f"published {len(flows)} flows to {args.topic}")


if __name__ == "__main__":
    main()
