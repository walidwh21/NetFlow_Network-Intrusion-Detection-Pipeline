#!/bin/bash
# Rotate pcap files with tcpdump, convert each to NetFlow with nfdump,
# export CSV and publish the flows to Kafka.
set -euo pipefail

IFACE="${CAPTURE_INTERFACE:-eth0}"
KAFKA="${KAFKA_BROKERS:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-netflow-data}"
PCAP_DIR=/work/pcap; FLOW_DIR=/work/flows; CSV_DIR=/work/csv
mkdir -p "$PCAP_DIR" "$FLOW_DIR" "$CSV_DIR"

echo "capturing on $IFACE (10 x 100 MB ring buffer)"
tcpdump -i "$IFACE" -s 0 -W 10 -C 100 -w "$PCAP_DIR/cap-%Y%m%d%H%M.pcap" &

inotifywait -m -e create -e moved_to "$PCAP_DIR" | while read -r dir _ file; do
  [[ "$file" == *.pcap ]] || continue
  sleep 5                                  # let tcpdump close the previous file
  pcap="$dir$file"; stamp=$(date +%s)
  rm -f "$FLOW_DIR"/nfcapd.*
  nfpcapd -r "$pcap" -l "$FLOW_DIR/" -t 60 -w
  nfdump -R "$FLOW_DIR" -o csv > "$CSV_DIR/flows_$stamp.csv"
  python3 /opt/flow_exporter.py --csv "$CSV_DIR/flows_$stamp.csv" --kafka "$KAFKA" --topic "$TOPIC"
  rm -f "$pcap"
done
