#!/bin/bash
# Produce light, ordinary outbound traffic so the live detector has something to score.
# Run it inside the spark-master container:  docker compose exec spark-master /app/scripts/generate_traffic.sh
# Only test attack-like traffic against machines you own, on an isolated network.
for i in $(seq 1 "${1:-20}"); do
  curl -s -o /dev/null https://example.org
  sleep 1
done
