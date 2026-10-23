# Convenience targets. All Spark jobs run inside the spark-master container.
EXEC = docker compose exec spark-master
SUBMIT = $(EXEC) spark-submit --master local[*]

.PHONY: up down init ingest train detect stream test clean

up:            ## start Cassandra, Spark and Grafana (schema is created automatically)
	docker compose up -d --build

ingest:        ## load the sample CSV into Cassandra
	$(SUBMIT) /app/flowids/ingest.py

train:         ## train the Random Forest, save models/rf_pipeline + metrics.json
	$(SUBMIT) /app/flowids/train.py

detect:        ## sniff live traffic inside the container and score it
	$(SUBMIT) /app/flowids/detect_live.py

stream:        ## score flows arriving on Kafka (needs: docker compose --profile streaming up -d)
	$(SUBMIT) --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/flowids/stream_kafka.py

test:          ## unit tests (no Spark needed)
	python3 -m pytest -q

down:
	docker compose down

clean:         ## also delete volumes (Cassandra and Grafana data)
	docker compose down -v
