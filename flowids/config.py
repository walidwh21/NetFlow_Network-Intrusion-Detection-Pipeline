"""Central, environment-driven settings.

Every value can be overridden with an environment variable so the same code
runs on a laptop, inside Docker, or on a cluster without edits.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _ports(name: str, default: str) -> frozenset:
    raw = _env(name, default)
    return frozenset(int(p) for p in raw.split(",") if p.strip())


@dataclass(frozen=True)
class Settings:
    home: Path = field(default_factory=lambda: Path(_env("FLOWIDS_HOME", ".")).resolve())

    # Cassandra
    cassandra_host: str = _env("CASSANDRA_HOST", "cassandra")
    cassandra_port: str = _env("CASSANDRA_PORT", "9042")
    cassandra_user: str = _env("CASSANDRA_USER", "")
    cassandra_password: str = _env("CASSANDRA_PASSWORD", "")
    keyspace: str = _env("CASSANDRA_KEYSPACE", "netflow")
    flows_table: str = _env("FLOWS_TABLE", "flows")
    predictions_table: str = _env("PREDICTIONS_TABLE", "predictions")

    # Spark
    spark_master: str = _env("SPARK_MASTER_URL", "local[*]")
    driver_memory: str = _env("SPARK_DRIVER_MEMORY", "2g")

    # Kafka (optional streaming mode)
    kafka_brokers: str = _env("KAFKA_BROKERS", "kafka:9092")
    kafka_topic: str = _env("KAFKA_TOPIC", "netflow-data")

    # Live capture
    interface: str = _env("CAPTURE_INTERFACE", "eth0")
    batch_size: int = int(_env("DETECT_BATCH_SIZE", "20"))
    # Traffic to/from the pipeline's own services is ignored by default.
    ignore_ports: frozenset = field(default_factory=lambda: _ports("IGNORE_PORTS", "9042,7077,8080,3000"))
    alert_threshold: float = float(_env("ALERT_THRESHOLD", "0.5"))

    @property
    def data_dir(self) -> Path:
        return Path(_env("DATA_DIR", str(self.home / "data")))

    @property
    def model_dir(self) -> Path:
        return Path(_env("MODEL_DIR", str(self.home / "models")))

    @property
    def dataset_csv(self) -> Path:
        return Path(_env("DATASET_CSV", str(self.data_dir / "sample" / "netflow_sample.csv")))

    @property
    def model_path(self) -> Path:
        return Path(_env("MODEL_PATH", str(self.model_dir / "rf_pipeline")))


settings = Settings()
