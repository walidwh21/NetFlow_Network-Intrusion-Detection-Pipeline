-- Schema for the NetFlow IDS pipeline.
-- Column set follows the NF-CSE-CIC-IDS2018-v2 NetFlow dataset.

CREATE KEYSPACE IF NOT EXISTS netflow
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;

-- Historical / batch flows -------------------------------------------------
CREATE TABLE IF NOT EXISTS netflow.flows (
    flow_id                      BIGINT,
    src_ip                       TEXT,
    dst_ip                       TEXT,
    src_ip_int                   INT,       -- signed 32-bit form of the source IPv4
    dst_ip_int                   INT,
    src_port                     INT,
    dst_port                     INT,
    protocol                     INT,       -- 6 = TCP, 17 = UDP, 1 = ICMP
    l7_proto                     DOUBLE,
    in_bytes                     BIGINT,
    out_bytes                    BIGINT,
    in_pkts                      BIGINT,
    out_pkts                     BIGINT,
    duration_ms                  BIGINT,
    tcp_flags                    INT,
    client_tcp_flags             INT,
    server_tcp_flags             INT,
    flow_start_time              TIMESTAMP,
    src_to_dst_avg_throughput    DOUBLE,
    dst_to_src_avg_throughput    DOUBLE,
    num_pkts_up_to_128_bytes     BIGINT,
    num_pkts_128_to_256_bytes    BIGINT,
    num_pkts_256_to_512_bytes    BIGINT,
    num_pkts_512_to_1024_bytes   BIGINT,
    num_pkts_1024_to_1514_bytes  BIGINT,
    label                        INT,       -- 0 = benign, 1 = attack
    attack_type                  TEXT,
    PRIMARY KEY ((src_ip_int), flow_start_time, flow_id)
) WITH CLUSTERING ORDER BY (flow_start_time DESC, flow_id ASC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS',
                    'compaction_window_size': 1};

CREATE INDEX IF NOT EXISTS flows_label_idx       ON netflow.flows (label);
CREATE INDEX IF NOT EXISTS flows_attack_type_idx ON netflow.flows (attack_type);

-- Model output -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS netflow.predictions (
    src_ip          TEXT,
    dst_ip          TEXT,
    src_port        INT,
    dst_port        INT,
    protocol        INT,
    is_anomaly      INT,                    -- 1 when the model predicts an attack
    anomaly_score   DOUBLE,                 -- attack probability, 0-100
    ingestion_time  TIMESTAMP,
    PRIMARY KEY ((src_ip), ingestion_time, dst_ip, dst_port, src_port)
) WITH CLUSTERING ORDER BY (ingestion_time DESC, dst_ip ASC, dst_port ASC, src_port ASC);
