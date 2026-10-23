"""Feature definitions and small, dependency-free helpers.

Nothing in this module imports Spark, so it is cheap to unit test.
"""
from typing import Dict, List

# The 41 numeric NetFlow-v2 features the classifier is trained on.
FEATURE_COLUMNS: List[str] = [
    "L4_SRC_PORT", "L4_DST_PORT", "PROTOCOL", "L7_PROTO",
    "IN_BYTES", "IN_PKTS", "OUT_BYTES", "OUT_PKTS",
    "TCP_FLAGS", "CLIENT_TCP_FLAGS", "SERVER_TCP_FLAGS",
    "FLOW_DURATION_MILLISECONDS", "DURATION_IN", "DURATION_OUT",
    "MIN_TTL", "MAX_TTL",
    "LONGEST_FLOW_PKT", "SHORTEST_FLOW_PKT", "MIN_IP_PKT_LEN", "MAX_IP_PKT_LEN",
    "SRC_TO_DST_SECOND_BYTES", "DST_TO_SRC_SECOND_BYTES",
    "RETRANSMITTED_IN_BYTES", "RETRANSMITTED_IN_PKTS",
    "RETRANSMITTED_OUT_BYTES", "RETRANSMITTED_OUT_PKTS",
    "SRC_TO_DST_AVG_THROUGHPUT", "DST_TO_SRC_AVG_THROUGHPUT",
    "NUM_PKTS_UP_TO_128_BYTES", "NUM_PKTS_128_TO_256_BYTES",
    "NUM_PKTS_256_TO_512_BYTES", "NUM_PKTS_512_TO_1024_BYTES",
    "NUM_PKTS_1024_TO_1514_BYTES",
    "TCP_WIN_MAX_IN", "TCP_WIN_MAX_OUT",
    "ICMP_TYPE", "ICMP_IPV4_TYPE",
    "DNS_QUERY_ID", "DNS_QUERY_TYPE", "DNS_TTL_ANSWER", "FTP_COMMAND_RET_CODE",
]

# Columns of the source CSV that identify the flow rather than describe it.
ADDRESS_COLUMNS = ("IPV4_SRC_ADDR", "IPV4_DST_ADDR")
LABEL_COLUMN = "Label"      # 0 = benign, 1 = attack
ATTACK_COLUMN = "Attack"    # attack family name, "Benign" for normal traffic

# Human-readable grouping used in the docs / charts.
FEATURE_GROUPS: Dict[str, List[str]] = {
    "Volume": ["IN_BYTES", "OUT_BYTES", "IN_PKTS", "OUT_PKTS"],
    "Timing": ["FLOW_DURATION_MILLISECONDS", "DURATION_IN", "DURATION_OUT"],
    "IP / TTL": ["MIN_TTL", "MAX_TTL", "MIN_IP_PKT_LEN", "MAX_IP_PKT_LEN",
                 "LONGEST_FLOW_PKT", "SHORTEST_FLOW_PKT"],
    "TCP": ["TCP_FLAGS", "CLIENT_TCP_FLAGS", "SERVER_TCP_FLAGS",
            "TCP_WIN_MAX_IN", "TCP_WIN_MAX_OUT",
            "RETRANSMITTED_IN_BYTES", "RETRANSMITTED_IN_PKTS",
            "RETRANSMITTED_OUT_BYTES", "RETRANSMITTED_OUT_PKTS"],
    "Throughput": ["SRC_TO_DST_SECOND_BYTES", "DST_TO_SRC_SECOND_BYTES",
                   "SRC_TO_DST_AVG_THROUGHPUT", "DST_TO_SRC_AVG_THROUGHPUT"],
    "Packet sizes": ["NUM_PKTS_UP_TO_128_BYTES", "NUM_PKTS_128_TO_256_BYTES",
                     "NUM_PKTS_256_TO_512_BYTES", "NUM_PKTS_512_TO_1024_BYTES",
                     "NUM_PKTS_1024_TO_1514_BYTES"],
    "Application": ["L4_SRC_PORT", "L4_DST_PORT", "PROTOCOL", "L7_PROTO", "ICMP_TYPE",
                    "ICMP_IPV4_TYPE", "DNS_QUERY_ID", "DNS_QUERY_TYPE",
                    "DNS_TTL_ANSWER", "FTP_COMMAND_RET_CODE"],
}


def ip_to_int(ip: str) -> int:
    """Dotted IPv4 -> unsigned 32-bit integer (0 when the address is invalid)."""
    try:
        a, b, c, d = (int(x) for x in ip.strip().split("."))
    except (ValueError, AttributeError):
        return 0
    if not all(0 <= octet <= 255 for octet in (a, b, c, d)):
        return 0
    return (a << 24) | (b << 16) | (c << 8) | d


def to_signed_int32(value: int) -> int:
    """Cassandra INT is signed 32-bit; fold an unsigned IPv4 integer into range."""
    return value - (1 << 32) if value >= (1 << 31) else value


def size_buckets(length: int) -> Dict[str, int]:
    """Place one packet of `length` bytes into the five size-histogram features."""
    return {
        "NUM_PKTS_UP_TO_128_BYTES": int(length <= 128),
        "NUM_PKTS_128_TO_256_BYTES": int(128 < length <= 256),
        "NUM_PKTS_256_TO_512_BYTES": int(256 < length <= 512),
        "NUM_PKTS_512_TO_1024_BYTES": int(512 < length <= 1024),
        "NUM_PKTS_1024_TO_1514_BYTES": int(length > 1024),
    }


def approximate_flow_features(*, src_port: int, dst_port: int, proto: int,
                              length: int, ttl: int, tcp_flags: int = 0) -> Dict[str, float]:
    """Build a full feature row from a SINGLE captured packet.

    A real flow exporter aggregates many packets. For the live demo we only
    see one packet at a time, so the flow-level counters are filled with
    fixed, documented placeholders. Treat the resulting scores as a demo of
    the plumbing, not as production-grade detection.
    """
    row = {name: 0.0 for name in FEATURE_COLUMNS}
    row.update({
        "L4_SRC_PORT": src_port,
        "L4_DST_PORT": dst_port,
        "PROTOCOL": proto,
        "L7_PROTO": 80.0 if dst_port in (80, 443) else 0.0,
        "IN_BYTES": length * 15, "IN_PKTS": 15,
        "OUT_BYTES": length * 12, "OUT_PKTS": 12,
        "TCP_FLAGS": tcp_flags, "CLIENT_TCP_FLAGS": tcp_flags, "SERVER_TCP_FLAGS": tcp_flags,
        "FLOW_DURATION_MILLISECONDS": 800, "DURATION_IN": 400, "DURATION_OUT": 400,
        "MIN_TTL": ttl, "MAX_TTL": ttl,
        "LONGEST_FLOW_PKT": length, "SHORTEST_FLOW_PKT": length,
        "MIN_IP_PKT_LEN": length, "MAX_IP_PKT_LEN": length,
        "SRC_TO_DST_SECOND_BYTES": length * 20.0, "DST_TO_SRC_SECOND_BYTES": length * 15.0,
        "SRC_TO_DST_AVG_THROUGHPUT": 5_000_000.0, "DST_TO_SRC_AVG_THROUGHPUT": 4_000_000.0,
        "TCP_WIN_MAX_IN": 65535, "TCP_WIN_MAX_OUT": 65535,
        "ICMP_TYPE": 8 if proto == 1 else 0, "ICMP_IPV4_TYPE": 8 if proto == 1 else 0,
    })
    row.update(size_buckets(length))
    return row
