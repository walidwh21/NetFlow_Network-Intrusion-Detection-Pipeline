import csv
from pathlib import Path

from flowids.features import (ADDRESS_COLUMNS, ATTACK_COLUMN, FEATURE_COLUMNS, FEATURE_GROUPS,
                              LABEL_COLUMN, approximate_flow_features, ip_to_int, size_buckets,
                              to_signed_int32)

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sample" / "netflow_sample.csv"


def test_feature_list_is_41_unique_columns():
    assert len(FEATURE_COLUMNS) == 41
    assert len(set(FEATURE_COLUMNS)) == 41


def test_groups_cover_every_feature_exactly_once():
    grouped = [c for cols in FEATURE_GROUPS.values() for c in cols]
    assert sorted(grouped) == sorted(FEATURE_COLUMNS)


def test_ip_to_int():
    assert ip_to_int("0.0.0.1") == 1
    assert ip_to_int("192.168.1.10") == 3232235786
    assert ip_to_int("255.255.255.255") == 2**32 - 1
    assert ip_to_int("999.1.1.1") == 0
    assert ip_to_int("not-an-ip") == 0
    assert ip_to_int(None) == 0


def test_signed_fold_matches_cassandra_int_range():
    assert to_signed_int32(ip_to_int("127.0.0.1")) == 2130706433
    assert to_signed_int32(ip_to_int("255.255.255.255")) == -1
    assert -2**31 <= to_signed_int32(ip_to_int("200.1.2.3")) < 2**31


def test_size_buckets_are_one_hot():
    for length in (40, 128, 129, 256, 400, 512, 900, 1024, 1500):
        assert sum(size_buckets(length).values()) == 1
    assert size_buckets(128)["NUM_PKTS_UP_TO_128_BYTES"] == 1
    assert size_buckets(129)["NUM_PKTS_128_TO_256_BYTES"] == 1


def test_packet_row_has_every_feature():
    row = approximate_flow_features(src_port=50000, dst_port=80, proto=6, length=60, ttl=64, tcp_flags=2)
    assert set(row) == set(FEATURE_COLUMNS)
    assert row["L7_PROTO"] == 80.0 and row["TCP_FLAGS"] == 2
    icmp = approximate_flow_features(src_port=0, dst_port=0, proto=1, length=84, ttl=64)
    assert icmp["ICMP_TYPE"] == 8


def test_sample_csv_matches_schema():
    with SAMPLE.open() as fh:
        header = next(csv.reader(fh))
    for name in (*ADDRESS_COLUMNS, LABEL_COLUMN, ATTACK_COLUMN, *FEATURE_COLUMNS):
        assert name in header, name
