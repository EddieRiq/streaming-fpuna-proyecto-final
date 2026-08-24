"""Tests unitarios de la serialización/CLI de kafka_pipeline.py, sin broker.

No arrancan KafkaIO ni el expansion service (requieren Java real): eso se
valida con el smoke test contra Kafka real, aparte.
"""

import json

from streaming_payments.common.config import KAFKA_BOOTSTRAP_SERVERS
from streaming_payments.pipeline.kafka_pipeline import (
    extract_kafka_value,
    parse_args,
    resolve_bootstrap_servers,
    serialize_kafka_aggregate,
    serialize_kafka_dlq,
)


def _aggregate(
    merchant_id: str = "merchant-001",
    window_start: str = "2026-01-01T12:00:00.000Z",
    window_end: str = "2026-01-01T12:01:00.000Z",
    transaction_count: int = 2,
    total_amount_pyg: int = 35_000,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "key": f"{merchant_id}|{window_start}",
        "window_start": window_start,
        "window_end": window_end,
        "payload": {
            "merchant_id": merchant_id,
            "transaction_count": transaction_count,
            "total_amount_pyg": total_amount_pyg,
        },
    }


# --- extract_kafka_value ----------------------------------------------------------


def test_extract_kafka_value_returns_value_bytes():
    record = (b"merchant-001", b'{"hello":"world"}')
    assert extract_kafka_value(record) == b'{"hello":"world"}'


def test_extract_kafka_value_normalizes_none_to_empty_bytes():
    assert extract_kafka_value((b"merchant-001", None)) == b""


# --- serialize_kafka_aggregate -----------------------------------------------------


def test_serialize_kafka_aggregate_key_matches_aggregate_key_field():
    aggregate = _aggregate()
    key, _value = serialize_kafka_aggregate(aggregate)
    assert key == aggregate["key"].encode("utf-8")
    assert key == b"merchant-001|2026-01-01T12:00:00.000Z"


def test_serialize_kafka_aggregate_value_is_json_roundtrip():
    aggregate = _aggregate()
    _key, value = serialize_kafka_aggregate(aggregate)
    assert json.loads(value.decode("utf-8")) == aggregate


def test_serialize_kafka_aggregate_amounts_stay_int():
    aggregate = _aggregate(total_amount_pyg=999_999_999)
    _key, value = serialize_kafka_aggregate(aggregate)
    decoded = json.loads(value.decode("utf-8"))
    assert decoded["payload"]["total_amount_pyg"] == 999_999_999
    assert isinstance(decoded["payload"]["total_amount_pyg"], int)
    assert isinstance(decoded["payload"]["transaction_count"], int)


# --- serialize_kafka_dlq ------------------------------------------------------------


def test_serialize_kafka_dlq_key_uses_event_id_when_present():
    invalid = {
        "raw_data": {"event_id": "evt-123", "key": "merchant-001"},
        "errors": ("falta 'payload'",),
    }
    key, _value = serialize_kafka_dlq(invalid)
    assert key == b"evt-123"


def test_serialize_kafka_dlq_key_is_empty_bytes_when_event_id_missing():
    invalid = {"raw_data": {"key": "merchant-001"}, "errors": ("falta 'event_id'",)}
    key, _value = serialize_kafka_dlq(invalid)
    assert key == b""


def test_serialize_kafka_dlq_value_serializes_errors_as_list():
    invalid = {"raw_data": {"event_id": "evt-1"}, "errors": ("error uno", "error dos")}
    _key, value = serialize_kafka_dlq(invalid)
    decoded = json.loads(value.decode("utf-8"))
    assert decoded["errors"] == ["error uno", "error dos"]
    assert decoded["raw_data"] == {"event_id": "evt-1"}


def test_serialize_kafka_dlq_handles_str_raw_data():
    """raw_data no-dict real que produce ParseAndValidateFn: str decodificado
    con errors='replace' cuando falla el parseo de bytes/JSON crudos."""
    invalid = {
        "raw_data": "{not valid json",
        "errors": ("Expecting property name enclosed in double quotes",),
    }
    key, value = serialize_kafka_dlq(invalid)
    assert key == b""
    decoded = json.loads(value.decode("utf-8"))
    assert decoded["raw_data"] == "{not valid json"
    assert decoded["errors"] == ["Expecting property name enclosed in double quotes"]


# --- resolve_bootstrap_servers: precedencia CLI > env > default -------------------


def test_resolve_bootstrap_servers_cli_takes_precedence(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-host:9092")
    assert resolve_bootstrap_servers("cli-host:9092") == "cli-host:9092"


def test_resolve_bootstrap_servers_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-host:9092")
    assert resolve_bootstrap_servers(None) == "env-host:9092"


def test_resolve_bootstrap_servers_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    assert resolve_bootstrap_servers(None) == KAFKA_BOOTSTRAP_SERVERS


# --- parse_args: --max-num-records --------------------------------------------------


def test_parse_args_max_num_records_defaults_to_none():
    args, _pipeline_argv = parse_args([])
    assert args.max_num_records is None


def test_parse_args_max_num_records_parses_int():
    args, _pipeline_argv = parse_args(["--max-num-records", "10"])
    assert args.max_num_records == 10
