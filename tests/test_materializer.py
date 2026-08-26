"""Tests unitarios del materializador, sin Kafka. SQLite real vía tmp_path."""

import json
import sqlite3

import pytest

from streaming_payments.common.config import KAFKA_BOOTSTRAP_SERVERS
from streaming_payments.materializer.materializer import (
    initialize_database,
    materialize_record,
    parse_aggregate_message,
    resolve_bootstrap_servers,
    upsert_aggregate,
    validate_kafka_key,
)


def _aggregate(
    merchant_id: str = "merchant-001",
    window_start: str = "2026-01-01T12:00:00.000Z",
    window_end: str = "2026-01-01T12:01:00.000Z",
    transaction_count: int = 1,
    total_amount_pyg: int = 10_000,
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


def _serialize(aggregate: dict[str, object]) -> bytes:
    return json.dumps(aggregate, sort_keys=True, ensure_ascii=False).encode("utf-8")


def _connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    initialize_database(connection)
    return connection


def _rows(connection: sqlite3.Connection) -> list[tuple]:
    return connection.execute(
        "SELECT aggregate_key, merchant_id, window_start, window_end, "
        "transaction_count, total_amount_pyg "
        "FROM merchant_window_aggregates ORDER BY aggregate_key"
    ).fetchall()


def test_parse_aggregate_message_accepts_valid():
    aggregate = _aggregate()
    assert parse_aggregate_message(_serialize(aggregate)) == aggregate


def test_parse_aggregate_message_rejects_invalid_utf8():
    with pytest.raises(ValueError, match="UTF-8"):
        parse_aggregate_message(b"\xff\xfe\xfd")


def test_parse_aggregate_message_rejects_invalid_json():
    with pytest.raises(ValueError, match="JSON"):
        parse_aggregate_message(b"{not valid json")


def test_parse_aggregate_message_rejects_wrong_schema_version():
    aggregate = _aggregate()
    aggregate["schema_version"] = 2
    with pytest.raises(ValueError, match="schema_version"):
        parse_aggregate_message(_serialize(aggregate))


def test_parse_aggregate_message_rejects_inconsistent_key():
    aggregate = _aggregate()
    aggregate["key"] = "merchant-999|2026-01-01T12:00:00.000Z"
    with pytest.raises(ValueError, match="inconsistente"):
        parse_aggregate_message(_serialize(aggregate))


def test_parse_aggregate_message_rejects_transaction_count_bool():
    aggregate = _aggregate()
    aggregate["payload"]["transaction_count"] = True
    with pytest.raises(ValueError, match="transaction_count"):
        parse_aggregate_message(_serialize(aggregate))


def test_parse_aggregate_message_rejects_total_amount_pyg_bool():
    aggregate = _aggregate()
    aggregate["payload"]["total_amount_pyg"] = False
    with pytest.raises(ValueError, match="total_amount_pyg"):
        parse_aggregate_message(_serialize(aggregate))


def test_validate_kafka_key_accepts_matching_key():
    aggregate = _aggregate()
    validate_kafka_key(aggregate["key"].encode("utf-8"), aggregate)


def test_validate_kafka_key_rejects_mismatched_key():
    aggregate = _aggregate()
    with pytest.raises(ValueError, match="no coincide"):
        validate_kafka_key(
            b"merchant-999|2026-01-01T12:00:00.000Z",
            aggregate,
        )


def test_validate_kafka_key_rejects_none_key():
    aggregate = _aggregate()
    with pytest.raises(ValueError, match="None"):
        validate_kafka_key(None, aggregate)


def test_initialize_database_creates_table():
    connection = sqlite3.connect(":memory:")
    initialize_database(connection)
    tables = connection.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name='merchant_window_aggregates'"
    ).fetchall()
    assert len(tables) == 1


def test_upsert_aggregate_creates_row_on_first_insert():
    connection = _connection()
    aggregate = _aggregate()
    upsert_aggregate(connection, aggregate)
    connection.commit()
    assert _rows(connection) == [
        (
            "merchant-001|2026-01-01T12:00:00.000Z",
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            1,
            10_000,
        )
    ]


def test_upsert_aggregate_distinct_keys_create_separate_rows():
    connection = _connection()
    upsert_aggregate(connection, _aggregate(merchant_id="merchant-001"))
    upsert_aggregate(connection, _aggregate(merchant_id="merchant-002"))
    connection.commit()
    assert len(_rows(connection)) == 2


def test_upsert_aggregate_money_values_are_integers():
    connection = _connection()
    upsert_aggregate(connection, _aggregate(total_amount_pyg=999_999_999))
    connection.commit()
    row = _rows(connection)[0]
    assert row[5] == 999_999_999
    assert isinstance(row[5], int)


def test_materialize_record_persists_valid_aggregate():
    connection = _connection()
    aggregate = _aggregate()
    materialize_record(
        connection,
        aggregate["key"].encode("utf-8"),
        _serialize(aggregate),
    )
    assert len(_rows(connection)) == 1


def test_materialize_record_invalid_aggregate_does_not_modify_db():
    connection = _connection()
    with pytest.raises(ValueError):
        materialize_record(connection, b"whatever", b"{not valid json")
    assert _rows(connection) == []


def test_materialize_record_kafka_key_mismatch_does_not_modify_db():
    connection = _connection()
    aggregate = _aggregate()
    with pytest.raises(ValueError, match="no coincide"):
        materialize_record(
            connection,
            b"merchant-999|2026-01-01T12:00:00.000Z",
            _serialize(aggregate),
        )
    assert _rows(connection) == []


def test_materialize_record_rolls_back_on_sqlite_failure(tmp_path):
    db_path = tmp_path / "rollback.db"
    setup_connection = sqlite3.connect(str(db_path))
    initialize_database(setup_connection)
    setup_connection.close()

    readonly_connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    aggregate = _aggregate()
    with pytest.raises(sqlite3.Error):
        materialize_record(
            readonly_connection,
            aggregate["key"].encode("utf-8"),
            _serialize(aggregate),
        )
    readonly_connection.close()

    check_connection = sqlite3.connect(str(db_path))
    assert _rows(check_connection) == []
    check_connection.close()


def test_materialize_record_exact_duplicate_is_idempotent():
    connection = _connection()
    aggregate = _aggregate()
    value = _serialize(aggregate)
    key = aggregate["key"].encode("utf-8")

    materialize_record(connection, key, value)
    materialize_record(connection, key, value)

    assert len(_rows(connection)) == 1
    assert _rows(connection)[0][4:] == (1, 10_000)


def test_materialize_record_same_key_updates_existing_row():
    connection = _connection()
    first_pane = _aggregate(transaction_count=1, total_amount_pyg=10_000)
    late_pane = _aggregate(transaction_count=2, total_amount_pyg=35_000)
    key = first_pane["key"].encode("utf-8")

    materialize_record(connection, key, _serialize(first_pane))
    materialize_record(connection, key, _serialize(late_pane))

    rows = _rows(connection)
    assert len(rows) == 1
    assert rows[0][4:] == (2, 35_000)


def test_resolve_bootstrap_servers_cli_takes_precedence(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-host:9092")
    assert resolve_bootstrap_servers("cli-host:9092") == "cli-host:9092"


def test_resolve_bootstrap_servers_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "env-host:9092")
    assert resolve_bootstrap_servers(None) == "env-host:9092"


def test_resolve_bootstrap_servers_falls_back_to_default(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    assert resolve_bootstrap_servers(None) == KAFKA_BOOTSTRAP_SERVERS
