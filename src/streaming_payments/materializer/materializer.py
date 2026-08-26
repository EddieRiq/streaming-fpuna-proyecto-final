"""Materializador idempotente: payments.aggregates.v1 -> SQLite (UPSERT).

Consumer/sink separado del pipeline Beam -- no usa Apache Beam. Usa
kafka-python para consumir (KafkaIO ya es obligatorio del lado del pipeline
del Bloque 2; este es un consumidor distinto) y sqlite3 de la librería
estándar para persistir, sin ORM.

Semántica de entrega: at-least-once consumption + idempotent UPSERT sink.
NO es exactly-once.

Manejo de errores: fail-fast. Ante UTF-8/JSON inválido, AggregateContract
inválido, Kafka key None/inconsistente, o error de SQLite, el proceso NO
commitea ese offset y termina con código de salida distinto de cero -- un
commit posterior podría avanzar el offset de la partición más allá del
mensaje fallido y confirmarlo indirectamente. Sin DLQ de aggregates en este
bloque: un mensaje "poison" requiere intervención manual.
"""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

from streaming_payments.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_MATERIALIZER_GROUP_ID,
    TOPIC_AGGREGATES_OUTPUT,
)

DEFAULT_DB_PATH = "data/materialized.db"

_MISSING = object()  # sentinel: distingue "falta la clave" de "está pero es inválida"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS merchant_window_aggregates (
    aggregate_key TEXT PRIMARY KEY,
    merchant_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    transaction_count INTEGER NOT NULL,
    total_amount_pyg INTEGER NOT NULL
)
"""

UPSERT_SQL = """
INSERT INTO merchant_window_aggregates (
    aggregate_key, merchant_id, window_start, window_end,
    transaction_count, total_amount_pyg
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(aggregate_key) DO UPDATE SET
    merchant_id = excluded.merchant_id,
    window_start = excluded.window_start,
    window_end = excluded.window_end,
    transaction_count = excluded.transaction_count,
    total_amount_pyg = excluded.total_amount_pyg
"""


# --- Parseo/validación puro, sin Kafka ni SQLite ----------------------------------


def parse_aggregate_message(value: bytes) -> dict[str, object]:
    """bytes Kafka -> AggregateContract validado. Lanza ValueError con un
    mensaje claro ante cualquier violación del contrato ya producido por
    kafka_pipeline.serialize_kafka_aggregate. No inventa recovery."""
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"UTF-8 inválido: {exc}") from exc

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON inválido: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError(
            f"el aggregate debe ser un objeto JSON, se recibió {type(parsed).__name__}"
        )

    schema_version = parsed.get("schema_version", _MISSING)
    if schema_version is _MISSING:
        raise ValueError("falta 'schema_version'")
    if schema_version != 1:
        raise ValueError(f"'schema_version' no soportado: {schema_version!r}")

    key = parsed.get("key", _MISSING)
    if key is _MISSING or not isinstance(key, str) or key == "":
        raise ValueError("'key' debe ser str no vacío")

    window_start = parsed.get("window_start", _MISSING)
    if window_start is _MISSING or not isinstance(window_start, str) or window_start == "":
        raise ValueError("'window_start' debe ser str no vacío")

    window_end = parsed.get("window_end", _MISSING)
    if window_end is _MISSING or not isinstance(window_end, str) or window_end == "":
        raise ValueError("'window_end' debe ser str no vacío")

    payload = parsed.get("payload", _MISSING)
    if payload is _MISSING or not isinstance(payload, dict):
        raise ValueError("'payload' debe ser dict")

    merchant_id = payload.get("merchant_id", _MISSING)
    if merchant_id is _MISSING or not isinstance(merchant_id, str) or merchant_id == "":
        raise ValueError("'payload.merchant_id' debe ser str no vacío")

    transaction_count = payload.get("transaction_count", _MISSING)
    if (
        transaction_count is _MISSING
        or not isinstance(transaction_count, int)
        or isinstance(transaction_count, bool)
        or transaction_count < 0
    ):
        raise ValueError("'payload.transaction_count' debe ser int >= 0 (no bool)")

    total_amount_pyg = payload.get("total_amount_pyg", _MISSING)
    if (
        total_amount_pyg is _MISSING
        or not isinstance(total_amount_pyg, int)
        or isinstance(total_amount_pyg, bool)
        or total_amount_pyg < 0
    ):
        raise ValueError("'payload.total_amount_pyg' debe ser int >= 0 (no bool)")

    expected_key = f"{merchant_id}|{window_start}"
    if key != expected_key:
        raise ValueError(f"'key' inconsistente: esperado {expected_key!r}, recibido {key!r}")

    return parsed


def validate_kafka_key(kafka_key: bytes | None, aggregate: dict[str, object]) -> None:
    """La key Kafka debe coincidir exactamente con aggregate['key']. Se
    rechaza None: los aggregates siempre deben salir de Beam con key real."""
    if kafka_key is None:
        raise ValueError("Kafka key es None; se esperaba una key real")
    decoded = kafka_key.decode("utf-8")
    if decoded != aggregate["key"]:
        raise ValueError(
            f"Kafka key {decoded!r} no coincide con aggregate['key'] {aggregate['key']!r}"
        )


# --- SQLite --------------------------------------------------------------------


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_TABLE_SQL)
    connection.commit()


def upsert_aggregate(connection: sqlite3.Connection, aggregate: dict[str, object]) -> None:
    payload = aggregate["payload"]
    connection.execute(
        UPSERT_SQL,
        (
            aggregate["key"],
            payload["merchant_id"],
            aggregate["window_start"],
            aggregate["window_end"],
            payload["transaction_count"],
            payload["total_amount_pyg"],
        ),
    )


def materialize_record(
    connection: sqlite3.Connection,
    kafka_key: bytes | None,
    value: bytes,
) -> None:
    """parse -> validate key -> UPSERT dentro de una transacción SQLite.
    El context manager de connection commitea si el UPSERT termina bien y
    hace rollback (re-lanzando la excepción) si falla. No hace commit de
    Kafka: eso es responsabilidad exclusiva del loop, después de que esta
    función retorne sin excepción."""
    aggregate = parse_aggregate_message(value)
    validate_kafka_key(kafka_key, aggregate)

    with connection:
        upsert_aggregate(connection, aggregate)


def open_database(db_path: str) -> sqlite3.Connection:
    """Crea el directorio padre de db_path si hace falta y devuelve una
    conexión con la tabla ya inicializada."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    initialize_database(connection)
    return connection


# --- CLI ------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="streaming_payments.materializer.materializer",
        description="Materializa payments.aggregates.v1 en SQLite vía UPSERT idempotente.",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=TOPIC_AGGREGATES_OUTPUT)
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--max-messages", type=int, default=None)
    return parser.parse_args(argv)


def resolve_bootstrap_servers(cli_value: str | None) -> str:
    if cli_value:
        return cli_value
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)


def build_kafka_consumer(bootstrap_servers: str, topic: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=KAFKA_MATERIALIZER_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )


def run(
    consumer: KafkaConsumer,
    connection: sqlite3.Connection,
    max_messages: int | None,
) -> None:
    """Un mensaje a la vez: materialize_record y, solo si no lanzó, commit
    explícito de la partición/offset de ESE registro -- no un commit global
    de todas las particiones prefetched del consumer. Si materialize_record
    lanza, la excepción se propaga tal cual -- no se atrapa acá (fail-fast)."""
    processed = 0
    for record in consumer:
        materialize_record(connection, record.key, record.value)
        topic_partition = TopicPartition(record.topic, record.partition)
        consumer.commit(offsets={topic_partition: OffsetAndMetadata(record.offset + 1, "")})
        processed += 1
        if max_messages is not None and processed >= max_messages:
            break


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    bootstrap_servers = resolve_bootstrap_servers(args.bootstrap_servers)

    consumer = None
    connection = None
    try:
        consumer = build_kafka_consumer(bootstrap_servers, args.topic)
        connection = open_database(args.db_path)
        run(consumer, connection, args.max_messages)
    except KeyboardInterrupt:
        print("materializer: interrumpido, cerrando limpiamente.", file=sys.stderr)
        return 0
    except Exception as exc:
        print(f"materializer: error fatal, offset no commiteado: {exc}", file=sys.stderr)
        return 1
    finally:
        if consumer is not None:
            consumer.close()
        if connection is not None:
            connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
