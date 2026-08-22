"""Productor sintético de eventos de pago: generación reproducible y escritura en Kafka.

La generación de eventos (pura, sin red) está separada de la escritura en Kafka,
para poder testear la primera sin necesidad de un broker.
"""

import argparse
import copy
import json
import os
import random
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from kafka import KafkaProducer

from streaming_payments.common.config import (
    ALLOWED_LATENESS_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_EVENTS_INPUT,
    WINDOW_SIZE_SECONDS,
)
from streaming_payments.common.contracts import EventContract, PaymentPayload

# Ancla de tiempo fija para que la generación pura sea determinista respecto de
# seed + start_time, sin depender del reloj del sistema (datetime.now()).
DEFAULT_BASE_TIME = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

DEMO_SEED = 1337

MERCHANT_IDS = ("merchant-001", "merchant-002", "merchant-003")
ACCOUNT_IDS = tuple(f"account-{i:03d}" for i in range(1, 11))
# El pipeline posterior agregará solamente pagos CONFIRMED.
STATUSES = ("CONFIRMED", "DECLINED", "PENDING")
AMOUNT_MIN_PYG = 1_000
AMOUNT_MAX_PYG = 500_000


@dataclass(frozen=True)
class PlannedEvent:
    """Un evento a publicar más metadata de control del generador.

    `event` es JSON-compatible genérico (no EventContract) porque el escenario
    demo incluye a propósito un evento que no cumple el contrato. `scenario_tag`
    y `note` son solo para logging/demo y nunca deben aparecer dentro de `event`.
    """

    event: dict[str, object]
    scenario_tag: str
    note: str = ""


# --- Generación pura ---------------------------------------------------------


def _iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("event_time debe ser timezone-aware")
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _new_transaction_id(rng: random.Random) -> str:
    return f"txn-{rng.getrandbits(64):016x}"


def _new_event_id(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generate_event(
    rng: random.Random,
    event_time: datetime,
    merchant_id: str,
    account_id: str,
    amount: int,
    status: str,
    transaction_id: str | None = None,
) -> EventContract:
    """Construye un evento válido que respeta exactamente EventContract."""
    if transaction_id is None:
        transaction_id = _new_transaction_id(rng)
    payload: PaymentPayload = {
        "transaction_id": transaction_id,
        "merchant_id": merchant_id,
        "account_id": account_id,
        "amount": amount,
        "currency": "PYG",
        "status": status,
    }
    return {
        "schema_version": 1,
        "event_id": _new_event_id(rng),
        "key": merchant_id,
        "event_time": _iso_utc(event_time),
        "payload": payload,
    }


def generate_normal_stream(
    seed: int, count: int, start_time: datetime | None = None
) -> list[EventContract]:
    """Genera `count` eventos válidos, reproducibles a partir de seed + start_time.

    No usa el reloj del sistema: si start_time es None, se usa DEFAULT_BASE_TIME.
    """
    rng = random.Random(seed)
    base = start_time if start_time is not None else DEFAULT_BASE_TIME
    events: list[EventContract] = []
    for i in range(count):
        event_time = base + timedelta(seconds=2 * i)
        merchant_id = rng.choice(MERCHANT_IDS)
        account_id = rng.choice(ACCOUNT_IDS)
        amount = rng.randint(AMOUNT_MIN_PYG, AMOUNT_MAX_PYG)
        status = rng.choice(STATUSES)
        events.append(generate_event(rng, event_time, merchant_id, account_id, amount, status))
    return events


def _make_invalid_missing_event_id(valid_event: EventContract) -> dict[str, object]:
    broken = copy.deepcopy(dict(valid_event))
    del broken["event_id"]
    return broken


def build_demo_scenario(
    seed: int = DEMO_SEED, base_time: datetime | None = None
) -> list[PlannedEvent]:
    """Escenario demo determinista: normales, duplicado, fuera de orden, tardío e inválido.

    E1..E5  normales, event_time = base + {0,2,4,6,8}s
    E6      out_of_order, event_time = base + 3s (anterior al de E5, el enviado justo antes)
    E7      duplicate, copia exacta de E3 (mismo event_id, mismo status CONFIRMED)
    E8      late, event_time muy anterior a la ventana + lateness permitido
    E9      invalid, copia de un evento válido sin la clave event_id
    E10     normal, event_time = base + 10s

    Los status de los eventos del demo son explícitos (no rng.choice), para
    garantizar que E3/E7 sean CONFIRMED y así poder demostrar más adelante que
    la deduplicación evita sumar dos veces ese pago.
    """
    base = base_time if base_time is not None else DEFAULT_BASE_TIME
    rng = random.Random(seed)

    def _normal_at(offset_seconds: int, status: str) -> EventContract:
        event_time = base + timedelta(seconds=offset_seconds)
        merchant_id = rng.choice(MERCHANT_IDS)
        account_id = rng.choice(ACCOUNT_IDS)
        amount = rng.randint(AMOUNT_MIN_PYG, AMOUNT_MAX_PYG)
        return generate_event(rng, event_time, merchant_id, account_id, amount, status)

    e1 = _normal_at(0, "CONFIRMED")
    e2 = _normal_at(2, "DECLINED")
    e3 = _normal_at(4, "CONFIRMED")
    e4 = _normal_at(6, "PENDING")
    e5 = _normal_at(8, "CONFIRMED")
    e6 = _normal_at(3, "CONFIRMED")
    e7 = copy.deepcopy(e3)

    late_time = base - timedelta(seconds=WINDOW_SIZE_SECONDS + ALLOWED_LATENESS_SECONDS + 30)
    late_merchant_id = rng.choice(MERCHANT_IDS)
    late_account_id = rng.choice(ACCOUNT_IDS)
    late_amount = rng.randint(AMOUNT_MIN_PYG, AMOUNT_MAX_PYG)
    e8 = generate_event(
        rng, late_time, late_merchant_id, late_account_id, late_amount, "CONFIRMED"
    )

    e9 = _make_invalid_missing_event_id(_normal_at(9, "CONFIRMED"))
    e10 = _normal_at(10, "CONFIRMED")

    return [
        PlannedEvent(e1, "normal"),
        PlannedEvent(e2, "normal"),
        PlannedEvent(e3, "normal"),
        PlannedEvent(e4, "normal"),
        PlannedEvent(e5, "normal"),
        PlannedEvent(
            e6,
            "out_of_order",
            note="event_time anterior al del evento inmediatamente enviado antes (E5)",
        ),
        PlannedEvent(e7, "duplicate", note="copia exacta de E3, mismo event_id"),
        PlannedEvent(
            e8,
            "late",
            note=(
                "diseñado para llegar muy atrasado; si el pipeline lo descarta o no "
                "depende del watermark, se valida más adelante con TestStream"
            ),
        ),
        PlannedEvent(e9, "invalid", note="falta event_id"),
        PlannedEvent(e10, "normal"),
    ]


# --- Serialización de transporte ---------------------------------------------


def serialize_event(event: dict[str, object]) -> bytes:
    """JSON UTF-8 del evento, sin ocultar la serialización detrás de kafka-python."""
    return json.dumps(event, ensure_ascii=False).encode("utf-8")


def serialize_key(event: dict[str, object]) -> bytes:
    """UTF-8 de la key Kafka, que es merchant_id (campo `key` del contrato).

    No hace coerción silenciosa: si `key` no es str, falla explícitamente.
    """
    key = event["key"]
    if not isinstance(key, str):
        raise TypeError(f"event['key'] debe ser str, recibido {type(key).__name__}")
    return key.encode("utf-8")


# --- Escritura en Kafka --------------------------------------------------------


class _ProducerLike(Protocol):
    def send(self, topic: str, key: bytes, value: bytes) -> object: ...

    def flush(self) -> None: ...


class KafkaEventWriter:
    """Envía PlannedEvent ya serializados a bytes; hace flush una sola vez al final.

    No crea ni cierra el producer: lo recibe inyectado. El cierre es
    responsabilidad de quien lo crea (ver build_kafka_producer / main).
    """

    def __init__(self, producer: _ProducerLike, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def send(self, planned: PlannedEvent) -> None:
        key = serialize_key(planned.event)
        value = serialize_event(planned.event)
        self._producer.send(self._topic, key=key, value=value)

    def flush(self) -> None:
        self._producer.flush()

    def publish(self, planned_events: Iterable[PlannedEvent]) -> None:
        for planned in planned_events:
            self.send(planned)
        self.flush()


def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """Construye el KafkaProducer real, sin value_serializer/key_serializer.

    La serialización a bytes ya la hacen serialize_event/serialize_key. Quien
    llama a esta función es dueño del producer resultante y debe cerrarlo.
    """
    return KafkaProducer(bootstrap_servers=bootstrap_servers)


# --- CLI ------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="streaming_payments.producer.producer",
        description="Productor sintético de eventos de pago para Kafka.",
    )
    parser.add_argument("--scenario", choices=["normal", "demo"], default="normal")
    parser.add_argument("--count", type=int, default=50, help="solo aplica a --scenario normal")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--start-now",
        action="store_true",
        help="usa datetime.now(UTC) (calculado una sola vez) en vez de DEFAULT_BASE_TIME",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=TOPIC_EVENTS_INPUT)
    parser.add_argument("--dry-run", action="store_true", help="imprime sin enviar a Kafka")
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="pausa cosmética entre envíos; no altera event_time",
    )
    return parser.parse_args(argv)


def resolve_bootstrap_servers(cli_value: str | None) -> str:
    if cli_value:
        return cli_value
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)


def _planned_events_for(
    args: argparse.Namespace, start_time: datetime | None
) -> list[PlannedEvent]:
    if args.scenario == "demo":
        seed = args.seed if args.seed is not None else DEMO_SEED
        return build_demo_scenario(seed=seed, base_time=start_time)
    seed = args.seed if args.seed is not None else 0
    return [
        PlannedEvent(event, "normal")
        for event in generate_normal_stream(seed, args.count, start_time=start_time)
    ]


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    start_time = datetime.now(UTC).replace(microsecond=0) if args.start_now else None
    planned = _planned_events_for(args, start_time)

    if args.dry_run:
        for p in planned:
            print(serialize_event(p.event).decode("utf-8"))
        return 0

    bootstrap_servers = resolve_bootstrap_servers(args.bootstrap_servers)
    producer = build_kafka_producer(bootstrap_servers)
    try:
        writer = KafkaEventWriter(producer, args.topic)
        for p in planned:
            writer.send(p)
            print(f"[{p.scenario_tag}] event_id={p.event.get('event_id', '<missing>')}")
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000)
        writer.flush()
    finally:
        producer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
