"""Entrypoint real del pipeline Beam contra Kafka (Bloque 2 de 5).

Conecta los transforms puros de pipeline.py a Kafka real vía Apache Beam
KafkaIO (ReadFromKafka/WriteToKafka, cross-language sobre Java). No reemplaza
KafkaIO por kafka-python: eso solo se usa fuera de este módulo (producer,
smoke tests, inspección de topics).

Requiere Java 11+ disponible en el entorno (JAVA_HOME o 'java' en PATH) para
el expansion service de KafkaIO; no se hardcodea ninguna ruta de JDK acá.

El pipeline se construye y ejecuta SIEMPRE con:

    with beam.Pipeline(options=options) as p:
        ...

nunca con p.run() manual: en este entorno, la ejecución manual enruta el
transform Java a un entorno Docker que no puede reconectar con el proceso
Python (falla de red específica de Docker Desktop). El bloque `with` habilita
el loopback worker del expansion service, que sí funciona.

Garantía de entrega: at-least-once desde Kafka (KafkaIO), acotada por
deduplicación de event_id dentro de DEDUP_HORIZON_SECONDS. La key de salida
de agregados es estable (merchant_id|window_start), pero panes on-time y
tardíos se publican como mensajes separados en el mismo topic -- todavía no
hay UPSERT idempotente (eso es el materializer, Bloque 3). No es
exactly-once end-to-end.
"""

import argparse
import json
import os

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka, WriteToKafka
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

from streaming_payments.common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP_ID,
    TOPIC_AGGREGATES_OUTPUT,
    TOPIC_EVENTS_DLQ,
    TOPIC_EVENTS_INPUT,
)
from streaming_payments.pipeline.pipeline import (
    apply_deduplication,
    apply_parse_validate_filter,
    apply_windowed_aggregation,
)

# --- Serialización pura, sin Beam ------------------------------------------------


def extract_kafka_value(
    record: tuple[bytes | None, bytes | None],
) -> bytes:
    """Extrae el value leído por ReadFromKafka.

    KafkaIO declara key/value como opcionales porque Kafka admite registros
    nulos. Un value nulo se normaliza a bytes vacíos para que la validación
    existente lo trate como evento inválido y pueda enviarlo al DLQ, en vez
    de romper el pipeline.
    """
    _key, value = record
    return b"" if value is None else value


def serialize_kafka_aggregate(aggregate: dict[str, object]) -> tuple[bytes, bytes]:
    """AggregateContract -> (key, value) para WriteToKafka.

    La key es aggregate['key'] (merchant_id|window_start): estable entre el
    pane on-time y los panes tardíos, para el futuro UPSERT idempotente. El
    value es JSON UTF-8 determinista (sort_keys), sin metadata de pane."""
    key = aggregate["key"].encode("utf-8")
    value = json.dumps(aggregate, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return key, value


def _dlq_key(invalid: dict[str, object]) -> bytes:
    """event_id como key solo si raw_data es dict y tiene event_id str no
    vacío; en cualquier otro caso, bytes vacíos (WriteToKafka rechaza None,
    verificado empíricamente). No se inventa ningún id nuevo."""
    raw_data = invalid.get("raw_data")
    if isinstance(raw_data, dict):
        event_id = raw_data.get("event_id")
        if isinstance(event_id, str) and event_id:
            return event_id.encode("utf-8")
    return b""


def serialize_kafka_dlq(invalid: dict[str, object]) -> tuple[bytes, bytes]:
    """{'raw_data', 'errors'} (salida invalid de apply_parse_validate_filter)
    -> (key, value) para WriteToKafka. raw_data ya es siempre dict (evento
    parseado que no pasó validate_event) o str (fallback UTF-8 con reemplazo
    cuando falla el parseo de bytes/JSON en ParseAndValidateFn) -- ambos son
    JSON-serializables tal cual. errors es tuple en el pipeline; se normaliza
    a list solo al serializar."""
    payload = {"raw_data": invalid["raw_data"], "errors": list(invalid["errors"])}
    value = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return _dlq_key(invalid), value


# --- Wiring Beam + KafkaIO --------------------------------------------------------


def build_pipeline(
    p: beam.Pipeline,
    bootstrap_servers: str,
    input_topic: str,
    aggregates_topic: str,
    dlq_topic: str,
    max_num_records: int | None = None,
) -> None:
    """Arma bytes Kafka -> parse/validate -> dedup -> ventana/agregado -> Kafka,
    y invalid -> DLQ Kafka. No reimplementa parseo/validación/agregación.

    max_num_records es None por defecto: el pipeline productivo es unbounded.
    Pasar un valor acota ReadFromKafka para smoke/demo local, donde el
    DirectRunner solo materializa las escrituras Kafka al finalizar la
    fuente -- limitación del entorno local, no un cambio de semántica."""
    raw_records = p | "ReadFromKafka" >> ReadFromKafka(
        consumer_config={
            "bootstrap.servers": bootstrap_servers,
            "group.id": KAFKA_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
        },
        topics=[input_topic],
        max_num_records=max_num_records,
    )
    raw_values = raw_records | "ExtractValue" >> beam.Map(extract_kafka_value)
    confirmed, invalid = apply_parse_validate_filter(raw_values)
    deduped = apply_deduplication(confirmed)
    aggregates = apply_windowed_aggregation(deduped)

    (
        aggregates
        | "SerializeAggregate" >> beam.Map(serialize_kafka_aggregate)
        | "WriteAggregates"
        >> WriteToKafka(
            producer_config={"bootstrap.servers": bootstrap_servers},
            topic=aggregates_topic,
        )
    )
    (
        invalid
        | "SerializeDlq" >> beam.Map(serialize_kafka_dlq)
        | "WriteDlq"
        >> WriteToKafka(
            producer_config={"bootstrap.servers": bootstrap_servers},
            topic=dlq_topic,
        )
    )


# --- CLI ------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        prog="streaming_payments.pipeline.kafka_pipeline",
        description="Pipeline Beam real: Kafka -> parse/validate/dedup/window/aggregate -> Kafka.",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--input-topic", default=TOPIC_EVENTS_INPUT)
    parser.add_argument("--aggregates-topic", default=TOPIC_AGGREGATES_OUTPUT)
    parser.add_argument("--dlq-topic", default=TOPIC_EVENTS_DLQ)
    parser.add_argument("--max-num-records", type=int, default=None)
    return parser.parse_known_args(argv)


def resolve_bootstrap_servers(cli_value: str | None) -> str:
    if cli_value:
        return cli_value
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS)


def main(argv: list[str] | None = None) -> int:
    args, pipeline_argv = parse_args(argv)
    bootstrap_servers = resolve_bootstrap_servers(args.bootstrap_servers)

    options = PipelineOptions(pipeline_argv)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        build_pipeline(
            p,
            bootstrap_servers,
            args.input_topic,
            args.aggregates_topic,
            args.dlq_topic,
            args.max_num_records,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
