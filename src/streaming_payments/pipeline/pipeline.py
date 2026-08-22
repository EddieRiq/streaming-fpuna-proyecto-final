"""Primera capa del pipeline Beam: parseo, validación de contrato, timestamp de
evento y filtro de status CONFIRMED.

Testeable con DirectRunner (TestPipeline), sin KafkaIO. La lógica pura de
parseo/validación está separada de los transforms Beam para poder testearla
sin pipeline.

Todavía no implementa ventanas, deduplicación, triggers, KafkaIO ni
materializer -- eso llega en checkpoints posteriores.
"""

import json
from dataclasses import dataclass
from datetime import datetime

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.window import TimestampedValue

VALID_STATUSES = ("CONFIRMED", "DECLINED", "PENDING")

_MISSING = object()  # sentinel: distingue "falta la clave" de "está pero es inválida"


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    event: dict[str, object] | None
    errors: tuple[str, ...] = ()


# --- Parsing puro --------------------------------------------------------------


def parse_event(raw: bytes) -> dict[str, object]:
    """Decodifica UTF-8 y parsea JSON. No oculta errores: falla explícitamente
    ante UTF-8 inválido (UnicodeDecodeError), JSON inválido (json.JSONDecodeError,
    subclase de ValueError) o JSON que no representa un objeto (ValueError)."""
    text = raw.decode("utf-8")
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError(f"el evento debe ser un objeto JSON, se recibió {type(parsed).__name__}")
    return parsed


# --- Validación pura -------------------------------------------------------------


def _parse_iso8601(value: object) -> datetime | None:
    """None si `value` no es str o no es parseable. No exige UTC: cualquier
    offset válido produce un datetime timezone-aware."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def validate_event(event: dict[str, object]) -> ValidationResult:
    """Nunca lanza excepciones. Acumula todos los errores encontrados (no
    fail-fast) para que el motivo de rechazo sea completo."""
    errors: list[str] = []

    schema_version = event.get("schema_version", _MISSING)
    if schema_version is _MISSING:
        errors.append("falta 'schema_version'")
    elif type(schema_version) is not int:
        errors.append("'schema_version' debe ser int")
    elif schema_version != 1:
        errors.append(f"'schema_version' no soportado: {schema_version!r}")

    event_id = event.get("event_id", _MISSING)
    if event_id is _MISSING:
        errors.append("falta 'event_id'")
    elif not isinstance(event_id, str) or event_id == "":
        errors.append("'event_id' debe ser str no vacío")

    key = event.get("key", _MISSING)
    key_ok = isinstance(key, str) and key != ""
    if key is _MISSING:
        errors.append("falta 'key'")
    elif not key_ok:
        errors.append("'key' debe ser str no vacío")

    event_time = event.get("event_time", _MISSING)
    if event_time is _MISSING:
        errors.append("falta 'event_time'")
    elif not isinstance(event_time, str):
        errors.append("'event_time' debe ser str")
    else:
        parsed_event_time = _parse_iso8601(event_time)
        if parsed_event_time is None:
            errors.append("'event_time' no es parseable como ISO 8601")
        elif parsed_event_time.tzinfo is None:
            errors.append("'event_time' debe ser timezone-aware")

    payload = event.get("payload", _MISSING)
    merchant_id_ok = False
    merchant_id_value: object = None
    if payload is _MISSING:
        errors.append("falta 'payload'")
    elif not isinstance(payload, dict):
        errors.append("'payload' debe ser dict")
    else:
        transaction_id = payload.get("transaction_id", _MISSING)
        if transaction_id is _MISSING:
            errors.append("falta 'payload.transaction_id'")
        elif not isinstance(transaction_id, str) or transaction_id == "":
            errors.append("'payload.transaction_id' debe ser str no vacío")

        merchant_id = payload.get("merchant_id", _MISSING)
        if merchant_id is _MISSING:
            errors.append("falta 'payload.merchant_id'")
        elif not isinstance(merchant_id, str) or merchant_id == "":
            errors.append("'payload.merchant_id' debe ser str no vacío")
        else:
            merchant_id_ok = True
            merchant_id_value = merchant_id

        account_id = payload.get("account_id", _MISSING)
        if account_id is _MISSING:
            errors.append("falta 'payload.account_id'")
        elif not isinstance(account_id, str) or account_id == "":
            errors.append("'payload.account_id' debe ser str no vacío")

        amount = payload.get("amount", _MISSING)
        if amount is _MISSING:
            errors.append("falta 'payload.amount'")
        elif not isinstance(amount, int) or isinstance(amount, bool):
            errors.append("'payload.amount' debe ser int real (no bool/float)")
        elif amount <= 0:
            errors.append("'payload.amount' debe ser mayor que 0")

        currency = payload.get("currency", _MISSING)
        if currency is _MISSING:
            errors.append("falta 'payload.currency'")
        elif currency != "PYG":
            errors.append(f"'payload.currency' debe ser 'PYG', se recibió {currency!r}")

        status = payload.get("status", _MISSING)
        if status is _MISSING:
            errors.append("falta 'payload.status'")
        elif status not in VALID_STATUSES:
            errors.append(f"'payload.status' inválido: {status!r}")

    if key_ok and merchant_id_ok and key != merchant_id_value:
        errors.append("'key' debe ser igual a 'payload.merchant_id'")

    if errors:
        return ValidationResult(valid=False, event=None, errors=tuple(errors))
    return ValidationResult(valid=True, event=event, errors=())


def event_time_to_beam_timestamp(event: dict[str, object]) -> float:
    """Epoch seconds del instante UTC representado por event_time. Asume un
    evento ya validado (event_time es str ISO 8601 timezone-aware); cualquier
    offset válido representa el mismo instante absoluto."""
    parsed = _parse_iso8601(event["event_time"])
    return parsed.timestamp()


def _is_confirmed(event: dict[str, object]) -> bool:
    return event["payload"]["status"] == "CONFIRMED"


# --- Transforms Beam --------------------------------------------------------------


class ParseAndValidateFn(beam.DoFn):
    """bytes -> evento válido con timestamp Beam = event_time (salida principal)
    o registro {raw_data, errors} (salida tageada INVALID_TAG)."""

    INVALID_TAG = "invalid"

    def process(self, raw: bytes):
        try:
            event = parse_event(raw)
        except (UnicodeDecodeError, ValueError) as exc:
            yield TaggedOutput(
                self.INVALID_TAG,
                {"raw_data": raw.decode("utf-8", errors="replace"), "errors": (str(exc),)},
            )
            return

        result = validate_event(event)
        if not result.valid:
            yield TaggedOutput(
                self.INVALID_TAG, {"raw_data": event, "errors": result.errors}
            )
            return

        timestamp = event_time_to_beam_timestamp(result.event)
        yield TimestampedValue(result.event, timestamp)


def apply_parse_validate_filter(
    raw_events: beam.PCollection,
) -> tuple[beam.PCollection, beam.PCollection]:
    """Arma bytes -> parse -> validate -> filter CONFIRMED. Devuelve
    (confirmed_pcoll, invalid_pcoll). Sin ventanas, sin agregación, sin KafkaIO."""
    results = raw_events | "ParseAndValidate" >> beam.ParDo(ParseAndValidateFn()).with_outputs(
        ParseAndValidateFn.INVALID_TAG, main="valid"
    )
    confirmed = results.valid | "FilterConfirmed" >> beam.Filter(_is_confirmed)
    return confirmed, results.invalid
