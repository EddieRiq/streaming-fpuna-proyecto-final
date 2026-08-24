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
from apache_beam.coders import BooleanCoder
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.trigger import AccumulationMode, AfterCount, AfterWatermark
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, TimerSpec, on_timer
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from streaming_payments.common.config import (
    ALLOWED_LATENESS_SECONDS,
    DEDUP_HORIZON_SECONDS,
    WINDOW_SIZE_SECONDS,
)

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
            yield TaggedOutput(self.INVALID_TAG, {"raw_data": event, "errors": result.errors})
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


# --- Deduplicación por event_id --------------------------------------------------


def _to_event_id_kv(event: dict[str, object]) -> tuple[str, dict[str, object]]:
    """Re-key temporal para deduplicar: la key de negocio/Kafka sigue siendo
    merchant_id; este (event_id, event) es interno solo a esta etapa de Beam."""
    return event["event_id"], event


class DeduplicateByEventIdFn(beam.DoFn):
    """Deduplica por event_id con estado acotado a DEDUP_HORIZON_SECONDS.
    El horizonte se cuenta desde el timestamp de evento Beam de la primera
    aparición aceptada: un duplicado no reinicia el timer de expiración."""

    SEEN_STATE = ReadModifyWriteStateSpec("seen", BooleanCoder())
    EXPIRY_TIMER = TimerSpec("expiry", TimeDomain.WATERMARK)

    # Beam inyecta state/timer mediante marcadores en los argumentos por defecto.
    def process(
        self,
        element: tuple[str, dict[str, object]],
        seen=beam.DoFn.StateParam(SEEN_STATE),  # noqa: B008
        expiry_timer=beam.DoFn.TimerParam(EXPIRY_TIMER),  # noqa: B008
        timestamp=beam.DoFn.TimestampParam,
    ):
        _event_id, event = element
        if seen.read():
            return

        seen.write(True)
        expiry_timer.set(timestamp + DEDUP_HORIZON_SECONDS)
        yield event

    @on_timer(EXPIRY_TIMER)
    def expire(
        self,
        seen=beam.DoFn.StateParam(SEEN_STATE),  # noqa: B008
    ):
        seen.clear()


def apply_deduplication(
    confirmed_events: beam.PCollection,
) -> beam.PCollection:
    """confirmed_events -> dedup por event_id (estado acotado a
    DEDUP_HORIZON_SECONDS) -> event dict, sin WindowInto."""
    keyed = confirmed_events | "ToEventIdKV" >> beam.Map(_to_event_id_kv)
    return keyed | "DeduplicateByEventId" >> beam.ParDo(DeduplicateByEventIdFn())


# --- Agregación por ventana fija -------------------------------------------------


def _to_merchant_amount_kv(event: dict[str, object]) -> tuple[str, int]:
    """Estructura mínima por clave para CombinePerKey: (merchant_id, amount)."""
    payload = event["payload"]
    return payload["merchant_id"], payload["amount"]


class PaymentStatsCombineFn(beam.CombineFn):
    """Acumulador: (transaction_count, total_amount_pyg). Sin lista completa."""

    def create_accumulator(self) -> tuple[int, int]:
        return (0, 0)

    def add_input(self, accumulator: tuple[int, int], amount: int) -> tuple[int, int]:
        count, total = accumulator
        return (count + 1, total + amount)

    def merge_accumulators(self, accumulators) -> tuple[int, int]:
        count = 0
        total = 0
        for acc_count, acc_total in accumulators:
            count += acc_count
            total += acc_total
        return (count, total)

    def extract_output(
        self,
        accumulator: tuple[int, int],
    ) -> tuple[int, int]:
        return accumulator


def _format_window_boundary(ts: Timestamp) -> str:
    """Timestamp Beam -> ISO 8601 UTC determinista, p.ej. '2026-01-01T12:00:00.000Z'."""
    dt = ts.to_utc_datetime()
    return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{dt.microsecond // 1000:03d}Z"


class _ToAggregateContractFn(beam.DoFn):
    """(merchant_id, (count, total)) + ventana -> dict con el AggregateContract."""

    def process(self, element, window=beam.DoFn.WindowParam):
        merchant_id, (transaction_count, total_amount_pyg) = element
        window_start = _format_window_boundary(window.start)
        window_end = _format_window_boundary(window.end)
        yield {
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


def apply_windowed_aggregation(
    confirmed_events: beam.PCollection,
) -> beam.PCollection:
    """Agrega eventos CONFIRMED por merchant_id y ventana fija, con pane on-time,
    panes tardíos acumulativos dentro de ALLOWED_LATENESS_SECONDS, y descarte de
    elementos más allá de ese horizonte."""
    windowed = confirmed_events | "WindowIntoFixed" >> beam.WindowInto(
        FixedWindows(WINDOW_SIZE_SECONDS),
        trigger=AfterWatermark(late=AfterCount(1)),
        accumulation_mode=AccumulationMode.ACCUMULATING,
        allowed_lateness=ALLOWED_LATENESS_SECONDS,
    )
    keyed = windowed | "ToMerchantAmountKV" >> beam.Map(_to_merchant_amount_kv)
    combined = keyed | "CombineStatsPerKey" >> beam.CombinePerKey(PaymentStatsCombineFn())
    return combined | "ToAggregateContract" >> beam.ParDo(_ToAggregateContractFn())
