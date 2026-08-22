"""Tests de integración de la primera capa del pipeline Beam.

Usan TestPipeline (DirectRunner), sin KafkaIO y sin broker.
"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.utils.timestamp import Timestamp

from streaming_payments.pipeline.pipeline import (
    ParseAndValidateFn,
    apply_parse_validate_filter,
    event_time_to_beam_timestamp,
)
from streaming_payments.producer.producer import serialize_event


def _event(status: str, event_id: str, event_time: str = "2026-01-01T12:00:00.000Z"):
    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": "merchant-001",
        "event_time": event_time,
        "payload": {
            "transaction_id": f"txn-{event_id}",
            "merchant_id": "merchant-001",
            "account_id": "account-001",
            "amount": 10_000,
            "currency": "PYG",
            "status": status,
        },
    }


def _confirmed_event(event_id: str = "evt-confirmed", **kwargs) -> dict[str, object]:
    return _event("CONFIRMED", event_id, **kwargs)


def _declined_event(event_id: str = "evt-declined") -> dict[str, object]:
    return _event("DECLINED", event_id)


def _pending_event(event_id: str = "evt-pending") -> dict[str, object]:
    return _event("PENDING", event_id)


def _run_parse_and_validate(p, raws):
    raw_events = p | "Create" >> beam.Create(raws)
    return raw_events | "ParseAndValidate" >> beam.ParDo(
        ParseAndValidateFn()
    ).with_outputs(
        ParseAndValidateFn.INVALID_TAG,
        main="valid",
    )


def _check_single_invalid(expected_raw_data, error_substring: str | None = None):
    def _check(actual):
        actual = list(actual)
        assert len(actual) == 1, f"se esperaba exactamente 1 registro inválido, hubo {actual}"
        record = actual[0]
        assert record["raw_data"] == expected_raw_data
        assert isinstance(record["errors"], tuple)
        assert len(record["errors"]) >= 1
        assert all(isinstance(e, str) and e for e in record["errors"])
        if error_substring is not None:
            assert any(error_substring in e for e in record["errors"])

    return _check


# --- parseo/validación: salida principal vs. invalid --------------------------


def test_valid_bytes_produce_valid_event_in_main_output():
    event = _confirmed_event()
    raw = serialize_event(event)

    with BeamTestPipeline() as p:
        results = _run_parse_and_validate(p, [raw])
        assert_that(results.valid, equal_to([event]), label="valid")
        assert_that(results.invalid, equal_to([]), label="invalid_empty")


def test_invalid_json_routed_to_invalid_output_with_error_message():
    raw = b"{not valid json"

    with BeamTestPipeline() as p:
        results = _run_parse_and_validate(p, [raw])
        assert_that(results.valid, equal_to([]), label="valid_empty")
        assert_that(
            results.invalid,
            _check_single_invalid(raw.decode("utf-8", errors="replace")),
            label="invalid_has_error",
        )


def test_contract_violation_routed_to_invalid_output_with_errors():
    event = _confirmed_event()
    del event["event_id"]
    raw = serialize_event(event)

    with BeamTestPipeline() as p:
        results = _run_parse_and_validate(p, [raw])
        assert_that(results.valid, equal_to([]), label="valid_empty")
        assert_that(
            results.invalid,
            _check_single_invalid(event, error_substring="event_id"),
            label="invalid_has_errors",
        )


# --- timestamp Beam = event_time ------------------------------------------------


def test_valid_event_gets_beam_timestamp_from_event_time():
    event = _confirmed_event(event_time="2026-03-15T08:30:00.000Z")
    raw = serialize_event(event)
    expected_timestamp = Timestamp.of(event_time_to_beam_timestamp(event))

    class _CaptureTimestamp(beam.DoFn):
        def process(self, element, timestamp=beam.DoFn.TimestampParam):
            yield (element["event_id"], timestamp)

    with BeamTestPipeline() as p:
        results = _run_parse_and_validate(p, [raw])
        tagged = results.valid | "CaptureTimestamp" >> beam.ParDo(_CaptureTimestamp())
        assert_that(tagged, equal_to([(event["event_id"], expected_timestamp)]))


# --- filtro CONFIRMED -----------------------------------------------------------


def test_confirmed_status_appears_in_confirmed_output():
    event = _confirmed_event()
    raw = serialize_event(event)

    with BeamTestPipeline() as p:
        raw_events = p | "Create" >> beam.Create([raw])
        confirmed, _invalid = apply_parse_validate_filter(raw_events)
        assert_that(confirmed, equal_to([event]))


def test_declined_and_pending_excluded_from_confirmed_output():
    confirmed_event = _confirmed_event(event_id="evt-c")
    declined_event = _declined_event(event_id="evt-d")
    pending_event = _pending_event(event_id="evt-p")
    raws = [serialize_event(e) for e in (confirmed_event, declined_event, pending_event)]

    with BeamTestPipeline() as p:
        raw_events = p | "Create" >> beam.Create(raws)
        confirmed, _invalid = apply_parse_validate_filter(raw_events)
        assert_that(confirmed, equal_to([confirmed_event]))
