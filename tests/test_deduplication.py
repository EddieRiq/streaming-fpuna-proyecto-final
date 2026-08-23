"""Tests de deduplicación por event_id con estado y timer de Beam (Checkpoint 3C).

Usan TestPipeline (DirectRunner), sin KafkaIO y sin broker. TestStream se usa
únicamente donde hace falta controlar el watermark para el timer de expiración.
"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.timestamp import Timestamp

from streaming_payments.common.config import DEDUP_HORIZON_SECONDS
from streaming_payments.pipeline.pipeline import (
    apply_deduplication,
    apply_parse_validate_filter,
    apply_windowed_aggregation,
    event_time_to_beam_timestamp,
)
from streaming_payments.producer.producer import serialize_event


def _event(
    event_id: str,
    merchant_id: str = "merchant-001",
    event_time: str = "2026-01-01T12:00:00.000Z",
    amount: int = 10_000,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": event_id,
        "key": merchant_id,
        "event_time": event_time,
        "payload": {
            "transaction_id": f"txn-{event_id}",
            "merchant_id": merchant_id,
            "account_id": "account-001",
            "amount": amount,
            "currency": "PYG",
            "status": "CONFIRMED",
        },
    }


def _confirmed_event(event_id: str = "evt-confirmed", **kwargs) -> dict[str, object]:
    return _event(event_id, **kwargs)


def _confirmed_pcoll(p, events: list[dict[str, object]]) -> beam.PCollection:
    """PCollection de eventos ya confirmados con timestamp Beam = event_time,
    tal como llegarían a apply_deduplication en el pipeline real."""
    return (
        p
        | "Create" >> beam.Create(events)
        | "AddEventTimeTimestamp"
        >> beam.Map(lambda e: TimestampedValue(e, event_time_to_beam_timestamp(e)))
    )


# --- casos básicos ----------------------------------------------------------------


def test_first_event_is_emitted():
    event = _confirmed_event(event_id="evt-1")

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, [event])
        deduped = apply_deduplication(confirmed)
        assert_that(deduped, equal_to([event]))


def test_exact_duplicate_is_emitted_only_once():
    event = _confirmed_event(event_id="evt-1")

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, [event, event])
        deduped = apply_deduplication(confirmed)
        assert_that(deduped, equal_to([event]))


def test_different_event_ids_are_both_emitted():
    event_a = _confirmed_event(event_id="evt-a")
    event_b = _confirmed_event(event_id="evt-b")

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, [event_a, event_b])
        deduped = apply_deduplication(confirmed)
        assert_that(deduped, equal_to([event_a, event_b]))


# --- expiración del horizonte de deduplicación -------------------------------------


def test_same_event_id_does_not_reset_expiry_timer():
    first_event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:00.000Z")
    duplicate_event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:10.000Z")
    after_expiry_event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:03:01.000Z")

    ts_first = event_time_to_beam_timestamp(first_event)
    ts_duplicate = event_time_to_beam_timestamp(duplicate_event)
    ts_after_expiry = event_time_to_beam_timestamp(after_expiry_event)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts_first)
        .add_elements([TimestampedValue(first_event, ts_first)])
        .advance_watermark_to(ts_duplicate)
        .add_elements([TimestampedValue(duplicate_event, ts_duplicate)])
        .advance_watermark_to(ts_first + DEDUP_HORIZON_SECONDS + 1)
        .add_elements([TimestampedValue(after_expiry_event, ts_after_expiry)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline() as p:
        confirmed = p | test_stream
        deduped = apply_deduplication(confirmed)
        assert_that(deduped, equal_to([first_event, after_expiry_event]))


def test_event_is_accepted_again_after_dedup_horizon():
    event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:00.000Z")
    start_ts = event_time_to_beam_timestamp(event)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(start_ts)
        .add_elements([TimestampedValue(event, start_ts)])
        .advance_watermark_to(start_ts + DEDUP_HORIZON_SECONDS + 1)
        .add_elements([TimestampedValue(event, start_ts)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline() as p:
        confirmed = p | test_stream
        deduped = apply_deduplication(confirmed)
        assert_that(deduped, equal_to([event, event]))


# --- preservación del timestamp Beam -----------------------------------------------


def test_deduplication_preserves_event_timestamp():
    event = _confirmed_event(event_id="evt-1", event_time="2026-03-15T08:30:00.000Z")
    expected_timestamp = Timestamp.of(event_time_to_beam_timestamp(event))

    class _CaptureTimestamp(beam.DoFn):
        def process(self, element, timestamp=beam.DoFn.TimestampParam):
            yield (element["event_id"], timestamp)

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, [event])
        deduped = apply_deduplication(confirmed)
        tagged = deduped | "CaptureTimestamp" >> beam.ParDo(_CaptureTimestamp())
        assert_that(tagged, equal_to([(event["event_id"], expected_timestamp)]))


# --- integración: bytes -> parse/validate -> dedup -> ventana -> agregado -------


def test_duplicate_contributes_once_to_windowed_aggregate():
    event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:00.000Z", amount=25_000)
    raws = [serialize_event(event), serialize_event(event)]

    def _check(actual):
        actual = list(actual)
        assert len(actual) == 1
        aggregate = actual[0]
        assert aggregate["payload"]["transaction_count"] == 1
        assert aggregate["payload"]["total_amount_pyg"] == 25_000

    with BeamTestPipeline() as p:
        raw_events = p | "Create" >> beam.Create(raws)
        confirmed, _invalid = apply_parse_validate_filter(raw_events)
        deduped = apply_deduplication(confirmed)
        aggregates = apply_windowed_aggregation(deduped)
        assert_that(aggregates, _check)


def test_two_distinct_events_same_merchant_both_contribute():
    event_a = _confirmed_event(
        event_id="evt-a", event_time="2026-01-01T12:00:00.000Z", amount=15_000
    )
    event_b = _confirmed_event(
        event_id="evt-b", event_time="2026-01-01T12:00:10.000Z", amount=20_000
    )
    raws = [serialize_event(event_a), serialize_event(event_b)]

    def _check(actual):
        actual = list(actual)
        assert len(actual) == 1
        aggregate = actual[0]
        assert aggregate["payload"]["transaction_count"] == 2
        assert aggregate["payload"]["total_amount_pyg"] == 35_000

    with BeamTestPipeline() as p:
        raw_events = p | "Create" >> beam.Create(raws)
        confirmed, _invalid = apply_parse_validate_filter(raw_events)
        deduped = apply_deduplication(confirmed)
        aggregates = apply_windowed_aggregation(deduped)
        assert_that(aggregates, _check)
