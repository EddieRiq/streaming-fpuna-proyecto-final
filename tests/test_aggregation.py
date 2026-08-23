"""Tests de agregación por ventana fija (Checkpoint 3B).

Usan TestPipeline (DirectRunner), sin KafkaIO y sin broker. No prueban
duplicados ni eventos tardíos -- eso queda para el checkpoint con TestStream.
"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from streaming_payments.pipeline.pipeline import (
    PaymentStatsCombineFn,
    apply_parse_validate_filter,
    apply_windowed_aggregation,
    event_time_to_beam_timestamp,
)
from streaming_payments.producer.producer import serialize_event


def _event(
    status: str,
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
            "status": status,
        },
    }


def _confirmed_event(event_id: str = "evt-confirmed", **kwargs) -> dict[str, object]:
    return _event("CONFIRMED", event_id, **kwargs)


def _declined_event(event_id: str = "evt-declined", **kwargs) -> dict[str, object]:
    return _event("DECLINED", event_id, **kwargs)


def _pending_event(event_id: str = "evt-pending", **kwargs) -> dict[str, object]:
    return _event("PENDING", event_id, **kwargs)


def _aggregate(
    merchant_id: str,
    window_start: str,
    window_end: str,
    transaction_count: int,
    total_amount_pyg: int,
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


def _confirmed_pcoll(p, events: list[dict[str, object]]) -> beam.PCollection:
    """PCollection de eventos ya confirmados con timestamp Beam = event_time,
    tal como llegarían a apply_windowed_aggregation en el pipeline real."""
    return (
        p
        | "Create" >> beam.Create(events)
        | "AddEventTimeTimestamp"
        >> beam.Map(lambda e: TimestampedValue(e, event_time_to_beam_timestamp(e)))
    )


# --- PaymentStatsCombineFn: unitario, sin pipeline -------------------------------


def test_merge_accumulators_accepts_single_pass_iterable():
    combine_fn = PaymentStatsCombineFn()
    accumulators = (
        (count, total)
        for count, total in [
            (2, 30_000),
            (3, 45_000),
            (1, 25_000),
        ]
    )

    assert combine_fn.merge_accumulators(accumulators) == (6, 100_000)


# --- agregación por clave y ventana ---------------------------------------------


def test_same_merchant_same_window_aggregates_count_and_total():
    events = [
        _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:00.000Z", amount=10_000),
        _confirmed_event(event_id="evt-2", event_time="2026-01-01T12:00:30.000Z", amount=25_000),
    ]
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=2,
            total_amount_pyg=35_000,
        )
    ]

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))


def test_different_merchants_same_window_produce_separate_aggregates():
    events = [
        _confirmed_event(
            event_id="evt-1",
            merchant_id="merchant-001",
            event_time="2026-01-01T12:00:00.000Z",
            amount=10_000,
        ),
        _confirmed_event(
            event_id="evt-2",
            merchant_id="merchant-002",
            event_time="2026-01-01T12:00:10.000Z",
            amount=20_000,
        ),
    ]
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=1,
            total_amount_pyg=10_000,
        ),
        _aggregate(
            "merchant-002",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=1,
            total_amount_pyg=20_000,
        ),
    ]

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))


def test_same_merchant_different_windows_produce_two_aggregates():
    events = [
        _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:00.000Z", amount=10_000),
        _confirmed_event(event_id="evt-2", event_time="2026-01-01T12:01:00.000Z", amount=20_000),
    ]
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=1,
            total_amount_pyg=10_000,
        ),
        _aggregate(
            "merchant-001",
            "2026-01-01T12:01:00.000Z",
            "2026-01-01T12:02:00.000Z",
            transaction_count=1,
            total_amount_pyg=20_000,
        ),
    ]

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))


def test_event_exactly_on_window_boundary_belongs_to_next_window():
    events = [
        _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:01:00.000Z", amount=10_000),
    ]
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:01:00.000Z",
            "2026-01-01T12:02:00.000Z",
            transaction_count=1,
            total_amount_pyg=10_000,
        )
    ]

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))


# --- formato de window_start / window_end / key ---------------------------------


def test_window_start_and_window_end_are_correct_iso8601():
    events = [
        _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:45.500Z", amount=10_000),
    ]

    def _check(actual):
        actual = list(actual)
        assert len(actual) == 1
        aggregate = actual[0]
        assert aggregate["window_start"] == "2026-01-01T12:00:00.000Z"
        assert aggregate["window_end"] == "2026-01-01T12:01:00.000Z"

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, _check)


def test_aggregate_key_is_merchant_id_pipe_window_start():
    events = [
        _confirmed_event(
            event_id="evt-1",
            merchant_id="merchant-042",
            event_time="2026-01-01T12:00:00.000Z",
            amount=10_000,
        ),
    ]

    def _check(actual):
        actual = list(actual)
        assert len(actual) == 1
        assert actual[0]["key"] == "merchant-042|2026-01-01T12:00:00.000Z"

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, _check)


def test_total_amount_pyg_sums_exact_integers():
    events = [
        _confirmed_event(
            event_id="evt-1", event_time="2026-01-01T12:00:00.000Z", amount=999_999_999
        ),
        _confirmed_event(event_id="evt-2", event_time="2026-01-01T12:00:01.000Z", amount=1),
    ]
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=2,
            total_amount_pyg=1_000_000_000,
        )
    ]

    with BeamTestPipeline() as p:
        confirmed = _confirmed_pcoll(p, events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))


# --- integración: bytes -> parse/validate -> filtro -> ventana -> agregado -----


def test_integration_raw_bytes_to_aggregate():
    event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:00.000Z", amount=15_000)
    raw = serialize_event(event)
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=1,
            total_amount_pyg=15_000,
        )
    ]

    with BeamTestPipeline() as p:
        raw_events = p | "Create" >> beam.Create([raw])
        confirmed, _invalid = apply_parse_validate_filter(raw_events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))


def test_declined_and_pending_do_not_contribute_to_aggregates():
    confirmed_event = _confirmed_event(
        event_id="evt-c", event_time="2026-01-01T12:00:00.000Z", amount=15_000
    )
    declined_event = _declined_event(event_id="evt-d", event_time="2026-01-01T12:00:00.000Z")
    pending_event = _pending_event(event_id="evt-p", event_time="2026-01-01T12:00:00.000Z")
    raws = [serialize_event(e) for e in (confirmed_event, declined_event, pending_event)]
    expected = [
        _aggregate(
            "merchant-001",
            "2026-01-01T12:00:00.000Z",
            "2026-01-01T12:01:00.000Z",
            transaction_count=1,
            total_amount_pyg=15_000,
        )
    ]

    with BeamTestPipeline() as p:
        raw_events = p | "Create" >> beam.Create(raws)
        confirmed, _invalid = apply_parse_validate_filter(raw_events)
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, equal_to(expected))
