"""Tests de eventos fuera de orden por event_time (Bloque 1 de 5).

Usan TestStream con streaming=True por la misma razón que test_late_events.py.
"""

from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue

from streaming_payments.pipeline.pipeline import (
    apply_windowed_aggregation,
    event_time_to_beam_timestamp,
)


def _confirmed_event(
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


def _streaming_options() -> PipelineOptions:
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True
    return options


def test_out_of_order_events_before_watermark_are_aggregated_together():
    event_a = _confirmed_event(
        event_id="evt-a", event_time="2026-01-01T12:00:40.000Z", amount=25_000
    )
    event_b = _confirmed_event(
        event_id="evt-b", event_time="2026-01-01T12:00:10.000Z", amount=10_000
    )
    ts_a = event_time_to_beam_timestamp(event_a)
    ts_b = event_time_to_beam_timestamp(event_b)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts_b)
        .add_elements([TimestampedValue(event_a, ts_a)])  # llega primero, event_time mayor
        .add_elements([TimestampedValue(event_b, ts_b)])  # llega segundo, event_time menor
        .advance_watermark_to(ts_a + 20)
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=_streaming_options()) as p:
        confirmed = p | test_stream
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(
            aggregates,
            equal_to(
                [
                    _aggregate(
                        "merchant-001",
                        "2026-01-01T12:00:00.000Z",
                        "2026-01-01T12:01:00.000Z",
                        transaction_count=2,
                        total_amount_pyg=35_000,
                    )
                ]
            ),
        )


def test_out_of_order_events_use_event_time_window_not_arrival_order():
    event_a = _confirmed_event(
        event_id="evt-a", event_time="2026-01-01T12:01:10.000Z", amount=25_000
    )
    event_b = _confirmed_event(
        event_id="evt-b", event_time="2026-01-01T12:00:10.000Z", amount=10_000
    )
    ts_a = event_time_to_beam_timestamp(event_a)
    ts_b = event_time_to_beam_timestamp(event_b)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts_b)
        .add_elements([TimestampedValue(event_a, ts_a)])  # llega primero, ventana posterior
        .add_elements([TimestampedValue(event_b, ts_b)])  # llega segundo, ventana anterior
        .advance_watermark_to(ts_a + 20)
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=_streaming_options()) as p:
        confirmed = p | test_stream
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(
            aggregates,
            equal_to(
                [
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
                        total_amount_pyg=25_000,
                    ),
                ]
            ),
        )
