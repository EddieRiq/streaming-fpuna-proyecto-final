"""Tests de allowed lateness, triggers y panes (Bloque 1 de 5).

Usan TestStream con streaming=True: es la única combinación verificada que en
Beam 2.75.0 respeta el disparo separado de panes on-time/late sobre PCollection
no acotadas. No prueban deduplicación ni orden de llegada -- eso está en
test_deduplication.py y test_out_of_order.py.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline as BeamTestPipeline
from apache_beam.testing.test_stream import TestStream as BeamTestStream
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import PaneInfoTiming

from streaming_payments.common.config import ALLOWED_LATENESS_SECONDS
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


def _ts(event_time: str) -> float:
    return event_time_to_beam_timestamp({"event_time": event_time})


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


class _CapturePaneTiming(beam.DoFn):
    """Solo para tests: expone PaneInfoTiming sin tocar el AggregateContract."""

    def process(self, element, pane=beam.DoFn.PaneInfoParam):
        yield (element, pane.timing)


WINDOW_START = "2026-01-01T12:00:00.000Z"
WINDOW_END = "2026-01-01T12:01:00.000Z"


def test_on_time_pane_is_emitted_when_watermark_passes_window_end():
    event = _confirmed_event(event_id="evt-1", event_time="2026-01-01T12:00:10.000Z", amount=10_000)
    ts = event_time_to_beam_timestamp(event)
    window_end_ts = _ts(WINDOW_END)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts)
        .add_elements([TimestampedValue(event, ts)])
        .advance_watermark_to(window_end_ts + 1)
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=_streaming_options()) as p:
        confirmed = p | test_stream
        aggregates = apply_windowed_aggregation(confirmed)
        tagged = aggregates | "CapturePaneTiming" >> beam.ParDo(_CapturePaneTiming())
        assert_that(
            tagged,
            equal_to(
                [
                    (
                        _aggregate("merchant-001", WINDOW_START, WINDOW_END, 1, 10_000),
                        PaneInfoTiming.ON_TIME,
                    )
                ]
            ),
        )


def test_late_event_within_allowed_lateness_updates_aggregate():
    on_time_event = _confirmed_event(
        event_id="evt-1", event_time="2026-01-01T12:00:10.000Z", amount=10_000
    )
    late_event = _confirmed_event(
        event_id="evt-2", event_time="2026-01-01T12:00:40.000Z", amount=25_000
    )
    ts_on_time = event_time_to_beam_timestamp(on_time_event)
    ts_late = event_time_to_beam_timestamp(late_event)
    window_end_ts = _ts(WINDOW_END)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts_on_time)
        .add_elements([TimestampedValue(on_time_event, ts_on_time)])
        .advance_watermark_to(window_end_ts + 1)
        .add_elements([TimestampedValue(late_event, ts_late)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=_streaming_options()) as p:
        confirmed = p | test_stream
        aggregates = apply_windowed_aggregation(confirmed)
        tagged = aggregates | "CapturePaneTiming" >> beam.ParDo(_CapturePaneTiming())
        assert_that(
            tagged,
            equal_to(
                [
                    (
                        _aggregate("merchant-001", WINDOW_START, WINDOW_END, 1, 10_000),
                        PaneInfoTiming.ON_TIME,
                    ),
                    (
                        _aggregate("merchant-001", WINDOW_START, WINDOW_END, 2, 35_000),
                        PaneInfoTiming.LATE,
                    ),
                ]
            ),
        )


def test_late_pane_uses_same_stable_key():
    on_time_event = _confirmed_event(
        event_id="evt-1", event_time="2026-01-01T12:00:10.000Z", amount=10_000
    )
    late_event = _confirmed_event(
        event_id="evt-2", event_time="2026-01-01T12:00:40.000Z", amount=25_000
    )
    ts_on_time = event_time_to_beam_timestamp(on_time_event)
    ts_late = event_time_to_beam_timestamp(late_event)
    window_end_ts = _ts(WINDOW_END)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts_on_time)
        .add_elements([TimestampedValue(on_time_event, ts_on_time)])
        .advance_watermark_to(window_end_ts + 1)
        .add_elements([TimestampedValue(late_event, ts_late)])
        .advance_watermark_to_infinity()
    )

    def _check(actual):
        actual = list(actual)
        assert len(actual) == 2
        keys = {aggregate["key"] for aggregate in actual}
        assert keys == {f"merchant-001|{WINDOW_START}"}

    with BeamTestPipeline(options=_streaming_options()) as p:
        confirmed = p | test_stream
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(aggregates, _check)


def test_event_beyond_allowed_lateness_does_not_update_aggregate():
    on_time_event = _confirmed_event(
        event_id="evt-1", event_time="2026-01-01T12:00:10.000Z", amount=10_000
    )
    too_late_event = _confirmed_event(
        event_id="evt-2", event_time="2026-01-01T12:00:05.000Z", amount=99_999
    )
    ts_on_time = event_time_to_beam_timestamp(on_time_event)
    ts_too_late = event_time_to_beam_timestamp(too_late_event)
    window_end_ts = _ts(WINDOW_END)

    test_stream = (
        BeamTestStream()
        .advance_watermark_to(ts_on_time)
        .add_elements([TimestampedValue(on_time_event, ts_on_time)])
        .advance_watermark_to(window_end_ts + ALLOWED_LATENESS_SECONDS + 1)
        .add_elements([TimestampedValue(too_late_event, ts_too_late)])
        .advance_watermark_to_infinity()
    )

    with BeamTestPipeline(options=_streaming_options()) as p:
        confirmed = p | test_stream
        aggregates = apply_windowed_aggregation(confirmed)
        assert_that(
            aggregates,
            equal_to([_aggregate("merchant-001", WINDOW_START, WINDOW_END, 1, 10_000)]),
        )
