"""Tests de la lógica pura de parseo y validación (sin Beam, sin broker)."""

import json
from datetime import UTC, datetime

import pytest

from streaming_payments.pipeline.pipeline import (
    event_time_to_beam_timestamp,
    parse_event,
    validate_event,
)


def _valid_payload(**overrides: object) -> dict[str, object]:
    payload = {
        "transaction_id": "txn-1",
        "merchant_id": "merchant-001",
        "account_id": "account-001",
        "amount": 10_000,
        "currency": "PYG",
        "status": "CONFIRMED",
    }
    payload.update(overrides)
    return payload


def _valid_event(
    payload: dict[str, object] | None = None, **overrides: object
) -> dict[str, object]:
    event = {
        "schema_version": 1,
        "event_id": "evt-1",
        "key": "merchant-001",
        "event_time": "2026-01-01T12:00:00.000Z",
        "payload": payload if payload is not None else _valid_payload(),
    }
    event.update(overrides)
    return event


# --- parse_event -------------------------------------------------------------


def test_parse_event_accepts_valid_json_bytes():
    event = _valid_event()
    raw = json.dumps(event).encode("utf-8")
    assert parse_event(raw) == event


def test_parse_event_raises_on_invalid_utf8():
    with pytest.raises(UnicodeDecodeError):
        parse_event(b"\xff\xfe\xfd")


def test_parse_event_raises_on_invalid_json():
    with pytest.raises(json.JSONDecodeError):
        parse_event(b"{not valid json")


def test_parse_event_raises_on_non_object_json():
    with pytest.raises(ValueError):
        parse_event(b"[1, 2, 3]")


# --- validate_event: caso válido ---------------------------------------------


def test_validate_event_accepts_valid_event():
    event = _valid_event()
    result = validate_event(event)
    assert result.valid is True
    assert result.event == event
    assert result.errors == ()


# --- validate_event: top level ------------------------------------------------


def test_validate_event_rejects_missing_event_id():
    event = _valid_event()
    del event["event_id"]
    result = validate_event(event)
    assert result.valid is False
    assert result.event is None
    assert any("event_id" in e for e in result.errors)


def test_validate_event_rejects_schema_version_not_1():
    event = _valid_event(schema_version=2)
    result = validate_event(event)
    assert result.valid is False
    assert any("schema_version" in e for e in result.errors)


# --- validate_event: payload ---------------------------------------------------


def test_validate_event_rejects_amount_float():
    event = _valid_event(payload=_valid_payload(amount=1000.5))
    result = validate_event(event)
    assert result.valid is False
    assert any("amount" in e for e in result.errors)


def test_validate_event_rejects_amount_bool():
    event = _valid_event(payload=_valid_payload(amount=True))
    result = validate_event(event)
    assert result.valid is False
    assert any("amount" in e for e in result.errors)


def test_validate_event_rejects_amount_not_positive():
    event = _valid_event(payload=_valid_payload(amount=0))
    result = validate_event(event)
    assert result.valid is False
    assert any("amount" in e for e in result.errors)


def test_validate_event_rejects_currency_not_pyg():
    event = _valid_event(payload=_valid_payload(currency="USD"))
    result = validate_event(event)
    assert result.valid is False
    assert any("currency" in e for e in result.errors)


def test_validate_event_rejects_invalid_status():
    event = _valid_event(payload=_valid_payload(status="REFUNDED"))
    result = validate_event(event)
    assert result.valid is False
    assert any("status" in e for e in result.errors)


# --- validate_event: invariantes -----------------------------------------------


def test_validate_event_rejects_key_mismatch_with_merchant_id():
    event = _valid_event(key="merchant-999")
    result = validate_event(event)
    assert result.valid is False
    assert any("merchant_id" in e for e in result.errors)


def test_validate_event_rejects_naive_event_time():
    event = _valid_event(event_time="2026-01-01T12:00:00")
    result = validate_event(event)
    assert result.valid is False
    assert any("timezone-aware" in e for e in result.errors)


def test_validate_event_rejects_unparseable_event_time():
    event = _valid_event(event_time="not-a-timestamp")
    result = validate_event(event)
    assert result.valid is False
    assert any("event_time" in e for e in result.errors)


def test_validate_event_accepts_timezone_aware_non_utc_offset():
    event = _valid_event(event_time="2026-08-21T19:00:00-03:00")
    result = validate_event(event)
    assert result.valid is True
    assert result.errors == ()


def test_validate_event_accumulates_multiple_errors():
    event = _valid_event(payload=_valid_payload(currency="USD", status="REFUNDED"))
    result = validate_event(event)
    assert result.valid is False
    assert any("currency" in e for e in result.errors)
    assert any("status" in e for e in result.errors)
    assert len(result.errors) >= 2


# --- event_time_to_beam_timestamp ----------------------------------------------


def test_event_time_to_beam_timestamp_matches_epoch_seconds():
    event = _valid_event(event_time="2026-01-01T00:00:00Z")
    expected = datetime(2026, 1, 1, tzinfo=UTC).timestamp()
    assert event_time_to_beam_timestamp(event) == expected


def test_event_time_offsets_map_to_same_beam_timestamp():
    event_utc = _valid_event(event_time="2026-08-21T22:00:00Z")
    event_offset = _valid_event(event_time="2026-08-21T19:00:00-03:00")
    assert event_time_to_beam_timestamp(event_utc) == event_time_to_beam_timestamp(event_offset)
