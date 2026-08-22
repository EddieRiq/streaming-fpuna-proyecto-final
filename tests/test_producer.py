"""Tests unitarios del productor sintético de pagos, sin broker Kafka."""

import json
from datetime import UTC, datetime, timedelta

from streaming_payments.common.config import ALLOWED_LATENESS_SECONDS, WINDOW_SIZE_SECONDS
from streaming_payments.producer import producer as producer_module
from streaming_payments.producer.producer import (
    DEFAULT_BASE_TIME,
    KafkaEventWriter,
    build_demo_scenario,
    generate_normal_stream,
    serialize_event,
    serialize_key,
)

CONTRACT_TOP_LEVEL_KEYS = {"schema_version", "event_id", "key", "event_time", "payload"}
CONTRACT_PAYLOAD_KEYS = {
    "transaction_id",
    "merchant_id",
    "account_id",
    "amount",
    "currency",
    "status",
}


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.flush_calls = 0
        self.close_calls = 0

    def send(self, topic: str, key: bytes, value: bytes) -> None:
        self.sent.append((topic, key, value))

    def flush(self) -> None:
        self.flush_calls += 1

    def close(self) -> None:
        self.close_calls += 1


# --- Generación pura -------------------------------------------------------


def test_generate_normal_stream_reproducible():
    first = generate_normal_stream(seed=42, count=10)
    second = generate_normal_stream(seed=42, count=10)
    assert first == second


def test_generate_normal_stream_varies_with_seed():
    a = generate_normal_stream(seed=1, count=10)
    b = generate_normal_stream(seed=2, count=10)
    assert [e["event_id"] for e in a] != [e["event_id"] for e in b]


def test_normal_event_matches_contract_shape():
    events = generate_normal_stream(seed=7, count=5)
    for event in events:
        assert set(event.keys()) == CONTRACT_TOP_LEVEL_KEYS
        assert set(event["payload"].keys()) == CONTRACT_PAYLOAD_KEYS
        assert event["schema_version"] == 1
        assert event["payload"]["currency"] == "PYG"
        assert event["payload"]["status"] in {"CONFIRMED", "DECLINED", "PENDING"}


def test_amount_is_int_not_float_or_bool():
    events = generate_normal_stream(seed=7, count=20)
    for event in events:
        amount = event["payload"]["amount"]
        assert isinstance(amount, int)
        assert not isinstance(amount, bool)


def test_event_time_is_iso8601_utc_and_increasing():
    events = generate_normal_stream(seed=3, count=5)
    parsed = [datetime.fromisoformat(e["event_time"].replace("Z", "+00:00")) for e in events]
    for dt in parsed:
        assert dt.tzinfo == UTC
    assert parsed == sorted(parsed)
    assert len(set(parsed)) == len(parsed)


def test_kafka_key_equals_merchant_id():
    events = generate_normal_stream(seed=9, count=5)
    for event in events:
        assert event["key"] == event["payload"]["merchant_id"]


def test_generate_normal_stream_uses_default_base_time_when_start_time_none():
    events = generate_normal_stream(seed=11, count=1, start_time=None)
    assert events[0]["event_time"] == DEFAULT_BASE_TIME.strftime("%Y-%m-%dT%H:%M:%S.%f")[
        :-3
    ] + "Z"


# --- Escenario demo ---------------------------------------------------------


def test_demo_scenario_is_deterministic():
    assert build_demo_scenario() == build_demo_scenario()


def test_demo_scenario_e3_and_duplicate_are_confirmed():
    planned = build_demo_scenario()
    e3 = planned[2].event
    duplicate = next(p for p in planned if p.scenario_tag == "duplicate")
    assert e3["payload"]["status"] == "CONFIRMED"
    assert duplicate.event["payload"]["status"] == "CONFIRMED"


def test_demo_scenario_duplicate_is_exact_copy_of_e3():
    planned = build_demo_scenario()
    e3 = planned[2].event
    duplicate = next(p for p in planned if p.scenario_tag == "duplicate")
    assert duplicate.event == e3
    assert duplicate.event["event_id"] == e3["event_id"]


def test_demo_scenario_out_of_order_strictly_before_previous_sent():
    planned = build_demo_scenario()
    index = next(i for i, p in enumerate(planned) if p.scenario_tag == "out_of_order")
    previous_event_time = planned[index - 1].event["event_time"]
    out_of_order_time = planned[index].event["event_time"]
    assert out_of_order_time < previous_event_time


def test_demo_scenario_late_event_far_before_window():
    planned = build_demo_scenario()
    late = next(p for p in planned if p.scenario_tag == "late")
    late_time = datetime.fromisoformat(late.event["event_time"].replace("Z", "+00:00"))
    threshold = DEFAULT_BASE_TIME - timedelta(
        seconds=WINDOW_SIZE_SECONDS + ALLOWED_LATENESS_SECONDS
    )
    assert late_time <= threshold


def test_demo_scenario_invalid_event_missing_event_id():
    planned = build_demo_scenario()
    invalid = [p for p in planned if p.scenario_tag == "invalid"]
    assert len(invalid) == 1
    event = invalid[0].event
    assert "event_id" not in event
    assert set(event.keys()) == CONTRACT_TOP_LEVEL_KEYS - {"event_id"}
    json.dumps(event)  # debe seguir siendo serializable


def test_demo_scenario_respects_custom_base_time():
    custom_base = datetime(2030, 5, 1, tzinfo=UTC)
    planned_a = build_demo_scenario(base_time=custom_base)
    planned_b = build_demo_scenario(base_time=custom_base)
    assert planned_a == planned_b
    assert planned_a[0].event["event_time"] == "2030-05-01T00:00:00.000Z"


def test_planned_event_never_leaks_control_metadata():
    planned = build_demo_scenario()
    for p in planned:
        assert "scenario_tag" not in p.event
        assert "note" not in p.event
        if p.scenario_tag != "invalid":
            assert set(p.event.keys()) == CONTRACT_TOP_LEVEL_KEYS


# --- Serialización -----------------------------------------------------------


def test_serialize_event_is_json_utf8_bytes():
    events = generate_normal_stream(seed=5, count=1)
    event = events[0]
    raw = serialize_event(event)
    assert isinstance(raw, bytes)
    assert json.loads(raw.decode("utf-8")) == event


def test_serialize_key_is_utf8_bytes_of_merchant_id():
    events = generate_normal_stream(seed=5, count=1)
    event = events[0]
    assert serialize_key(event) == event["key"].encode("utf-8")


def test_serialize_key_rejects_non_str_key():
    event = {"key": 12345, "payload": {}}
    try:
        serialize_key(event)
    except TypeError as exc:
        assert "str" in str(exc)
    else:
        raise AssertionError("serialize_key debería fallar si key no es str")


# --- KafkaEventWriter (fake producer, sin red) -------------------------------


def test_kafka_event_writer_sends_bytes_not_objects():
    planned = build_demo_scenario()
    fake = _FakeProducer()
    writer = KafkaEventWriter(fake, "payments.events.v1")

    writer.publish(planned)

    assert len(fake.sent) == len(planned)
    for (topic, key, value), p in zip(fake.sent, planned, strict=True):
        assert topic == "payments.events.v1"
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)
        assert key == serialize_key(p.event)
        assert value == serialize_event(p.event)


def test_kafka_event_writer_flushes_once():
    planned = build_demo_scenario()
    fake = _FakeProducer()
    writer = KafkaEventWriter(fake, "payments.events.v1")

    writer.publish(planned)

    assert fake.flush_calls == 1


def test_kafka_event_writer_does_not_close_producer():
    planned = build_demo_scenario()
    fake = _FakeProducer()
    writer = KafkaEventWriter(fake, "payments.events.v1")

    writer.publish(planned)

    assert fake.close_calls == 0


# --- CLI: main() y ciclo de vida del producer --------------------------------


def test_main_closes_kafka_producer_exactly_once(monkeypatch):
    fake = _FakeProducer()
    monkeypatch.setattr(producer_module, "build_kafka_producer", lambda bootstrap_servers: fake)

    exit_code = producer_module.main(["--scenario", "demo", "--seed", "1"])

    assert exit_code == 0
    assert fake.flush_calls == 1
    assert fake.close_calls == 1
    assert len(fake.sent) == len(build_demo_scenario(seed=1))


def test_main_dry_run_does_not_touch_kafka_producer(monkeypatch, capsys):
    def _fail_if_called(bootstrap_servers: str):
        raise AssertionError("build_kafka_producer no debería llamarse en --dry-run")

    monkeypatch.setattr(producer_module, "build_kafka_producer", _fail_if_called)

    exit_code = producer_module.main(
        ["--scenario", "normal", "--count", "3", "--seed", "1", "--dry-run"]
    )

    assert exit_code == 0
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 3
    for line in out:
        json.loads(line)


def test_main_start_now_uses_explicit_start_time_not_default(monkeypatch):
    captured: dict[str, object] = {}
    original = producer_module.generate_normal_stream

    def _spy(seed, count, start_time=None):
        captured["start_time"] = start_time
        return original(seed, count, start_time=start_time)

    monkeypatch.setattr(producer_module, "generate_normal_stream", _spy)

    exit_code = producer_module.main(
        ["--scenario", "normal", "--count", "1", "--seed", "1", "--start-now", "--dry-run"]
    )

    assert exit_code == 0
    assert captured["start_time"] is not None
    assert captured["start_time"] != DEFAULT_BASE_TIME
    assert captured["start_time"].tzinfo == UTC
