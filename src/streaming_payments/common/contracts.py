"""Contratos de datos (schemas) para eventos de entrada y agregados de salida.

Sin lógica de validación ni parsing todavía -- solo la forma de los datos.

La key del evento es merchant_id.
La key del agregado es estable por merchant_id + window_start.
"""

from typing import TypedDict


class PaymentPayload(TypedDict):
    transaction_id: str
    merchant_id: str
    account_id: str
    amount: int
    currency: str
    status: str


class EventContract(TypedDict):
    """Forma esperada de un evento de pago en payments.events.v1."""

    schema_version: int
    event_id: str
    key: str
    event_time: str
    payload: PaymentPayload


class AggregatePayload(TypedDict):
    merchant_id: str
    transaction_count: int
    total_amount_pyg: int


class AggregateContract(TypedDict):
    """Forma esperada de un agregado en payments.aggregates.v1."""

    schema_version: int
    key: str
    window_start: str
    window_end: str
    payload: AggregatePayload
