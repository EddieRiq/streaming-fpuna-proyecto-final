"""
Pruebas de eventos fuera de orden (por event_time).

Escenario a cubrir cuando se implemente streaming_payments.pipeline:
- Eventos que llegan a Kafka en un orden distinto al de su event_time deben
  agruparse igual en la ventana fija de WINDOW_SIZE_SECONDS correspondiente
  a su event_time.

TODO: implementar fixtures y funciones test_* una vez que exista
src/streaming_payments/pipeline/pipeline.py.
"""
