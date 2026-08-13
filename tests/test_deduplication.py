"""
Pruebas de deduplicacion por event_id.

Escenario a cubrir cuando se implemente streaming_payments.pipeline:
- Reenvio del mismo event_id dentro de la misma ventana no debe duplicar el agregado.
- Reenvio del mismo event_id en ventanas distintas, dentro de ALLOWED_LATENESS_SECONDS,
  no debe generar un agregado duplicado.

TODO: implementar fixtures y funciones test_* una vez que exista
src/streaming_payments/pipeline/pipeline.py.
"""
