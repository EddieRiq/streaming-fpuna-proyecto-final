"""
Pruebas de eventos tardios (late data).

Escenario a cubrir cuando se implemente streaming_payments.pipeline:
- Eventos que llegan dentro de ALLOWED_LATENESS_SECONDS despues del cierre de su
  ventana deben poder actualizar el agregado correspondiente.
- Eventos que llegan despues de ALLOWED_LATENESS_SECONDS deben descartarse o
  enviarse a payments.events.dlq.v1 -- comportamiento exacto a definir en la
  implementacion.

TODO: implementar fixtures y funciones test_* una vez que exista
src/streaming_payments/pipeline/pipeline.py.
"""
