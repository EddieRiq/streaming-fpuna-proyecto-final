"""
Pruebas de validacion del contrato de evento (EventContract) y manejo de
eventos invalidos.

Escenario a cubrir cuando se implemente la validacion:
- Eventos que no cumplen con EventContract (campos faltantes, tipos
  incorrectos, schema_version no soportado) deben enviarse a
  payments.events.dlq.v1 en lugar de procesarse en el pipeline principal.

TODO: implementar fixtures y funciones test_* una vez que exista la logica de
validacion en src/streaming_payments/common/contracts.py y el pipeline en
src/streaming_payments/pipeline/pipeline.py.
"""
