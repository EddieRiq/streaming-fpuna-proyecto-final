# Arquitectura

TODO: completar con el detalle de la arquitectura del pipeline de monitoreo de
pagos en tiempo real.

## Componentes

- Productor sintético (`src/streaming_payments/producer`)
- Kafka (log de entrada / salida)
- Apache Beam (`src/streaming_payments/pipeline`)
- Materializador idempotente (`src/streaming_payments/materializer`)

## Ver también

- Diagrama: [architecture.mmd](./architecture.mmd)
