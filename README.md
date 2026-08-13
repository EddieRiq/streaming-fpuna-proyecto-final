# Streaming FPUNA - Proyecto Integrador

Monitoreo de pagos en tiempo real con Apache Kafka y Apache Beam.

## Caso de uso

Pipeline de streaming que ingiere eventos sintéticos de pago, los procesa con Apache Beam
usando tiempo de evento, y produce agregados por comercio (`merchant_id`) en ventanas fijas,
con deduplicación, tolerancia a eventos tardíos y salida idempotente.

## Características objetivo

- Productor sintético reproducible
- Kafka como log de entrada (`payments.events.v1`)
- Apache Beam leyendo desde Kafka, con tiempo de evento
- Ventanas fijas de 60 segundos (`WINDOW_SIZE_SECONDS`)
- Deduplicación por `event_id`
- Allowed lateness de 120 segundos (`ALLOWED_LATENESS_SECONDS`)
- Agregación por `merchant_id`
- Publicación de agregados en payments.aggregates.v1
- Materialización idempotente mediante una clave estable por comercio y ventana
- Pruebas con duplicados, eventos fuera de orden y eventos tardíos

## Estructura del proyecto

```
src/
└── streaming_payments/
    ├── common/        # config.py (constantes) y contracts.py (schemas de evento/agregado)
    ├── producer/       # productor sintético (pendiente)
    ├── pipeline/        # pipeline Apache Beam (pendiente)
    └── materializer/     # consumidor/materializador idempotente (pendiente)

tests/          # pruebas de deduplicación, fuera de orden, tardíos y validación
data/           # escenarios de datos reproducibles
docs/           # arquitectura (md + diagrama Mermaid) e informe técnico (tex)
evidence/       # capturas, logs y salidas de demostración
```

## Estado actual

Este repositorio contiene únicamente la estructura base del proyecto. Aún no están
implementados producer.py, pipeline.py ni materializer.py.

## Uso (cuando esté implementado)

```
make up       # levanta Kafka y crea los tópicos
make test     # corre la suite de pruebas
make down     # detiene la infraestructura
```

## Tópicos Kafka

| Tópico | Propósito |
|---|---|
| `payments.events.v1` | Entrada: eventos de pago sintéticos |
| `payments.aggregates.v1` | Salida: agregados por merchant_id y ventana |
| `payments.events.dlq.v1` | Dead-letter queue para eventos inválidos |
