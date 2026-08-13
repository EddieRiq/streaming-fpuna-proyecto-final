"""Configuración y constantes compartidas del pipeline de monitoreo de pagos."""

# Ventaneo por tiempo de evento
WINDOW_SIZE_SECONDS = 60
ALLOWED_LATENESS_SECONDS = 120

# Claves de deduplicación y agregación
DEDUP_KEY = "event_id"
GROUP_BY_KEY = "merchant_id"

# Tópicos Kafka
TOPIC_EVENTS_INPUT = "payments.events.v1"
TOPIC_AGGREGATES_OUTPUT = "payments.aggregates.v1"
TOPIC_EVENTS_DLQ = "payments.events.dlq.v1"

# Conexión Kafka (valores por defecto para entorno local via docker-compose)
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
