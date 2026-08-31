# Documentación técnica

## 1. Problema y métrica

El sistema responde una pregunta: por cada comercio (`merchant_id`) y por cada
minuto, cuántas transacciones `CONFIRMED` hubo y cuál fue el monto total en
guaraníes. La entrada es un flujo de eventos de pago que pueden llegar fuera de
orden, repetidos o atrasados. La métrica se calcula sobre tiempo de evento, no
sobre el momento en que el evento llega al pipeline.

## 2. Arquitectura y flujo

```text
producer  ->  payments.events.v1  ->  Beam (KafkaIO)  ->  payments.aggregates.v1  ->  materializer  ->  SQLite
                                          |
                                          +-- inválidos --> payments.events.dlq.v1
```

Etapas dentro de Beam, en orden:

1. `ReadFromKafka` entrega pares `(key, value)` en bytes.
2. `extract_kafka_value` toma el value. Un value nulo se normaliza a bytes
   vacíos para que la validación lo trate como inválido en lugar de romper el
   pipeline.
3. `ParseAndValidateFn` decodifica, parsea JSON y valida el contrato. Produce
   dos salidas: los eventos válidos con su timestamp de evento, y los inválidos
   con la lista de errores (salida tageada `invalid`).
4. `Filter(_is_confirmed)` deja pasar solo `status == "CONFIRMED"`.
5. `DeduplicateByEventIdFn` descarta `event_id` repetidos.
6. `WindowInto` asigna cada evento a una ventana fija de 60 segundos y fija el
   trigger y el modo de acumulación.
7. `CombinePerKey(PaymentStatsCombineFn)` calcula `transaction_count` y
   `total_amount_pyg` por `merchant_id` y ventana.
8. `_ToAggregateContractFn` arma el `AggregateContract` con la key estable.
9. `WriteToKafka` publica el agregado. Una rama paralela publica los inválidos
   en el DLQ.

El pipeline se construye y ejecuta con `with beam.Pipeline(options=options)`.
En este entorno Windows con DirectRunner y KafkaIO cross-language, ese patrón
permitió usar correctamente el loopback worker del expansion service. Es una
observación del entorno local y no un requisito general de Beam.

## 3. Contrato de entrada

`EventContract` (ver `src/streaming_payments/common/contracts.py`):

| Campo | Tipo | Regla |
|---|---|---|
| `schema_version` | int | igual a `1` |
| `event_id` | str | no vacío |
| `key` | str | no vacío, igual a `payload.merchant_id` |
| `event_time` | str | ISO 8601, parseable, con zona horaria |
| `payload.transaction_id` | str | no vacío |
| `payload.merchant_id` | str | no vacío |
| `payload.account_id` | str | no vacío |
| `payload.amount` | int | mayor que 0, no `bool` |
| `payload.currency` | str | igual a `"PYG"` |
| `payload.status` | str | `CONFIRMED`, `DECLINED` o `PENDING` |

`validate_event` no lanza excepciones y acumula todos los errores encontrados,
para que el motivo de rechazo en el DLQ sea completo. Acepta cualquier offset
horario válido en `event_time`; no exige que sea UTC, porque el instante
absoluto es el mismo.

## 4. Contrato de salida

`AggregateContract`:

```json
{
  "schema_version": 1,
  "key": "merchant-002|2026-08-26T23:43:00.000Z",
  "window_start": "2026-08-26T23:43:00.000Z",
  "window_end": "2026-08-26T23:44:00.000Z",
  "payload": {
    "merchant_id": "merchant-002",
    "transaction_count": 2,
    "total_amount_pyg": 484422
  }
}
```

`key` es `merchant_id|window_start`. El value se serializa con `sort_keys=True`
para que sea determinista. No lleva metadata de pane: on-time y tardío salen
con el mismo formato.

## 5. Kafka: topics, keys, particiones y orden

| Topic | Particiones | Key |
|---|---:|---|
| `payments.events.v1` | 3 | `merchant_id` |
| `payments.aggregates.v1` | 3 | `merchant_id\|window_start` |
| `payments.events.dlq.v1` | 1 | `event_id` o vacía |

Kafka mantiene el orden dentro de una partición. La key `merchant_id` mantiene
los eventos de un mismo comercio en la misma partición según el particionador.
No existe un orden global entre particiones.

La `aggregate_key` estable hace que las versiones de una misma ventana y key se
publiquen en la misma partición. `last(k)` es la última versión de esa key y se
determina por el mayor offset dentro de su partición.

El broker corre en modo KRaft, factor de replicación 1, apto para un entorno
local de un solo nodo.

## 6. Event time frente a processing time

El timestamp Beam de cada evento es el epoch de su `event_time`
(`event_time_to_beam_timestamp`). La ventana a la que pertenece un evento
depende solo de ese valor, no de cuándo llegó. Un evento con `event_time`
viejo cae en una ventana vieja aunque llegue ahora.

El producer separa la generación de eventos de la escritura en Kafka. La
generación es pura y determinista a partir de una semilla y un tiempo base, sin
usar `datetime.now()` dentro de la generación. Sin `--start-now`, el escenario
es reproducible a partir de la seed y un tiempo base fijo (`DEFAULT_BASE_TIME`).
Con `--start-now`, la estructura y los valores generados siguen determinados
por la seed, pero el tiempo base pasa a ser el momento UTC de ejecución
redondeado al segundo (`datetime.now(UTC).replace(microsecond=0)`).

## 7. Ventanas fijas de 60 segundos

`FixedWindows(WINDOW_SIZE_SECONDS)` con `WINDOW_SIZE_SECONDS = 60`. Los límites
son semiabiertos: un evento en el borde exacto pertenece a la ventana
siguiente. Esto está fijado en
`test_event_exactly_on_window_boundary_belongs_to_next_window`.

`window_start` y `window_end` se formatean como ISO 8601 UTC con milisegundos,
por ejemplo `2026-01-01T12:00:00.000Z`.

## 8. Watermark y allowed lateness

`allowed_lateness = ALLOWED_LATENESS_SECONDS = 120`. Este margen se evalúa
respecto del watermark, no del processing time. Mientras el watermark no haya
superado `window_end + 120 s`, un elemento tardío de esa ventana todavía se
procesa y dispara un pane nuevo. Cuando el watermark pasa ese límite, un
elemento para esa ventana se descarta y el agregado no cambia.

## 9. Trigger

`AfterWatermark(late=AfterCount(1))`. Dispara un pane cuando el watermark cruza
`window_end`, y un pane adicional por cada elemento tardío que entra dentro del
allowed lateness.

## 10. Panes ON_TIME y tardíos

El primer pane de una ventana es `ON_TIME`. Los siguientes, disparados por
elementos tardíos, son `LATE`. Los cuatro tests de `test_late_events.py` usan
`TestStream` con control explícito del watermark:

- `test_on_time_pane_is_emitted_when_watermark_passes_window_end`: llega un
  evento, el watermark cruza el fin de la ventana, sale un pane `ON_TIME` con
  `transaction_count = 1`.
- `test_late_event_within_allowed_lateness_updates_aggregate`: después del pane
  `ON_TIME` llega un evento tardío dentro de los 120 s; sale un segundo pane
  `LATE` con el acumulado (`transaction_count = 2`, `total_amount_pyg = 35000`).
- `test_late_pane_uses_same_stable_key`: los dos panes salen con la misma
  `aggregate_key`, `merchant-001|<window_start>`.
- `test_event_beyond_allowed_lateness_does_not_update_aggregate`: el watermark
  avanza más allá de `window_end + 120 s`, llega un evento tardío, el agregado
  queda con `transaction_count = 1`.

## 11. Modo ACCUMULATING

`accumulation_mode = AccumulationMode.ACCUMULATING`. Cada pane de una ventana
contiene el resultado acumulado de todos los elementos de esa ventana vistos
hasta ese momento, no solo el delta desde el pane anterior. Beam no reescribe
el pane ya emitido; publica una versión nueva del agregado con la misma
`aggregate_key`. Los mensajes ya publicados no se reescriben cuando aparece un
pane nuevo.

Aguas abajo, el estado final de una ventana es su último pane, no la suma de
los panes. El materializer procesa las versiones recibidas y el UPSERT
sobrescribe el estado materializado anterior de esa `aggregate_key`.

## 12. Deduplicación por event_id

Antes de la deduplicación, `_to_event_id_kv` re-keya cada evento a
`(event["event_id"], event)`. Esa key es interna a esta etapa; la key de
negocio sigue siendo `merchant_id`.

`DeduplicateByEventIdFn` es un `DoFn` con estado:

- `SEEN_STATE = ReadModifyWriteStateSpec("seen", BooleanCoder())`
- `EXPIRY_TIMER = TimerSpec("expiry", TimeDomain.WATERMARK)`

En `process`, si `seen.read()` es verdadero el evento ya se vio y no se emite
nada. Si no, escribe `seen.write(True)`, programa el timer en
`timestamp + DEDUP_HORIZON_SECONDS` y emite el evento. `timestamp` es el
timestamp Beam del primer evento aceptado para ese `event_id`. Un duplicado
entra por la rama que retorna antes de reprogramar el timer, así que no
reinicia la expiración. Esto es lo que verifica
`test_same_event_id_does_not_reset_expiry_timer`. Cuando el timer se dispara,
`expire` hace `seen.clear()`; después del horizonte el mismo `event_id` puede
volver a aceptarse (`test_event_is_accepted_again_after_dedup_horizon`).

`DEDUP_HORIZON_SECONDS = WINDOW_SIZE_SECONDS + ALLOWED_LATENESS_SECONDS = 180 s`.
Es un horizonte acotado y conservador, alineado con la ventana de 60 s y la
política de lateness de 120 s.

## 13. PaymentStatsCombineFn y CombinePerKey

Antes de agregar, `_to_merchant_amount_kv` reduce cada evento a
`(merchant_id, amount)`. El acumulador de `PaymentStatsCombineFn` es una tupla
`(transaction_count, total_amount_pyg)` que empieza en `(0, 0)`. `add_input`
suma 1 al conteo y `amount` al total. `merge_accumulators` recorre los
acumuladores parciales y suma componente a componente. `extract_output`
devuelve la tupla tal cual.

Se usa `CombinePerKey(PaymentStatsCombineFn())` y no un agrupamiento explícito
porque no hace falta retener la lista completa de eventos de una ventana, solo
esos dos enteros. La suma de `amount` se hace sobre enteros, sin punto
flotante.

## 14. DLQ

`ParseAndValidateFn` manda a la salida tageada `invalid` un registro
`{raw_data, errors}` cuando el parseo o la validación fallan. Si el JSON era
válido, `raw_data` es el dict parseado. Si los bytes no se pudieron decodificar
o el JSON era inválido, `raw_data` es el texto con reemplazo de caracteres.

`serialize_kafka_dlq` convierte `errors` a lista y serializa el registro a
JSON. La key del mensaje DLQ es el `event_id` cuando `raw_data` es un dict que
lo trae como str no vacío; en cualquier otro caso es bytes vacíos, porque
`WriteToKafka` no acepta `None` como key.

## 15. aggregate_key estable

`aggregate_key = f"{merchant_id}|{window_start}"`. Es la misma para el pane
on-time y para los panes tardíos de la misma ventana. Cumple dos funciones: es
la key Kafka del topic de salida, con lo cual mantiene las versiones de esa key
en la misma partición, y es la primary key en SQLite, con lo cual habilita el
UPSERT idempotente.

## 16. Materializer

Consumidor separado, sin Beam. Usa `kafka-python` y `sqlite3` de la librería
estándar.

- `KafkaConsumer(topic, bootstrap_servers=..., group_id="streaming-payments-materializer",
  enable_auto_commit=False, auto_offset_reset="earliest")`.
- Procesa un registro a la vez, en un bucle sobre el consumer.
- Para cada registro, `materialize_record` hace tres cosas en orden:
  `parse_aggregate_message` valida el contrato del agregado, `validate_kafka_key`
  verifica que la key Kafka del registro coincida con `aggregate["key"]`, y el
  UPSERT se ejecuta dentro de una transacción SQLite (`with connection:`).
- Solo después de que ese registro quedó en SQLite, el materializer commitea su
  offset:

  ```python
  consumer.commit(
      offsets={
          TopicPartition(record.topic, record.partition):
              OffsetAndMetadata(record.offset + 1, "")
      }
  )
  ```

  El commit es explícito, por la partición de ese registro y con
  `record.offset + 1`. No es un commit global de offsets prefetched.
- `--max-messages N` corta el bucle después de N registros.

`validate_kafka_key` rechaza una key Kafka `None` y una key que no decodifique
exactamente a `aggregate["key"]`. Ante contrato inválido, key Kafka
inconsistente o error de SQLite, el materializer no commitea el offset de ese
mensaje y termina con código distinto de cero (fail-fast; el detalle está en la
sección 19).

## 17. Orden SQLite y commit de Kafka

El orden es SQLite primero, commit de Kafka después. Si el proceso muere entre
la escritura en SQLite y el commit, al reiniciar Kafka reentrega ese registro,
el materializer vuelve a hacer el UPSERT con el mismo contenido y el resultado
en SQLite no cambia. El orden inverso (commit primero) podría avanzar el offset
sobre un registro que todavía no se persistió.

## 18. UPSERT

SQL tal como está en `materializer.py`:

```sql
INSERT INTO merchant_window_aggregates (
    aggregate_key, merchant_id, window_start, window_end,
    transaction_count, total_amount_pyg
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(aggregate_key) DO UPDATE SET
    merchant_id = excluded.merchant_id,
    window_start = excluded.window_start,
    window_end = excluded.window_end,
    transaction_count = excluded.transaction_count,
    total_amount_pyg = excluded.total_amount_pyg
```

`aggregate_key` es `PRIMARY KEY`. `transaction_count` y `total_amount_pyg` son
`INTEGER`. Una segunda versión de la misma ventana reemplaza la fila anterior.

## 19. Fail-fast

Ante UTF-8 o JSON inválido, un `AggregateContract` inválido, una key Kafka
`None` o inconsistente, o un error de SQLite, el materializer no commitea el
offset de ese mensaje y termina con código distinto de cero. No hay DLQ de
agregados en esta etapa: un mensaje envenenado requiere intervención manual. La
decisión es que un commit posterior podría confirmar de forma indirecta un
mensaje que nunca se procesó bien.

## 20. Semántica de entrega

La semántica declarada es at-least-once con sink idempotente. Un registro puede
reprocesarse si ocurre un fallo antes del commit de su offset. El materializer
hace UPSERT sobre una `aggregate_key` estable, por lo que repetir la misma
versión deja el mismo estado en SQLite.

## 21. Por qué no exactly-once

No existe una transacción única que abarque la escritura en SQLite y el commit
del offset de Kafka. Tampoco se usan transacciones de productor de Kafka para
la salida de agregados. Por eso el proyecto no declara exactly-once.

La corrección práctica del resultado depende de la `aggregate_key` estable y
del UPSERT idempotente. Reprocesar la misma versión de una ventana deja el
mismo estado en SQLite: aplicarla dos veces da el mismo resultado que
aplicarla una vez.

## 22. KafkaIO cross-language y Java

`ReadFromKafka` y `WriteToKafka` son transforms Java. Beam los ejecuta a través
de un expansion service, que necesita Java 11 o superior. El proyecto se
verificó con OpenJDK 17.

La ruta al JDK no está hardcodeada en el código: se toma de `JAVA_HOME`. En
Windows, si `JAVA_HOME` persistente apunta a otra versión, se puede definir
`JAVA_HOME` solo para la sesión de PowerShell donde se corre Beam, sin
modificar la configuración persistente.

## 23. Limitación del E2E bounded local

El pipeline es unbounded por defecto (`max_num_records = None`). Para el smoke y
el E2E local se usó `--max-num-records 10`.

En este entorno con DirectRunner y KafkaIO cross-language, `WriteToKafka` no
materializaba las escrituras mientras la fuente seguía unbounded. Al acotar la
lectura, la corrida local pudo finalizar y materializar las escrituras. Es una
limitación observada de esta combinación local. No es una regla general de
Apache Beam ni de otros runners.

## 24. Diferencia entre evidencia TestStream y evidencia E2E

`tests/test_late_events.py` y `tests/test_out_of_order.py` usan `TestStream`
con el watermark controlado de forma determinista. Son la evidencia para el
pane `ON_TIME`, los panes `LATE`, el allowed lateness, el descarte después del
horizonte y los eventos fuera de orden.

El E2E del Bloque 4 (`evidence/block-04-e2e/`) corre el producer real, publica
en Kafka real, corre el pipeline con `--max-num-records 10`, corre el
materializer y reconcilia SQLite contra el último agregado por clave. Prueba
que los componentes se integran contra Kafka real.

La corrida bounded del DirectRunner no se usa para inferir la semántica exacta
de lateness. Esa semántica se verifica con TestStream. En esa corrida, el
evento tardío del escenario demo (E8) apareció en un agregado; ese resultado no
se usa como prueba de allowed lateness.

Resultados de esa corrida: 10 eventos de entrada, 1 evento al DLQ, 3 agregados
producidos, 3 filas en SQLite, `transaction_count` total 6, `total_amount_pyg`
total 1 527 772. El duplicado del escenario no infló el agregado. El consumer
group de Beam quedó con lag 0 y el del materializer también. Las referencias son
`evidence/block-04-e2e/reconciliation.txt` y
`evidence/block-04-e2e/e2e-summary.txt`.

Esos números pertenecen a esa corrida. Una nueva ejecución con `--start-now`
puede producir una distribución distinta de ventanas y no siempre da 3
agregados.

## 25. Tests

110 tests, sin broker.

```text
uv run pytest -q
uv run ruff check src tests
```

| Archivo | Tests | Cobertura |
|---|---:|---|
| `test_producer.py` | 24 | generación reproducible y determinista, contrato, escenario demo, serialización, CLI y `KafkaEventWriter` |
| `test_validation.py` | 19 | parseo, reglas del contrato y `event_time` |
| `test_pipeline.py` | 6 | `ParseAndValidateFn` y primera capa Beam |
| `test_aggregation.py` | 10 | `CombineFn`, ventanas, `aggregate_key`, suma y conteo, filtrado de `DECLINED` y `PENDING` |
| `test_deduplication.py` | 8 | estado, timer, horizonte, duplicados |
| `test_late_events.py` | 4 | `ON_TIME`, `LATE`, allowed lateness, key estable |
| `test_out_of_order.py` | 2 | eventos fuera de orden y asignación por event time |
| `test_kafka_pipeline.py` | 14 | serialización Kafka, DLQ, extracción de value, bootstrap y argumentos |
| `test_materializer.py` | 23 | contrato, key Kafka, tabla, UPSERT, idempotencia, rollback |

Total: 24 + 19 + 6 + 10 + 8 + 4 + 2 + 14 + 23 = 110.
