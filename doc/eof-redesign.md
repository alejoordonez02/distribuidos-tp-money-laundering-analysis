# EOF / ring redesign — diseño y plan de wiring

Estado al escribir esto: base limpia en `4b8ddc3` (+ commits siguientes con las
foundations). **Baseline 5/5** en topología 3-escalable. Backup de los intentos
descartados del ring en la branch `backup-ring-attempts`.

## El problema (por qué el ring actual es frágil)

Cada nodo corre **3 threads** que comparten estado: el de datos (controller), el del
ring (`_start_consuming_back`) y el de emisión (`internal_eofs`). El lock protege cada
mutación, pero **no la atomicidad entre "estado del ring + estado de negocio +
checkpoint"**. De ahí salen todos los bugs de crash-recovery:
- conteo destructivo (zeroing) pierde crédito si el zero no se checkpointea atómico;
- los flags de idempotencia se pierden o racean al flushear desde el thread del ring;
- los mensajes de control no son idempotentes ante redelivery (no hay fase persistida).

Son síntomas del **mismo problema de raíz: estado no-atómico entre threads.**

## La solución: single-thread + máquina de fases + affinity

1. **Un solo consumidor** lee las dos colas (datos + ring) en un thread →
   todo el estado se muta en un thread → **checkpoint atómico** → crash restaura un
   snapshot consistente. Idempotencia **por construcción** (la fase es la verdad).
2. **Modelo affinity**: cada peer tiene su cola dedicada y recibe **su propio EOF**.
   Sabe su completion local, emite local, y **una sola barrera** junta
   "todos terminaron + total enviado". Mucho más simple que las 3 fases actuales.

### Performance (verificado conceptualmente)
No regresiona en large: GIL ⇒ los 3 threads nunca dieron paralelismo de CPU (el
cuello es procesar datos, single-thread-bound igual); el ring es solo al EOF; affinity
round-robin reparte parejo. Único costo (rebalanceo dinámico de la work queue) es
marginal con workers homogéneos.

## Foundations YA construidas (committeadas, testeadas, aisladas)

- `src/common/comms/middleware/multi_consumer.py` — `MultiQueueConsumer`: consume N
  colas en un thread, prefetch por-consumidor (data retiene acks sin tapar el ring).
- `src/common/comms/eof_handler/ring_completion.py` — `RingCompletion`: máquina de
  fases pura (PROCESSING→EMITTED→DONE), sin I/O, snapshotable. Eventos: `on_data`,
  `on_upstream_eof`, `report_sent`, `on_token`. Acciones: `Emit`, `Forward`,
  `DownstreamEOF`. **6/6 unit tests** en `test/test_ring_completion.py`.

## El insight clave: affinity y "sacar working queue" son EL MISMO cambio

El ring nuevo (EOF por-peer) **requiere colas por-peer** = remover la working queue.
No son dos pasos. La cascada por el pipeline:

```
gateway (rutea per-peer a default_filter)
  → default_filter (affinity_upstream=True + EOF per-peer a group_by)
  → group_by (affinity + EOF per-peer a aggregate)
  → aggregate (RingCompletion) → merge → join
```

Cada etapa: `affinity_upstream=True` + el upstream con `naffinity_downstream=N` y
ruteo por shard (round-robin para stateless, por key para los que ya lo tienen) +
emisión de **un EOF por shard** (con el count de ese shard) en vez de un EOF de cluster.

## Plan de wiring (etapa por etapa, validar perfect en cada una)

1. **EOF por-peer en el emisor**: que el handler que manda el downstream EOF lo mande
   a CADA shard (no solo `external_txs[0]`) con el sent_data de ese shard. Es el
   cambio base que habilita `RingCompletion` aguas abajo.
2. **Aggregate** (ya es affinity): reemplazar `StatefulRingEOFHandler` + el thread de
   `internal_eofs` por `MultiQueueConsumer(data, ring)` + `RingCompletion`. Emitir
   inline en el mismo thread. Validar perfect (UC2/UC4).
3. **group_by / filter / converter**: convertir `affinity_upstream=False` → `True`,
   su upstream a `naffinity_downstream=N` + ruteo, y al mismo modelo single-thread.
   Validar perfect por UC.
4. **gateway**: rutear transactions per-peer a las colas de default_filter
   (round-robin) — esto mata el último working queue y el derived stamping.
5. **merge / join**: revisar (single por diseño; el merge lee left+right enteros).
6. Validar **perfect → small → FT** (`make test_ft`, todo a 3-escalable).

## Notas de topología (importante)

- **Reducers NO escalan**: `uc3_average` (promedio por formato) y `uc5_count` (count
  global) a 3 competing dan parciales sin combinar → baseline rompe. Quedan en 1
  (o necesitan affinity-por-key / un combinador). Merges/join: single por diseño.
- "3 por ring" aplica a lo escalable (filters, group_bys, aggregates affinity).

## Validación

- `make test` — e2e correctitud (perfect por defecto; cfg.py para otros datasets).
- `make test_ft` — fault-tolerance (scripts/ft_e2e.py): crashea cada controller en
  cada punto, descubre topología del compose. Knobs: FT_ONLY_NODES, FT_SKIP_NODES,
  FT_ONLY_POINTS, FT_ALL_REPLICAS, FT_TIMEOUT, FT_SKIP_GEN.
- Oráculo (`make gen_input_output`) en medium/large come ~10-15GB → correr bajo
  cgroup (`systemd-run --user --scope -p MemoryMax -p MemorySwapMax=0`) para no
  congelar el host.

## Lo que YA quedó crash-safe (sin tocar el ring nuevo)

Single-node EOF (counters checkpointeados), join (finalize idempotente), stateless
ring (conteo no-destructivo), gateway (unique stamping para dedup competing), restart
policy. Lo único que falla recovery es el competing (working queue) — que este
rediseño elimina.
