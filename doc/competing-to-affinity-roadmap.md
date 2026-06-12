# Roadmap: competing → affinity (crash-safe FT)

**OBJETIVO EXPLÍCITO: `make test_ft` 100% verde — TODOS los nodos, incluido lo
pre-existente.** No alcanza con dejar UC3 limpio: el grueso del pipeline son
competing-consumers (default_filter, group_bys y filters de los 5 UCs) y TODOS
stallean igual. Hay que convertirlos a **affinity per-peer + RingCompletion**, el
patrón que ya usan aggregate y merge. Es un proyecto de varias sesiones, nodo por
nodo, validando cada uno (correctitud perfect/small/medium + FT 10/10).

## Qué FUNCIONA (committeado + validado)

| Cosa | Commit | Validación |
|------|--------|-----------|
| UC3 **aggregate** affinity por formato | `6b21d03` | 5/5 perfect/small/medium |
| UC3 **merge** broadcast-join (`RingMerge`) | `6b21d03` | 5/5 + **FT 10/10** |
| UC3 **filter** affinity (`RingFilter`) | `655f896` | 5/5 perfect/small + **FT 10/10** |
| FT harness: `docker start` tras kill | `78d7da0` | KILL points ahora ejercitan recovery |

**Patrón de nodo affinity (reutilizable, ya probado):** `MultiQueueConsumer` (data +
ring en UN thread → checkpoint atómico) + `RingCompletion` (máquina de fases pura,
devuelve acciones Emit/Forward/DownstreamEOF) + `RingRabbitMQ`. Ver `ring_aggregate.py`,
`ring_merge.py`, `ring_filter.py`. El stateless usa `SentCounts` (cuenta por shard,
checkpointeada) y reporta al barrier en el EOF.

## Qué NO funciona (y por qué)

### 1. Nodos competing stallean en FT (PRE-EXISTENTE, probado con A/B)
`default_filter` y los group_bys/filters competing de UC1/UC2/UC4/UC5 stallean en
`during_checkpoint_write`, `after_checkpoint_before_ack`, `after_restore_on_startup`,
`after_dup_before_ack`. Causa: el nodo crashea a mitad del ring-round de EOF (stateless
ring), reinicia, pero **no retoma el round** → el EOF nunca propaga → cuelga.
A/B con `UC3_MERGES=1` (sin mis cambios) → stallea idéntico ⇒ NO lo introduje yo.

Detalle: `docker kill` lo trata el daemon como stop manual → `restart: on-failure` NO
reinicia (por eso el harness ahora hace `docker start`). Los crashes in-code (excepción
→ exit) SÍ reinician, pero el competing igual no retoma el ring-round.

### 2. Stage 2 (group_by affinity) — STASHEADO por BACKPRESSURE
Intenté convertir `uc3_group_by_format` a affinity (rama `stash@{0}`: `RingGroupBy` +
sharding de período A en el default_filter). **Deadlock por backpressure**: el
`default_filter` es competing con UN solo thread de consumo y manda SÍNCRONO a todas
sus rutas. Al shardear período A hacia group_bys affinity más lentos, las colas de
período A se llenan → flow control de RabbitMQ → el `send` bloquea el thread del
default_filter → deja de consumir (`processed_count` clavado) → **cuelga TODO el
pipeline** (incluso UC4, que no usa período A).

**LECCIÓN CLAVE — el orden importa:** no se puede convertir un downstream mientras su
upstream sigue siendo competing-de-un-thread. **Hay que convertir el `default_filter`
(la raíz) a affinity PRIMERO.** Es la "cascada" que advierte la nota de engram #109.

## Roadmap (orden corregido: upstream → downstream)

1. **`default_filter` → affinity (PRIMERO).** El gateway rutea transacciones por hash a
   colas per-default_filter; el default_filter usa un `RingFilter`-broadcast (consume su
   shard, reenvía a todas las UC queues). Esto elimina el thread único bloqueante.
   *Lo más grande: toca el gateway + el default_filter (router con N rutas).*
2. **`uc3_group_by_format` → affinity** (pop `stash@{0}`; ya tiene `RingGroupBy` +
   sharding de período A). Una vez el default_filter es affinity, sin backpressure.
3. **`uc3_average_filter`** ya está affinity (stage 1, hecho).
4. **Replicar a UC1/UC2/UC4/UC5** competing (group_bys, filters, converter). Mecánico
   con `RingFilter`/`RingGroupBy`. OJO: group_bys/filters STATEFUL (ej. UC4ComputeGraph
   acumula) NO sirven con el RingFilter/RingGroupBy stateless tal cual — revisar caso a
   caso (los de UC3 son stateless per-mensaje, por eso anduvieron).

## GOTCHA AMBIENTAL (crítico — costó horas)

El churn de docker (sweep de FT, A/B, runs repetidos) **acumula containers zombie y
volúmenes**. Llegamos a **62 containers corriendo (load 30)** → el pipeline GATEA →
timeouts que PARECEN bugs de código pero son starvation. Síntoma: backlogs enormes
(`uc4_graphs` con 13k mensajes, `client_transactions` 1000+) SIN un EOF trabado puntual.
**Antes de culpar al código: `docker ps | wc -l`, `uptime` (load), y limpiar
(`docker rm -f`, `docker volume prune`, `docker compose down`).** El commit `655f896`
está validado; los "fallos" del cierre fueron 100% ambientales.

## Cómo validar (recordatorio)
- Datasets en `scripts/cfg.py`. Cache de expected en `test/expected_cache/{small,medium}`
  → `cp` a `test/expected_responses/` (NO regenerar con gen_input_output salvo perfect).
- `ln -sf <Trans>.csv datasets/transactions_0.csv` (frac=1, NCLIENTS=1 = dataset entero).
- FT aislado: `FT_ONLY_NODES=<nodo> FT_ONLY_POINTS=<pts> FT_SKIP_GEN=1 make test_ft`.
