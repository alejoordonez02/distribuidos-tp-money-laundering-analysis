# Fault tolerance: dedup + checkpoint en nodos stateful

Rama: `ft/dedup-checkpoint-stateful`

Objetivo global: tolerar la caída temporal de procesos propios y seguir produciendo
resultados consistentes (el resultado con fallas debe coincidir con el baseline sin
fallas). RabbitMQ se asume estable y queda fuera del modelo de fallas.

Avance incremental en una sola rama: cada fase se implementa, se prueba y se valida
contra el baseline (dataset small/perfect) antes de pasar a la siguiente.

---

## Fase 1 — Identidad liviana de mensajes

### Objetivo
Que todo mensaje transporte una identidad estable de productor/ruta + una secuencia
incremental, para habilitar dedup en Fase 2 sin guardar el set de todos los IDs vistos.

### Diseño
La identidad va en el **header del wire format**, no dentro del payload de cada mensaje.
Así el cambio queda en la clase base `Message` y lo heredan todos los tipos sin tocar
sus `_fields()`, y se puede estampar a nivel bytes (mismo patrón que el `client_id`).

Wire format:

```
[type:1][client_id:16][producer_id:16][seq:8][msgpack payload]
```

- `client_id`: el tenant, viaja punta a punta. No cambia.
- `producer_id`: identifica la (instancia de nodo + ruta de salida) que puso el mensaje
  en la cola. Es por hop: cuando un nodo deriva un mensaje nuevo, estampa su propio
  `producer_id` y una `seq` fresca.
- `seq`: contador monótono por (producer_id, cola de salida).

`producer_id` se deriva determinísticamente de `(tx_name, idx, route)` con `uuid5`, así
es estable ante restart y único por replica y por ruta.

El estampado lo hace `StampingMOM`, un decorator de `MOM` que envuelve cada tx y reescribe
los bytes del header en `send()`. Los mensajes de control (EOF, ring, hello, fin) **no**
se estampan: deja el EOF ring intacto y no consume números de secuencia. Los clones de un
`StampingMOM` comparten el contador, de modo que data y EOF enviados por handles clonados
a la misma ruta mantienen una sola secuencia.

Enganche en `make_rx_tx`: cubre los nodos que rutean por afinidad (aggregate, group_by,
filter no-default) sin cambiar la firma ni los callers. El gateway preserva el header al
re-estampar el `client_id`.

### Archivos
- `src/common/comms/messages/message.py` — wire format extendido (offsets, serialize/deserialize).
- `src/common/comms/messages/__init__.py` — exports de constantes nuevas.
- `src/common/comms/middleware/stamping_mom.py` — `StampingMOM` + `derive_producer_id` (nuevo).
- `src/common/comms/middleware/__init__.py` — exports.
- `src/common/comms/middleware/make_rx_tx.py` — envuelve cada tx en `StampingMOM`.
- `src/gateway/client_stream_handler.py` — `_stamp_id` preserva `producer_id`/`seq`.
- `src/common/fault_injection.py` — helper `maybe_crash` (nuevo, off por default).
- `test/test_message_identity.py`, `test/test_stamping_mom.py`, `test/test_fault_injection.py` (nuevos).

### Fault injection
`maybe_crash("punto")` en `src/common/fault_injection.py`. Off salvo `FAULT_INJECTION=1`.
Se dispara con `FAULT_CRASH_POINT=<punto>` (y opcional `FAULT_CRASH_NODE=<id>`). En Fase 1
queda scaffoldeado sin puntos cableados; los puntos concretos arrancan en Fase 2.

### Validación
- Unit: 12/12 (round-trip de serialización, stamping, fault injection no-op).
- E2E: `perfect_sample` 5/5 UCs == baseline. Stamping transparente de punta a punta.

### Pendiente / riesgos
- `seq` no es estable ante restart todavía (contador en memoria) → se ata al checkpoint en Fase 2.
- Productores que no pasan por `make_rx_tx` (converter, merge, join, default-filter, gateway)
  emiten `producer_id` null por ahora → reciben stamping en Fase 2/3 (resolviendo ahí la
  disambiguación de replicas en nodos sin IDX).

---

## Fase 2 — Dedup + checkpoint (PoC en un nodo stateful)

### Objetivo
PoC completa de deduplicación + checkpoint en un nodo stateful simple, recuperable
ante caída.

### Nodo elegido
`Aggregate` + `UC2MaxAmountAggregateFn` (máximo por banco/cliente). Estado acumulado
trivial de validar; su upstream (group_by UC2 max) ya estampa `producer_id+seq`.
`NPEERS=1` → usa `StatefulSingleNodeEOFHandler`, que no compara contadores de EOF, así
que el PoC no necesita persistir contadores (eso es Fase 3 para los nodos en ring).

### Diseño
Componentes nuevos en `src/common/checkpoint/` (contra interfaces, reutilizables):
- `Deduplicator`: `last_seq[producer_id]`. Descarta `seq <= last_seq`. No deduplica
  `seq == 0` (mensajes sin estampar). Liviano: no guarda el set de ids vistos.
- `CheckpointStore`: persistencia atómica (temp + fsync + `os.replace`) en `STATE_DIR`.
- `Checkpointer`: coordina dedup + checkpoint batcheado. Retiene los ACK hasta que el
  checkpoint que los cubre es durable, así una caída nunca pierde un efecto ya ACKeado.

Interfaz: `AggregateFn` expone `snapshot_state()/restore_state()` (default
`NotImplementedError`); sólo `UC2MaxAmount` los implementa. El controller decide cuándo
persistir/ACKear; la fn sólo sabe serializar su estado.

### Política de ACK/checkpoint
Batcheado y configurable: `CHECKPOINT_EVERY` mensajes aplicados → un checkpoint + flush
de los ACK retenidos. En EOF se hace flush del batch antes de procesarlo. Requiere
`prefetch >= CHECKPOINT_EVERY` (el rx ya usa `prefetch=10`).

Recuperación:
- Caída tras aplicar y antes del checkpoint → el efecto está sólo en RAM y el msg no se
  ACKeó → RabbitMQ lo reentrega → `last_seq` persistido no lo cubre → se reprocesa.
- Caída tras checkpoint y antes del ACK → al revivir, `last_seq` ya lo cubre → la
  reentrega se descarta. Sin doble conteo.

### Colas durables (prerequisito)
El rx con afinidad declaraba la cola `exclusive=True` → al caer el nodo, RabbitMQ borraba
la cola y se perdían los mensajes. Para el modelo de fallas ("RabbitMQ acumula hasta que
el nodo vuelve") el rx de un nodo con checkpoint se declara **no-exclusivo + durable**
(`make_rx_tx(durable_rx=True)` cuando hay `STATE_DIR`).

### Persistencia
`STATE_DIR` (env) → volume `./state/{node}` montado vía `gen_compose` sólo en el nodo con
checkpoint. Nunca `/tmp`. `state/` está en `.gitignore`.

### Fault injection (puntos cableados)
`maybe_crash(...)` en: `after_apply_before_checkpoint`, `after_checkpoint_before_ack`,
`after_dup_before_ack`, `after_restore_on_startup`. Off por default. Se activa con
`FAULT_INJECTION=1` + `FAULT_CRASH_POINT=<punto>` en el env del nodo.

### Archivos
- `src/common/checkpoint/{deduplicator,checkpoint_store,checkpointer,__init__}.py` (nuevos).
- `src/aggregate/aggregate_fns/aggregate_fn.py`, `uc2_max_amount.py` — snapshot/restore.
- `src/aggregate/aggregate.py`, `main.py` — wiring del checkpointer + durable_rx.
- `src/common/comms/middleware/exchange_rabbitmq.py`, `make_rx_tx.py` — rx no-exclusivo.
- `scripts/gen_compose/src/gen_nodes.py`, `gen_uc2.py` — volume + env del estado.
- `test/test_{deduplicator,checkpoint_store,checkpointer,uc2_snapshot}.py` (nuevos).

### Validación
- Unit: 29/29 (dedup, store atómico, batching/dedup/restore del checkpointer, snapshot UC2).
- E2E normal (perfect): 5/5 == baseline + `.ckpt` creado + restore al reiniciar.
- E2E con falla (manual): crash en `after_checkpoint_before_ack`, recreación del nodo,
  reentrega deduplicada, **5/5 == baseline**. Logs confirman restore + downstream EOF.

### Cómo reproducir el crash test (manual)
1. `STATE_DIR` ya está en el nodo. Limpiar `state/` (vía contenedor root) y `responses/`.
2. Agregar al env del `uc2_max_amount_aggregate_0` en `docker-compose.yaml`:
   `FAULT_INJECTION=1`, `FAULT_CRASH_POINT=after_checkpoint_before_ack`.
3. `docker compose up -d` → el nodo cae tras el primer checkpoint.
4. Poner `FAULT_INJECTION=0` y `docker compose up -d --no-deps uc2_max_amount_aggregate_0`.
5. Esperar a los clientes y `make test`.

### Pendiente / riesgos
- Crash en la ruta de EOF (producción de resultados, async en otro thread) fuera de
  alcance: el EOF se ACKea al encolarse. Documentado para Fase 3.
- Persistencia de contadores del EOF ring (nodos multi-peer) → Fase 3.
- Limpieza de `state/` viejo entre corridas es manual (root-owned); evitar contaminación
  con `last_seq` viejo (producer_id es estable entre runs).

## Fase 3 — Propagar dedup/checkpoint a stateful principales

_Pendiente._
