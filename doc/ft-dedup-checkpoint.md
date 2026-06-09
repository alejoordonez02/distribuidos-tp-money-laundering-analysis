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

_Pendiente._

## Fase 3 — Propagar dedup/checkpoint a stateful principales

_Pendiente._
