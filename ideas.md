# Ideas / mejoras

> **Estado (rama large-parallel-ingest):** ✅ APLICADOS y validados e2e (small 5/5) — (1) handshake id por gateway, (2) JSON→msgpack, (3) timestamp fast_datetime, (4) logging (saqué debug por-msg del hot-path + WARNING en nodos, cliente en INFO). ❌ PyPy DESCARTADO (ver abajo).
>
> **Resultados small:** serial 272s → paralelo(JSON) 253s → **combo CPython 178s** (~35% mejor, pipeline drain ~127→64s). Build OK, msgpack wheel musllinux en alpine.
>
> **PyPy probado y DESCARTADO:** small 5/5 pero **294s (más lento que el serial)**. Razón: msgpack en PyPy es Python PURO (no la C-ext que lo hace rápido en CPython) → PyPy y msgpack están en CONFLICTO. + JIT no amortiza en runs cortos + 44 intérpretes con más overhead. PyPy brilla con Python puro CPU-bound, pero msgpack le saca ese terreno. **Se quedó CPython.** (Para habilitar PyPy hizo falta: parser.py `class X[T]`→Generic, merge.py Callable con retorno — esos fixes quedaron, son correctos.)


## 1. Handshake: que el gateway asigne el `client_id` (a hacer)

**Problema:** hoy (rama `large-parallel-ingest`) el cliente genera su propio `client_id` y lo estampa en cada mensaje; el gateway reenvía los bytes crudos (passthrough). Eso deja al cliente decidir su id → riesgo de **colisión / spoof** entre clientes.

**Solución (mantiene el passthrough):** handshake de 2 vías.
1. Cliente conecta → manda `Hello` (sin id, pidiendo).
2. Gateway genera el `uuid`, lo registra en `ClientMonitor`, y lo devuelve con un `HelloAck(client_id)`.
3. Cliente recibe el id asignado **antes de spawnear los workers** y lo estampa en cada mensaje.

- Cambio chico: reordenar `Client._run` (handshake antes de `_send_transactions_parallel`); pasar el id a los workers (ya va por `Process` args). Nuevo mensaje `HelloAck`. Gateway `ClientHandler`: en vez de leer el id del cliente, lo genera y lo manda.
- **Sigue siendo passthrough:** el gateway acuñó el id, así que confía en verlo en los bytes crudos → no deserializa por batch.
- **Resuelve:** colisiones + control del namespace. **No** frena spoof deliberado (eso requeriría validar por mensaje = parsear = mataría el passthrough).

**Idea futura (anti-spoof total sin perder performance):** cambiar el wire format a `[id_prefijo_separable][body_crudo]` — el id como header de tamaño fijo, NO embebido en el JSON. El gateway **sobreescribe** el prefijo con el id de la conexión (que él controla) **sin parsear el body** → anti-spoof **y** passthrough. Es un rediseño del protocolo (`Connection`/`Message`), para deploy no confiable / multi-tenant.

---

## Mejoras de CPU (el sistema es CPU-bound, no RAM-bound)

> Con los 8 cores saturados estamos en el techo del **código actual**, no del hardware. El código gasta CPU al pedo. Más RAM NO acelera (rabbit pagina a disco, join spillea). Cores + código barato = velocidad.

### 2. Serialización binaria en vez de JSON (la palanca más grande) 🔥

Cada mensaje usa `json.dumps`/`json.loads` (`message.py`), y eso pasa en **cada hop**: el filtro deserializa cada batch de 2000, lo procesa, y **re-serializa a JSON para cada uno de los 5 sub-streams** de UC. JSON es lento (texto/encoding) → por eso los filtros están al 90% de CPU.
- **Acción:** reemplazar JSON por binario (msgpack / protobuf / pickle) en `Message.serialize/deserialize`.
- **Impacto:** alto, en todo el pipeline. **Esfuerzo:** medio.

### 3. Parseo de timestamp rápido (evitar `strptime`)

El parser del cliente hace `datetime.strptime` por transacción (lentísimo en Python). Y si los filtros re-parsean el timestamp para comparar fechas (en vez de comparar strings como el oráculo), se paga doble.
- **Acción:** parsear el timestamp a mano (slicing) o **comparar fechas como strings** (`"2022/09/01" <= ts`).
- **Impacto:** medio (acelera cliente Y filtros). **Esfuerzo:** bajo. **Riesgo:** bajo.

### 4. Bajar el logging a WARNING/ERROR

`LOGGING_LEVEL` default = INFO. Cada log cuesta CPU.
- **Acción:** poner WARNING/ERROR por env.
- **Impacto:** chico. **Esfuerzo:** trivial.

### 5. Runtime PyPy (JIT)

Correr los nodos CPU-bound bajo PyPy aceleraría el Python puro (parseo/serialización) ~2-5x.
- **Impacto:** alto potencial. **Riesgo:** compatibilidad (pika, pandas). Es la apuesta grande.

---

## Lo que NO ayuda

- **Más RAM** — no es el cuello (rabbit pagina a disco, join spillea, ~3GB libres en large).
- **Más instancias de nodos** — no hay cores libres; competirían y empeoraría.
