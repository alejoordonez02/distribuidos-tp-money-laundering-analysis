El supervisor es entonces el encargado de levantar nodos cuando se caen, proveyendo así *crash recovery*; pero qué pasa si se cae el supervisor?  
Para resolver esto se agregaron replicas del nodo supervisor, de manera tal que este último pasa de ser un único nodo a un *servicio tolerante a fallos*. En el clúster de supervisores, el *líder* es el encargado de llevar la cuenta del estado de los nodos del pipeline y de levantarlos en caso de caídas. Por otro lado, para controlar el estado del líder, las réplicas del clúster le mandan *pings* con un delay configurable, a los cuales el líder contesta con *pongs* para certificar que sigue vivo. Cuando una réplica detecta que el líder se cayó, entonces se inicia una *elección de líder*. En particular, se implementó la *bully leader election* por ser más simple de programar.  
La implementación de los supervisores consta principalmente de tres módulos:

- **Event loop.** Lee de la cola de eventos y handlea acordemente.
- **Runtime.** Lleva a cabo la funcionalidad del *rol* actual del supervisor (líder o réplica), implementado con un *strategy pattern*. Cuando se cae el líder, se pushea el evento *LeaderDown* en la cola de eventos.
- **Internal/node listener.** Acepta conexiones de otros nodos del clúster y las pushea en la cola de eventos.

Los eventos están implementados con una *multiple producer single consumer queue*: los productores son el runtime y el listener; y el consumidor es el event loop.  
El listener está basado en el *modelo de actores*, escucha mensajes y delega el manejo y la correspondiente contestación de los mismos al event loop.

Se optó por mantener conexiones TCP entre los nodos, para evitar casos borde de pérdida de mensajes en el clúster.  
Algo a notar de la implementación, es que busca minimizar la cantidad de conexiones activas entre nodos. Normalmente, cuando el líder está vivo, sólo existen $N-1$ conexiones, con $N$ la cantidad de nodos: las réplicas conectadas al líder para hacer el ping pong. Cuando se cae el líder, se inicializan sólo las conexiones necesarias para llevar a cabo la bully leader election: la réplica que detecta la caída instancia conexiones con aquellos otros nodos con un ID mayor al suyo, manda mensaje de elección, espera el ACK, y corta la conexión; y así sucesivamente hasta que la cantidad de acks recibidos para el mensaje *election* es cero, en cuyo caso el nodo sabe que es aquel con el mayor id del clúster y brodcastea entonces el mensaje *coordinator*, para volver a las $N-1$ conexiones de la normalidad.
