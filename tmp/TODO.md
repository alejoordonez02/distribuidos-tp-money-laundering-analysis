# TODOs
#### msg FIN (?)
Me di cuenta de un problema: qué pasa si el cliente no manda datos tales que sea posible mandarle las responses correspondientes a los cinco use cases?  
Por ej si mandara todo un dataset de transacciones q no tiene ninguna transacción en USD el uc1 nunca le va a llegar.

El tema es cómo hacemos que pare de esperar msjitos? O sea idealmente tendríamos un msg FIN que mandamos del server, pero el server tampoco va a poder saber a priori si todos los use cases le corresponden a cada cliente.

##### solución ?
Pensándolo mejor en realidad si un cliente te manda datos tales que no hay ninguna transacción en USD (en el ej. de uc1) entonces sí hay response: lista vacía. Bueno entonces vamos a tener que considerar eso para mandar mensajes entre controllers.  
Yo creo que vamos a tener, más q un msj Transaction/Account, msjs Transaction**s**/Account**s**, que contengan las múltiples instancias de sus respectivos datos.
