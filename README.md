# Ejercicio 8

> Modificar el servidor para que permita aceptar conexiones y procesar mensajes en paralelo.

## Lógica Client Loop

Los cambios en el cliente no fueron muchos, simplemente se abandonó el uso de diferentes sockets para cada batch enviado y se cambió por un enfoque en el cual el cliente envía todas sus apuestas, espera un ACK sobre el cargado de las mismas y luego se bloquea a esperar los resultados del sorteo.
Se agrego cobertura a todos aquellos casos de error en los cuales el socket del cliente podría permanecer abierto luego de un error ya sea en el envío del batch como en la recepción de una respuesta.

## Lógica Server Loop

En el servidor se realizaron cambios más significativos.
Primero y principal, aparecieron varios Threads que se encargan de diferentes tareas que antes se ejecutaban secuencialmente.
- El Thread principal toma el rol de Acceptor Thread, permaneciendo bloqueado en la función accept() esperando nuevas conexiones y, al recibirlas, creando un nuevo Thread encargado de atender al cliente.
- Los clientes son atendidos en paralelo mediante Threads liberados por el Acceptor Thread. Cada uno de estos Threads se encarga de recibir los mensajes del cliente, procesarlos, cargarlos y enviar un ACK.
- Se agregó un "Lottery Thread", el cual es un Thread que se encuentra a la espera de que lo notifique el Monitor de apuestas (Sobre el que se va a elaborar más adelante).

### Monitor de apuestas

La clase BetsMonitor se encarga de encapsular toda la lógica que necesita exclusión mutua.
- Encapsula un lock el cual permite que solo uno de los threads que atienden clientes acceda al método store_bets() el cual NO es Thread safe.
- Recibe las notificaciones, también con exclusión, de los clientes cuando estos terminan de enviar sus apuestas y desean esperar los resultados.
- Notifica al Lottery Thread que ya se cargaron todas las apuestas y que puede realizar el sorteo. Esto lo hace mediante a una Conditional Variable la cual expone una interfaz pública para esperarla.

Una salvedad importante es que el BetsMonitor es también el encargado de guardar los sockets de los clientes una vez notifican que desean esperar noticias, principalmente para evitar el acceso no controlado a estructuras de datos que almacenen sockets.