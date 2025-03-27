# Ejercicio 7

>Modificar los clientes para que notifiquen al servidor al finalizar con el envío de todas las apuestas y así proceder con el sorteo. 
>Inmediatamente después de la notificacion, los clientes consultarán la lista de ganadores del sorteo correspondientes a su agencia.

## Lógica para la ejecución del sorteo

Siguiendo la misma lógica comunicativa del anterior ejercicio, se implementó la posibilidad de que el servidor pueda, además de recibir apuestas, interpretar un nuevo mensaje por parte de los clientes:
```
<id_agencia>#<AWAITING_RESULTS>#@
```

Este anuncia al servidor que el cliente ha terminado de enviar todas las apuestas y que está listo para recibir los resultados del sorteo. 
Al recibir este mensaje, el servidor guarda internamente el socket mediante el cual se está comunicando con el cliente para luego utilizarlo para enviarle los resultados del sorteo.

Por parte de los clientes, estos se quedan blockeados en el `recv` esperando recibir los resultados una vez el servidor entienda que todos los clientes están esperando.

El servidor valida si es hora de sortear luego de cada nueva iteración sobre el método `run`, la cual puede darse o por el fin de un batch o por la recepción de un mensaje de espera.

Lógicamente esta verificación será negativa hasta que se reciba el último mensaje de espera, momento en el cual se realizará el sorteo, enviará los resultados a los clientes y se reiniciará el ciclo (Previamente liberando los recursos allocados).

Para no modificar la lógica contenida en el archivo `utils.py` ya que este era provisto por la cátedra, sucesivos sorteos stackearan los resultados de los anteriores. Una potencial mejora de esta POC sería llamar a algún método extra que libere el archivo donde se almacenaron las apuestas post realizada esta.