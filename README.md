# Ejercicio 6

> Modificar los clientes para que envíen varias apuestas a la vez (modalidad conocida como procesamiento por chunks o batchs). 
> Los batchs permiten que el cliente registre varias apuestas en una misma consulta, acortando tiempos de transmisión y procesamiento.

## Desarrollo - Comunicación y Server Loop
Para el desarrollo del ejercicio 6 se definió el primer boceto de lo que sería el protocolo mediante el cual se comunicarían tanto Cliente/s como Servidor.
Básicamente se utilizaría el caracter `#` para separa los campos que componían a una apuesta particular, el campo `\n` para separar apuestas entre sí y el caracter `@` para indicar el fin de un batch que contiene apuestas.

Visualmente un batch podría observarse de la siguiente manera:
```
Sabiendo el formato:

#<id_agencia>#<nombre>#<apellido>#<dni>#<fecha_nacimiento>#<numero_de_apuesta>

Un ejemplo puede ser:

#1#Juan#Pascual#42777777#31-08-2000#7723\n#1#Juan#Pascual#42777777#31-08-2000#7723\n@
```

Del lado del servidor se escuchan conexiones entrantes, se recibe el batch entero y luego se cierra la conexión con el cliente para dar lugar a otra comunicación.
Del lado del cliente se establece conexión, se espera a ser atendido, se envía el batch en su totalidad, se espera la respuesta del servidor a modo de ACK de que se registró la totalidad del batch y se cierra la conexión.

El servidor indica que se registraron las apuestas mediante un mensaje del estilo:
```
Success on receiving {bets_received} bets#\n@
```
Aunque verdaderamente el cliente solo espera un mensaje de éxito que finalice con el caracter `@`.
Se podría extender sobre esta lógica para que el servidor envíe un mensaje de error en caso de que no se hayan registrado todas las apuestas, y de esa forma intentar enviar nuevamente el batch originalmente enviado. No obstante dentro de los límites del POC desarrollado no se implementó tal lógica.

## Desarrollo - Client Loop

El client loop en esencia es bastante simple. Antes de cada iteración del loop se consulta si se recibió una señal de alto en el canal específicamente dedicado a esto. En caso de que no, se entiende de que se debe loopear nuevamente. Las apuestas se consumen de otro canal que tiene como intención cargar las apuestas que tiene la agencia de manera incremental y no todas a la vez.
Esto se logra a partir de capear la capacidad máxima del canal. Al leer de este canal un EOF se entiende que no hay más apuestas para cargar y se pushea en el canal de finalización mencionado inicialmente.
También, debido al límite no solo de apuestas en el batch sino también de contenido en Bytes de los chunks a enviar, se creo una variable auxiliar que almacena mensajes encodeados que no se hayan podido agregar en la iteración anterior del loop (Es decir hayan sido dejados de lado para no exceder los límites de un chunk).