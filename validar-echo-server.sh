#!/bin/bash

# Docker run utilizando una imágen más ligera de Alpine y ejecutando 3 comandos
# Actualizacion del manejador de paquetes apk e instalacion de netcat
# Validacion de la respuesta del servidor echo
# echo del resultado de la validacion

#docker run --rm -it --network=tp0_testing_net alpine:latest sh -c \
#    "apk update && apk add --no-cache netcat-openbsd && \
#    if response=\$(echo 'Hello!' | nc -w 10 server 12345) && [ \"\$response\" == \"Hello!\" ]; then \
#        echo 'action: test_echo_server | result: success'; \
#    else \
#        echo 'action: test_echo_server | result: fail'; \
#    fi"

TESTING_MESSAGE="Hello"

response=$(docker run -it --network=tp0_testing_net --rm alpine:latest sh -c "echo '$TESTING_MESSAGE' | nc -w 10 server 12345" | tr -d '\r\n')
if [ "$response" = "$TESTING_MESSAGE" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi