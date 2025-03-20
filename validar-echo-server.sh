#!/bin/bash

# Docker run utilizando una imágen de Ubuntu y ejecutando 3 comandos
# Actualizacion del manejador de paquetes apt e instalacion de netcat
# Validacion de la respuesta del servidor echo
# echo del resultado de la validacion
docker run --rm -it --network=tp0_testing_net ubuntu:latest bash -c \
    "apt update && apt-get install -y netcat-openbsd && \
    if response=\$(echo 'Hello!' | nc server 12345) && [ \"\$response\" == \"Hello!\" ]; then \
        echo 'action: test_echo_server | result: success'; \
    else \
        echo 'action: test_echo_server | result: fail'; \
    fi"
