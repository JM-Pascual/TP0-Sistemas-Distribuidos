#!/bin/bash

# Docker run utilizando una imágen de Alpine y ejecutando 3 comandos
# Actualizacion del manejador de paquetes apt e instalacion de netcat
# Validacion de la respuesta del servidor echo
# echo del resultado de la validacion
docker run -d --rm --network=tp0_testing_net --name test_echo_server alpine:latest sh -c "while true; do sleep 1; done"

docker exec test_echo_server apk update

docker exec test_echo_server apk add netcat-openbsd

response=$(echo 'Hello!' | docker exec -i test_echo_server nc server 12345)
if [ "$response" == "Hello!" ]; then
    echo 'action: test_echo_server | result: success'
else
    echo 'action: test_echo_server | result: fail'
fi

docker stop test_echo_server
