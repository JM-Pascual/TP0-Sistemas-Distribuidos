#!/bin/bash

# Extraccion de los parametros de entrada
OUTPUT_FILE_NAME=$1
AMOUNT_OF_EXTRA_CLIENTS=$2

# Verifica si existe una version de python3 está instalada
if ! command -v python3 &> /dev/null
then
    echo "No existe version de Python3 instalada. Es necesario instalar una version para ejecutar el script."
    exit 1
fi

# Ejecucion del script de python que genera el archivo con el nombre indicado
python3 create_compose_file.py $OUTPUT_FILE_NAME $AMOUNT_OF_EXTRA_CLIENTS