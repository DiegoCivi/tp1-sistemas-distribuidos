#!/bin/bash

# Ejecuta el comando docker compose en segundo plano
nohup docker compose -f docker-compose-test.yaml up --build --remove-orphans > docker_output.log 2>&1 &

# Obtiene el PID del último proceso ejecutado
PID=$!

# Verifica si el contenedor rabbit está en ejecución
# Si esta en ejecucion es porq el sistema ya empezo
while true; do
    sleep 2
    if [ "$( docker container inspect -f '{{.State.Running}}' rabbit )" == "true" ]; then
        echo "rabbit is up and running"
        break
    else
        echo "rabbit container not found"
    fi
done

# Como el sistema ya empezo, los clientes tambien
# Ahora esperamos a que los clientes mueran
CONTAINER_NAME="client_1"
client_2="client_2"
while true; do
    sleep 2
    STATUS=$(docker inspect --format='{{.State.Running}}' $CONTAINER_NAME)
    if [ STATUS == "false" ]; then
        echo "Both clients finished"
        break
    else
        echo "Clients are still up"
    fi
done


# Mantiene el script ejecutándose para poder capturar la salida del contenedor rabbit
# while true; do
#     sleep 60
# done
