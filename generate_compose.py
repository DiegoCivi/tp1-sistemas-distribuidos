#!/bin/bash

# Script to generate docker-compose-dev.yaml according to multiple config files named config_file_Qi 
# where Qi es the query number i
# Usage: ./generate_compose.sh
# The configuration file should be in the format:
# - service_name1, dockerfile_path
# + env_vars: key1=value1,key2=value2
# - service_name2, dockerfile_path
# + env_vars: key1=value1,key2=value2
# ...
# - service_name, dockerfile_path
# + env_vars: key1=value1,key2=value2

# Define el nombre del archivo de configuración
config_file = "config_file_queries.config"

# Diccionario para almacenar las variables de entorno de cada servicio
env_vars = {}

# Lista para almacenar los nombres de los servicios y las rutas del Dockerfile
services = []

# Leer el archivo de configuración línea por línea
with open(config_file, "r") as file:
    service_name, dockerfile_path = None, None
    for line in file:
        line = line.strip()
        if line.startswith("-"):
            # Procesar línea de nombre de servicio y Dockerfile
            line = line[2:]  # Eliminar el prefijo '-'
            service_name, dockerfile_path = line.split("$")
            services.append((service_name.strip(), dockerfile_path.strip()))
        elif line.startswith("+"):
            # Procesar línea de variables de entorno
            line = line[2:]  # Eliminar el prefijo '+'
            env_vars[service_name] = dict(item.split("=") for item in line.split("$"))
        else:
            continue

# Ahora, env_vars contiene las variables de entorno de cada servicio
# Y services contiene los nombres de los servicios y las rutas del Dockerfile

# Lógica para generar docker-compose-dev2.yaml
with open("docker-compose-dev2.yaml", "w") as outfile:
    outfile.write("services:\n")
    # Escribir servicios predefinidos
    outfile.write("  rabbitmq:\n")
    outfile.write("    container_name: rabbit\n")
    outfile.write("    build:\n")
    outfile.write("      context: ./rabbitmq\n")
    outfile.write("      dockerfile: rabbitmq.dockerfile\n")
    outfile.write("    ports:\n")
    outfile.write("      - 15672:15672\n")
    outfile.write("    healthcheck:\n")
    outfile.write("      test: [\"CMD\", \"curl\", \"-f\", \"http://localhost:15672\"]\n")
    outfile.write("      interval: 10s\n")
    outfile.write("      timeout: 5s\n")
    outfile.write("      retries: 10\n")
    outfile.write("\n")
    outfile.write("  server:\n")
    outfile.write("    container_name: server\n")
    outfile.write("    build:\n")
    outfile.write("      context: .\n")
    outfile.write("      dockerfile: ./server/server.dockerfile\n")
    outfile.write("    depends_on:\n")
    outfile.write("      - rabbitmq\n")
    outfile.write("    links:\n")
    outfile.write("      - rabbitmq\n")
    outfile.write("    environment:\n")
    outfile.write("      - PYTHONUNBUFFERED=1\n")
    outfile.write("      - HOST=server\n")
    outfile.write("      - PORT=12345\n")
    outfile.write("      - LISTEN_BACKLOG=1\n")
    outfile.write("\n")
    outfile.write("  client:\n")
    outfile.write("    container_name: client\n")
    outfile.write("    build:\n")
    outfile.write("      context: .\n")
    outfile.write("      dockerfile: ./client/client.dockerfile\n")
    outfile.write("    depends_on:\n")
    outfile.write("      - server\n")
    outfile.write("    links:\n")
    outfile.write("      - server\n")
    outfile.write("    environment:\n")
    outfile.write("      - PYTHONUNBUFFERED=1\n")
    outfile.write("      - HOST=server\n")
    outfile.write("      - PORT=12345\n")
    outfile.write("      - TITLES_FILEPATH=./datasets/books_data.csv\n")
    outfile.write("      - REVIEWS_FILEPATH=./datasets/books_rating_sample.csv\n")
    outfile.write("    volumes:\n")
    outfile.write("      - ./datasets:/datasets\n")
    outfile.write("\n")
    outfile.write("  query_coordinator_worker:\n")
    outfile.write("    container_name: query_coordinator_worker\n")
    outfile.write("    build:\n")
    outfile.write("      context: .\n")
    outfile.write("      dockerfile: ./query_coordinator/query_coordinator_worker.dockerfile\n")
    outfile.write("    depends_on:\n")
    outfile.write("      - rabbitmq\n")
    outfile.write("    links:\n")
    outfile.write("      - rabbitmq\n")
    outfile.write("    environment:\n")
    outfile.write("      - EOF_TITLES_MAX_SUBS=6\n")
    outfile.write("      - EOF_REVIEWS_MAX_SUBS=6\n")
    outfile.write("\n")
    for service_name, dockerfile_path in services:
        # Escribir información del servicio
        outfile.write(f"  {service_name}:\n")
        outfile.write(f"    container_name: {service_name}\n")
        outfile.write(f"    build:\n")
        outfile.write(f"      context: .\n")
        outfile.write(f"      dockerfile: {dockerfile_path}\n")
        if service_name in env_vars:
            # Escribir variables de entorno si están definidas para este servicio
            outfile.write("    environment:\n")
            for key, value in env_vars[service_name].items():
                outfile.write(f"      - {key}={value}\n")
            # Verificar si existe la variable WORKERS_QUANTITY
            if "WORKERS_QUANTITY" in env_vars[service_name]:
                workers_quantity = int(env_vars[service_name]["WORKERS_QUANTITY"])
                # Crear tantos contenedores como se especifique en WORKERS_QUANTITY
                for i in range(workers_quantity):
                    outfile.write(f"  {service_name}_worker_{i}:\n")
                    outfile.write(f"    container_name: {service_name}_worker_{i}\n")
                    outfile.write(f"    build:\n")
                    outfile.write(f"      context: .\n")
                    outfile.write(f"      dockerfile: {dockerfile_path}\n")
                    # Copiar las variables de entorno al nuevo contenedor
                    for key, value in env_vars[service_name].items():
                        if key == "WORKER_ID":
                            outfile.write(f"    environment:\n")
                            outfile.write(f"      - {key}={i}\n")
                        else:
                            outfile.write(f"      - {key}={value}\n")



