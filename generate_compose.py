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

# Define config file
config_file = "config_file_queries.config"

# Dictionary to store the environment variables of each service
env_vars = {}

# List for storing the services and their Dockerfile paths
services = []

# Read the configuration file
with open(config_file, "r") as file:
    service_name, dockerfile_path = None, None
    last_service_name = None
    for line in file:
        line = line.strip()
        if line.startswith("-"):
            # Service name and Dockerfile path
            line = line[1:]  # Delete the prefix '-'
            service_name, dockerfile_path = line.split("$")
            services.append((service_name.strip(), dockerfile_path.strip()))
        elif line.startswith("+"):
            # Environment variables
            line = line[1:]  # Delete the prefix '+'
            print(line)
            env_vars[service_name] = dict(item.split("=") for item in line.split("$"))
            if service_name in ("review_sentiment_worker", "filter_category_worker"):
                if service_name == "filter_category_worker":
                    print("SOY EL SERVICE NAME", service_name, "Y TENGO ESTOS ENV VARS", env_vars[service_name])
                    env_vars[service_name]["NEXT_WORKER_QUANTITY"] = env_vars["hash_title_worker"]["WORKERS_QUANTITY"]
                    print("EL WORKER QUANTITY DEL HASH_TITLE_WORKER ES", env_vars["hash_title_worker"]["WORKERS_QUANTITY"])
                    print("MI NUEVO NEXT WORKER QUANTITY ES", env_vars[service_name]["NEXT_WORKER_QUANTITY"])
            if last_service_name and "NEXT_WORKER_QUANTITY" in env_vars[last_service_name] and service_name not in ("review_sentiment_worker", "filter_category_worker", "mean_review_sentiment_worker"):
                if not "END" in env_vars[last_service_name]:
                    env_vars[last_service_name]["NEXT_WORKER_QUANTITY"] = env_vars[service_name]["WORKERS_QUANTITY"]
            if "ACCUMULATOR" in env_vars[service_name] and env_vars[service_name]["ACCUMULATOR"] == "True":
                if service_name in ("reviews_counter_worker", "mean_review_sentiment_worker"):
                    env_vars[service_name]["EOF_QUANTITY"] = env_vars["hash_title_worker"]["WORKERS_QUANTITY"]
                    current_quantity = int(env_vars[service_name]["WORKERS_QUANTITY"])
                    if service_name == "reviews_counter_worker":
                        env_vars["hash_title_worker"]["Q3_QUANTITY"] = current_quantity
                    else:
                        env_vars["hash_title_worker"]["Q5_QUANTITY"] = current_quantity
                    last_service_name = service_name
                    continue
                env_vars[service_name]["EOF_QUANTITY"] = env_vars[last_service_name]["WORKERS_QUANTITY"]
            last_service_name = service_name
        else:
            continue

# Generate docker-compose-dev2.yaml
with open("docker-compose-dev.yaml", "w") as outfile:
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
    outfile.write("      - WORKERS_Q1=3\n")
    outfile.write("      - WORKERS_Q2=3\n")
    outfile.write("      - WORKERS_Q3_TITLES=3\n")
    outfile.write("      - WORKERS_Q3_REVIEWS=3\n")
    outfile.write("      - WORKERS_Q5_TITLES=3\n")
    outfile.write("      - WORKERS_Q5_REVIEWS=4\n")
    outfile.write("\n")
    for service_name, dockerfile_path in services:
        if service_name in env_vars:
            # Write as many workers as specified in WORKERS_QUANTITY
            if "WORKERS_QUANTITY" in env_vars[service_name]:
                workers_quantity = int(env_vars[service_name]["WORKERS_QUANTITY"])
                for i in range(workers_quantity):
                    worker_name = f"{service_name}{str(i)}"
                    outfile.write(f"  {worker_name}:\n")
                    outfile.write(f"    container_name: {worker_name}\n")
                    outfile.write(f"    build:\n")
                    outfile.write(f"      context: .\n")
                    outfile.write(f"      dockerfile: {dockerfile_path}\n")
                    outfile.write(f"    environment:\n")
                    for key, value in env_vars[service_name].items():
                        if key == "WORKER_ID":
                            outfile.write(f"      - {key}={i}\n")
                        else:
                            outfile.write(f"      - {key}={value}\n")



