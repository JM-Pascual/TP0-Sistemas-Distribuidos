import sys

file_header ="""name: tp0
services:"""

server_config = """
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
"""

network_config = """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

def get_client_config(client_id):
    return f"""
  client{client_id}:
    container_name: client{client_id}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={client_id}
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.yaml:/config.yaml
      - ./.data/agency-{client_id}.csv:/agency-data.csv
"""

def create_docker_compose(number_of_clients, output_file_name):
    with open(output_file_name, "w") as f:
        f.write(file_header)
        f.write(server_config)
        for i in range(number_of_clients):
            f.write(get_client_config(i+1))
        f.write(network_config)

if __name__ == "__main__":
    if len(sys.argv) != 3 or not sys.argv[2].isdigit():
        print("Usage: python3 create_compose_file.py <output_file_name> <num_extra_clients>")
        sys.exit(1)

    clients_number = int(sys.argv[2])
    output_file_name = sys.argv[1]

    if clients_number < 0:
        print("Number of extra clients must be greater or equal than 0")
        sys.exit(1)

    create_docker_compose(clients_number, output_file_name)