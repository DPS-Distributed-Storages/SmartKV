#!/bin/bash

# Start Prometheus server
start_prometheus() {
  echo "Starting Prometheus server ..."
  docker run --network host -v ${PWD}:/etc/prometheus -d --name prometheus_server -it prom/prometheus
}

# Start Grafana
start_grafana() {
    echo "Starting Grafana server ..."
  docker run --network host -d --name grafana_server -it grafana/grafana
}

# Stop and remove Docker containers
stop_containers() {
  echo "stopping containers ..."
  docker stop prometheus_server
  docker rm prometheus_server
  docker stop grafana_server
  docker rm grafana_server
  echo "containers stopped"
  exit
}

# Start Prometheus and Grafana
start_prometheus
start_grafana

sleep 5s

echo "... Started Prometheus and Grafana servers ..."

xdg-open http://localhost:3000/
xdg-open http://localhost:9090/

# Wait for user input to stop the script
echo "Press Ctrl+C to stop the script and terminate Docker containers."
trap stop_containers SIGINT
sleep infinity
