# Start a Prometheus & Grafana server
## Prerequisites
- Docker
## Prometheus setup
Specify all targets that should be scraped by Prometheus in `targets.yaml`.
Every time the file is changed, Prometheus will automatically reload the configuration.

To start a Prometheus server along with Grafana, run following commands in this directory:
```
### Start the Prometheus server on localhost and port 9090:
docker run --network host -v ${PWD}:/etc/prometheus -it prom/prometheus
### Start Grafana on localhost and port 3000:
docker run --network host -it grafana/grafana
```
Alternatively you can execute the `run.sh` script.

Open Grafana in a Browser using http://localhost:3000/ and login using default credentials (admin, admin).

Go to `Dashboards` and import the `sdkv_dashboard.json`. As datasource select `Prometheus` with `Prometheus server URL` http://localhost:9090.

To check if all configured targets were detected and can be scraped by Prometheus you can open http://localhost:9090/targets. 