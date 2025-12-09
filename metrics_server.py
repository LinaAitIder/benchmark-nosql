from prometheus_client import start_http_server
import time

if __name__ == '__main__':
    start_http_server(8000)
    print("Prometheus metrics server running on port 8000")
    print("This server will not interfere with InfluxDB configuration")
    while True:
        time.sleep(1)  # Keep server running indefinitely