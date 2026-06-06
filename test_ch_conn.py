import clickhouse_connect
import sys

try:
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
    print(f"Connected to ClickHouse: {client.server_version}")
    client.close()
except Exception as e:
    print(f"Failed to connect to ClickHouse: {e}")
    sys.exit(1)
