#!/usr/bin/env python3
import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
import clickhouse_connect

def main():
    if len(sys.argv) < 3:
        print("Usage: python repair_clickhouse_partition.py <table_name> <partition_id>")
        print("Example: python repair_clickhouse_partition.py daily_prices 202301")
        sys.exit(1)

    table = sys.argv[1]
    partition = sys.argv[2]
    
    try:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
    except Exception as e:
        print(f"Error connecting to ClickHouse: {e}")
        sys.exit(1)

    full_table_name = f"market_data.{table}"
    
    confirm = input(f"Are you sure you want to DROP partition {partition} from {full_table_name}? (y/N): ")
    if confirm.lower() != 'y':
        print("Aborted.")
        sys.exit(0)

    try:
        # Check if partition exists
        check_query = f"SELECT count() FROM system.parts WHERE table = '{table}' AND database = 'market_data' AND partition = '{partition}'"
        count = client.command(check_query)
        if int(count) == 0:
            print(f"Warning: Partition {partition} not found in system.parts for {full_table_name}.")
        
        drop_query = f"ALTER TABLE {full_table_name} DROP PARTITION '{partition}'"
        print(f"Executing: {drop_query}")
        client.command(drop_query)
        print("✅ Partition dropped successfully.")
        print("\nNote: You may want to reset the watermark for symbols in this month to trigger a re-import.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    main()
