import clickhouse_connect

client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
print("Daily prices count and date range for MSUMI:")
res = client.query("SELECT count(), min(trade_date), max(trade_date) FROM market_data.daily_prices WHERE symbol = 'MSUMI'")
print(res.result_rows)

print("\nFirst 5 rows for MSUMI:")
res = client.query("SELECT trade_date, open, high, low, close, volume FROM market_data.daily_prices WHERE symbol = 'MSUMI' ORDER BY trade_date DESC LIMIT 5")
for r in res.result_rows:
    print(r)

print("\nCorporate actions count for MSUMI:")
res = client.query("SELECT count() FROM market_data.corporate_actions WHERE symbol = 'MSUMI'")
print(res.result_rows)

client.close()
