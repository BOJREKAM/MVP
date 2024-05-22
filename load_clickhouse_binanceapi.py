from binance.client import Client as Client_binance
import pandas as pd
import clickhouse_connect
import os


# Initialize the Binance client
api_key = os.getenv('API_KEY')
secret_key = os.getenv('SECRET_KEY')
password = os.getenv('CLICK_PASS')
client_b = Client_binance(api_key, api_secret)

# Fetch historical futures trades
symbol = 'BTCUSDT'
limit = 1000  # Number of trades to fetch
trades = client_b.futures_aggregate_trades(symbol=symbol, limit=limit)
#print(trades)
# Prepare data for insertion
data = []
for trade in trades:
    record = [
        trade['a'],  # Aggregate trade ID
        str(trade['p']),  # Price as an array of strings
        float(trade['q']),   # Quantity as an array of float
        trade['f'],  # First Trade ID
        trade['l'],  # Last Trade ID
        trade['T'],  # Trade Time
        int(trade['m']) 
    ]
    data.append(record)

#print(data)
client: clickhouse_connect.driver.Client

client = clickhouse_connect.get_client(
        host='pnja9ag8qw.europe-west4.gcp.clickhouse.cloud',
        user='default',
        password=password,
        secure=True
    )

client.command('DROP TABLE IF EXISTS binance_futures_data')

client.command('''
CREATE TABLE IF NOT EXISTS binance_futures_data (
    id UInt64,
    price Float64,
    quantity Float64,
    first_trade_id UInt64,
    last_trade_id UInt64,
    trade_time DateTime64(3, 'Asia/Istanbul'),
    is_buyer_market_maker UInt8
) ENGINE = MergeTree()
ORDER BY id;
''')

# Insert the data into ClickHouse table using insert method
client.insert(
    'binance_futures_data', 
    data, 
    column_names=['id', 'price', 'quantity', 'first_trade_id', 'last_trade_id', 'trade_time', 'is_buyer_market_maker']
)

print("Futures trade data inserted into ClickHouse successfully!")