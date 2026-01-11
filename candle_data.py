import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.exchange.coinbase.com"
PRODUCT_IDS = [ 'BTC-USD', 'ETH-USD', 'SOL-USD', 'ADA-USD', 'LINK-USD' ]

GRANULARITY = 21600  # 6h (valid)
HISTORY_DAYS = 10*365  # 10 years
MAX_CANDLES_PER_CALL = 300

def fetch_candles(start, end, product_id):
    url = f"{BASE_URL}/products/{product_id}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": GRANULARITY
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

all_candles = []

window = timedelta(seconds=GRANULARITY * MAX_CANDLES_PER_CALL)

for product_id in PRODUCT_IDS:
    print(f"Fetching data for {product_id}")
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=HISTORY_DAYS)

    while end > start:
        chunk_start = max(start, end - window)
        data = fetch_candles(chunk_start, end, product_id)

        if not data:
            end = chunk_start
            continue

        # prepend ticker to every row
        data = [[product_id] + row for row in data]
        all_candles.extend(data)

        end = chunk_start

df = pd.DataFrame(
    all_candles,
    columns=["ticker", "time", "low", "high", "open", "close", "volume"]
)

df["time"] = pd.to_datetime(df["time"], unit="s")
df = df.sort_values(["ticker", "time"]).drop_duplicates(["ticker", "time"])

print(df.head())
df.to_parquet("./data/crypto_6h_10y.parquet")
