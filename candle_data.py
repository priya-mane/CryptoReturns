import requests
import pandas as pd
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.exchange.coinbase.com"
PRODUCT_ID = "BTC-USD"
GRANULARITY = 60  # 1 minute

def fetch_candles(start, end):
    url = f"{BASE_URL}/products/{PRODUCT_ID}/candles"
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": GRANULARITY
    }
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.json()

end = datetime.now(timezone.utc)
start = end - timedelta(days=180)

all_candles = []

while end > start:
    chunk_start = max(start, end - timedelta(minutes=300))
    data = fetch_candles(chunk_start, end)
    all_candles.extend(data)
    end = chunk_start

df = pd.DataFrame(
    all_candles,
    columns=["time", "low", "high", "open", "close", "volume"]
)

df["time"] = pd.to_datetime(df["time"], unit="s")
df = df.sort_values("time").drop_duplicates("time")

print(df.head())
df.to_parquet("btc_usd_1m.parquet")
