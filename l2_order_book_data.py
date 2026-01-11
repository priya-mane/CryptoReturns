import asyncio, json, time
import websockets

WS_URL = "wss://advanced-trade-ws.coinbase.com"

async def coinbase_collector(product_id="BTC-USD"):
    async with websockets.connect(
        WS_URL,
        ping_interval=20,
        ping_timeout=20,
        max_size=8 * 1024 * 1024,   # 8 MiB (or None)
    ) as ws:
        subs = [
            {"type": "subscribe", "channel": "level2", 
             "product_ids": [product_id]},
            {"type": "subscribe", "channel": "market_trades", 
             "product_ids": [product_id]},
            # heartbeats optional
        ]
        for msg in subs:
            await ws.send(json.dumps(msg))

        with open(f"cb_{product_id}_ws.jsonl", "a") as f:
            while True:
                raw = await ws.recv()
                f.write(json.dumps({"received_ts": time.time(), "payload": json.loads(raw)}) + "\n")

asyncio.run(coinbase_collector("BTC-USD"))
