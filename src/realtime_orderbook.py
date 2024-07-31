import asyncio
import time
from decimal import Decimal
from typing import Dict, List, Tuple
from sortedcontainers import SortedDict
import argparse
from datetime import datetime, timezone
import os
import logging
from logging.handlers import TimedRotatingFileHandler

import aiohttp
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import websockets
from websockets.exceptions import ConnectionClosed
import boto3
from botocore.client import Config
import ujson

# R2 storage configuration
R2_ENDPOINT_URL = "https://<accountid>.r2.cloudflarestorage.com"
R2_ACCESS_KEY_ID = "<access_key_id>"
R2_SECRET_ACCESS_KEY = "<access_key_secret>"
R2_BUCKET_NAME = "<your_bucket_name>"

SNAPSHOT_INTERVAL = 30  # seconds
MAX_LEVELS = 100  # Aggregate to 100 levels
WEBSOCKET_RECONNECT_INTERVAL = 23 * 60 * 60  # 23 hours in seconds
PING_INTERVAL = 3 * 60  # 3 minutes in seconds
PONG_TIMEOUT = 10 * 60  # 10 minutes in seconds
AGGTRADE_AGGREGATION_INTERVAL = 1  # 1 second to match order book updates

last_log_time = 0
last_pong_log_time = 0
LOG_INTERVAL = 60  # Log every 60 seconds
PONG_LOG_INTERVAL = 60  # Log pong every 60 seconds
MAX_RECONNECT_ATTEMPTS = 5
INITIAL_RECONNECT_DELAY = 1
MAX_RECONNECT_DELAY = 60

# Modify logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    "binance_data_collection.log",
    when="midnight",
    interval=1,
    backupCount=7,
    encoding="utf-8",
)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class AggTradeManager:
    def __init__(self):
        self.buy_volume = Decimal('0')
        self.sell_volume = Decimal('0')
        self.vwap_numerator = Decimal('0')
        self.total_volume = Decimal('0')
        self.total_trades = 0
        self.last_update_time = 0
        self.current_second = 0
        self.previous_data = None

    def process_aggtrade(self, event: Dict):
        price = Decimal(event['p'])
        quantity = Decimal(event['q'])
        is_buyer_maker = event['m']
        event_time = int(event['E'] // 1000)

        if event_time > self.current_second:
            self.previous_data = self.get_current_data()
            self.reset_data()
            self.current_second = event_time

        if is_buyer_maker:
            self.sell_volume += quantity
        else:
            self.buy_volume += quantity

        self.vwap_numerator += price * quantity
        self.total_volume += quantity
        self.total_trades += 1
        self.last_update_time = event_time

    def get_current_data(self):
        vwap = self.vwap_numerator / self.total_volume if self.total_volume > 0 else Decimal('0')
        return {
            "buy_volume": float(self.buy_volume),
            "sell_volume": float(self.sell_volume),
            "vwap": float(vwap),
            "total_trades": self.total_trades
        }

    def reset_data(self):
        self.buy_volume = Decimal('0')
        self.sell_volume = Decimal('0')
        self.vwap_numerator = Decimal('0')
        self.total_volume = Decimal('0')
        self.total_trades = 0

    def get_and_reset(self):
        current_data = self.get_current_data()
        if self.previous_data:
            result = self.previous_data
            self.previous_data = None
        else:
            result = current_data
        return result


class OrderBookManager:
    def __init__(self, symbol, granularity):
        self.symbol = symbol
        self.granularity = Decimal(str(granularity))
        self.bids = SortedDict(lambda x: -x)
        self.asks = SortedDict()
        self.last_update_id = 0
        self.previous_final_update_id = 0
        self.is_snapshot = True
        self.last_snapshot_time = 0
        self.last_save_time = 0

    def aggregate_price(self, price):
        return (price / self.granularity).quantize(Decimal('1')) * self.granularity

    def ensure_spread(self):
        if self.bids and self.asks:
            top_bid = next(iter(self.bids))
            top_ask = next(iter(self.asks))
            if top_ask <= top_bid:
                new_top_ask = self.aggregate_price(Decimal(str(top_bid)) + self.granularity)
                qty = self.asks.pop(top_ask)
                self.asks[new_top_ask] = qty

    async def get_snapshot(self):
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.symbol}&limit=1000"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                snapshot = await response.json()
                self.last_update_id = snapshot['lastUpdateId']
                self.bids.clear()
                self.asks.clear()
                for price, qty in snapshot['bids']:
                    agg_price = self.aggregate_price(Decimal(price))
                    self.bids[agg_price] = Decimal(qty)
                for price, qty in snapshot['asks']:
                    agg_price = self.aggregate_price(Decimal(price))
                    self.asks[agg_price] = Decimal(qty)
                
                self.ensure_spread()
                
                # Trim to MAX_LEVELS
                self.bids = SortedDict(lambda x: -x, list(self.bids.items())[:MAX_LEVELS])
                self.asks = SortedDict(list(self.asks.items())[:MAX_LEVELS])
        
        logger.info(f"Snapshot received for {self.symbol}. Last update ID: {self.last_update_id}")
        self.is_snapshot = True
        self.previous_final_update_id = 0
        self.last_snapshot_time = time.time()
        self.last_save_time = self.last_snapshot_time
        return self.get_current_state()

    def process_depth_event(self, event: Dict) -> bool:
        if event['u'] <= self.last_update_id:
            return False

        if self.previous_final_update_id == 0 or event['pu'] == self.previous_final_update_id:
            for bid in event['b']:
                price, qty = Decimal(bid[0]), Decimal(bid[1])
                agg_price = self.aggregate_price(price)
                if qty == 0:
                    self.bids.pop(agg_price, None)
                else:
                    self.bids[agg_price] = qty
            
            for ask in event['a']:
                price, qty = Decimal(ask[0]), Decimal(ask[1])
                agg_price = self.aggregate_price(price)
                if qty == 0:
                    self.asks.pop(agg_price, None)
                else:
                    self.asks[agg_price] = qty

            self.ensure_spread()

            # Trim to MAX_LEVELS
            self.bids = SortedDict(lambda x: -x, list(self.bids.items())[:MAX_LEVELS])
            self.asks = SortedDict(list(self.asks.items())[:MAX_LEVELS])

            self.previous_final_update_id = event['u']
            self.is_snapshot = False
            
            current_time = int(time.time())
            if current_time > self.last_save_time:
                self.last_save_time = current_time
                return True
            
            return False
        else:
            print(f"Out of sync for {self.symbol}. Previous update ID: {self.previous_final_update_id}, Current event previous update ID: {event['pu']}")
        
        return False

    def get_current_state(self) -> Dict[str, List[Tuple[float, float]]]:
        if self.symbol == "BTCUSDT":
            return {
                "bids": [(int(price), float(qty)) for price, qty in self.bids.items()],
                "asks": [(int(price), float(qty)) for price, qty in self.asks.items()],
                "is_snapshot": self.is_snapshot,
                "last_update_id": self.last_update_id
            }
        else:
            return {
                "bids": [(float(price), float(qty)) for price, qty in self.bids.items()],
                "asks": [(float(price), float(qty)) for price, qty in self.asks.items()],
                "is_snapshot": self.is_snapshot,
                "last_update_id": self.last_update_id
            }

async def save_state(state: Dict, aggtrade_data: Dict, file_path: str):
    global last_log_time
    current_time = time.time()
    
    df = pd.DataFrame({
        "timestamp": [pd.Timestamp.now(tz='UTC').floor('ms')],
        "bids": [ujson.dumps(state["bids"])],
        "asks": [ujson.dumps(state["asks"])],
        "is_snapshot": [state["is_snapshot"]],
        "last_update_id": [state["last_update_id"]],
        "buy_volume": [aggtrade_data["buy_volume"]],
        "sell_volume": [aggtrade_data["sell_volume"]],
        "vwap": [aggtrade_data["vwap"]],
        "total_trades": [aggtrade_data["total_trades"]]
    })

    table = pa.Table.from_pandas(df)
    
    if os.path.exists(file_path):
        existing_table = pq.read_table(file_path)
        combined_table = pa.concat_tables([existing_table, table])
        pq.write_table(combined_table, file_path)
    else:
        pq.write_table(table, file_path)
    
    if current_time - last_log_time >= LOG_INTERVAL:
        logger.info(f"Order book {'snapshot' if state['is_snapshot'] else 'update'} and AggTrade data saved to {file_path}.")
        last_log_time = current_time

async def upload_to_r2(local_file_path: str, r2_object_key: str):
    s3 = boto3.client('s3',
                      endpoint_url=R2_ENDPOINT_URL,
                      aws_access_key_id=R2_ACCESS_KEY_ID,
                      aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                      config=Config(signature_version='s3v4'))
    
    try:
        s3.upload_file(local_file_path, R2_BUCKET_NAME, r2_object_key)
        logger.info(f"Successfully uploaded {local_file_path} to R2 storage as {r2_object_key}")
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to R2 storage: {str(e)}")

async def regular_snapshot(manager, aggtrade_manager, file_path):
    while True:
        logger.info(f"Fetching regular snapshot for {manager.symbol}...")
        snapshot_state = await manager.get_snapshot()
        aggtrade_data = aggtrade_manager.get_and_reset()
        await save_state(snapshot_state, aggtrade_data, file_path)
        await asyncio.sleep(SNAPSHOT_INTERVAL)

async def check_for_new_day(symbol, manager, aggtrade_manager, file_path):
    current_date = datetime.now(timezone.utc).date()
    while True:
        await asyncio.sleep(60)  # Check every minute
        now = datetime.now(timezone.utc)
        if now.date() > current_date:
            current_date = now.date()
            r2_object_key = f"{symbol}-M/{os.path.basename(file_path)}"
            await upload_to_r2(file_path, r2_object_key)
        
            new_file_name = f"{symbol}_{current_date}.parquet"
            new_file_path = os.path.join("orderbook_data", new_file_name)
            initial_snapshot = await manager.get_snapshot()
            aggtrade_data = aggtrade_manager.get_and_reset()
            await save_state(initial_snapshot, aggtrade_data, new_file_path)
            logger.info(f"Started new file for {symbol}: {new_file_name}")
            return new_file_path  # Return the new file path


async def handle_websocket(websocket, symbol, manager, aggtrade_manager, file_path):
    global last_pong_log_time
    last_ping_time = time.time()
    last_pong_time = time.time()
    pong_count = 0

    async def send_ping():
        nonlocal last_ping_time
        while True:
            await asyncio.sleep(PING_INTERVAL)
            try:
                await websocket.ping()
                last_ping_time = time.time()
                logger.info(f"Sent ping to {symbol} WebSocket")
            except Exception as e:
                logger.error(f"Failed to send ping for {symbol}: {e}")
                return


    async def check_pong_timeout():
        nonlocal last_pong_time
        while True:
            await asyncio.sleep(10)  # Check every 10 seconds
            if time.time() - last_pong_time > PONG_TIMEOUT:
                logger.warning(f"Pong timeout for {symbol}, closing connection")
                await websocket.close()
                return

    ping_task = asyncio.create_task(send_ping())
    pong_checker = asyncio.create_task(check_pong_timeout())

    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=PONG_TIMEOUT)
                last_pong_time = time.time()
                pong_count += 1

                current_time = time.time()
                if current_time - last_pong_log_time >= PONG_LOG_INTERVAL:
                    logger.info(f"Received {pong_count} pongs from {symbol} WebSocket in the last {PONG_LOG_INTERVAL} seconds")
                    last_pong_log_time = current_time
                    pong_count = 0

                if isinstance(message, bytes):
                    continue

                event = ujson.loads(message)
                if 'stream' in event:
                    stream_name = event['stream']
                    event_data = event['data']
                    
                    if 'e' in event_data and event_data['e'] == 'aggTrade':
                        aggtrade_manager.process_aggtrade(event_data)
                    elif stream_name.endswith('@depth'):
                        if manager.process_depth_event(event_data):
                            state = manager.get_current_state()
                            aggtrade_data = aggtrade_manager.get_and_reset()
                            await save_state(state, aggtrade_data, file_path)
                else:
                    logger.warning(f"Received unexpected message format for {symbol}: {event}")

            except asyncio.TimeoutError:
                logger.warning(f"No message received for {symbol} in {PONG_TIMEOUT} seconds")
                await websocket.close()
                return
    except ConnectionClosed as e:
        logger.error(f"WebSocket connection closed for {symbol}: {e}")
    except Exception as e:
        logger.error(f"Error in WebSocket handler for {symbol}: {e}")
    finally:
        ping_task.cancel()
        pong_checker.cancel()


async def maintain_websocket_connection(symbol, manager, aggtrade_manager, file_path):
    url = f"wss://fstream.binance.com/stream?streams={symbol.lower()}@depth/{symbol.lower()}@aggTrade"
    reconnect_attempt = 0
    reconnect_delay = INITIAL_RECONNECT_DELAY

    while True:
        try:
            async with websockets.connect(url) as websocket:
                logger.info(f"WebSocket connected for {symbol}")
                reconnect_attempt = 0
                reconnect_delay = INITIAL_RECONNECT_DELAY
                await handle_websocket(websocket, symbol, manager, aggtrade_manager, file_path)
        except Exception as e:
            logger.error(f"WebSocket connection error for {symbol}: {e}")
            reconnect_attempt += 1
            if reconnect_attempt > MAX_RECONNECT_ATTEMPTS:
                logger.critical(f"Max reconnection attempts reached for {symbol}. Exiting.")
                return
            logger.info(f"Reconnecting WebSocket for {symbol} in {reconnect_delay} seconds... (Attempt {reconnect_attempt}/{MAX_RECONNECT_ATTEMPTS})")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)

async def main(symbol: str):
    granularity = {
        "BTCUSDT": 1,
        "ETHUSDT": 0.1,
        "BNBUSDT": 0.1,
        "SOLUSDT": 0.01
    }.get(symbol, 0.01)  # Default to 0.01 if symbol not in the list

    manager = OrderBookManager(symbol, granularity)
    aggtrade_manager = AggTradeManager()
    current_date = datetime.now(timezone.utc).date()
    file_name = f"{symbol}_{current_date}.parquet"
    file_path = os.path.join("orderbook_data", file_name)

    # Ensure the orderbook_data directory exists
    os.makedirs("orderbook_data", exist_ok=True)

    snapshot_task = asyncio.create_task(regular_snapshot(manager, aggtrade_manager, file_path))
    new_day_task = asyncio.create_task(check_for_new_day(symbol, manager, aggtrade_manager, file_path))

    try:
        # Initial snapshot
        initial_snapshot = await manager.get_snapshot()
        initial_aggtrade_data = aggtrade_manager.get_and_reset()
        await save_state(initial_snapshot, initial_aggtrade_data, file_path)

        websocket_task = asyncio.create_task(maintain_websocket_connection(symbol, manager, aggtrade_manager, file_path))
        
        # Wait for all tasks to complete
        while True:
            done, pending = await asyncio.wait(
                [snapshot_task, new_day_task, websocket_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in done:
                if task == new_day_task:
                    file_path = task.result()
                    new_day_task = asyncio.create_task(check_for_new_day(symbol, manager, aggtrade_manager, file_path))
                    # Update the file_path in the websocket_task
                    websocket_task.cancel()
                    await asyncio.sleep(1)  # Give some time for the task to cancel
                    websocket_task = asyncio.create_task(maintain_websocket_connection(symbol, manager, aggtrade_manager, file_path))
                elif task == websocket_task:
                    if task.exception():
                        logger.error(f"WebSocket task for {symbol} failed with exception: {task.exception()}")
                    websocket_task = asyncio.create_task(maintain_websocket_connection(symbol, manager, aggtrade_manager, file_path))
                elif task == snapshot_task:
                    snapshot_task = asyncio.create_task(regular_snapshot(manager, aggtrade_manager, file_path))
                
            if not pending:
                break
        
    except asyncio.CancelledError:
        for task in [snapshot_task, new_day_task, websocket_task]:
            if not task.done():
                task.cancel()
        try:
            await asyncio.gather(snapshot_task, new_day_task, websocket_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance Futures Order Book and AggTrade Data Collection")
    parser.add_argument("symbol", type=str, help="Trading symbol (e.g., BTCUSDT)")
    args = parser.parse_args()

    while True:
        try:
            asyncio.run(main(args.symbol))
        except Exception as e:
            logger.critical(f"Critical error in main loop for {args.symbol}: {e}")
        logger.info(f"Restarting main loop for {args.symbol}")
        time.sleep(5)  # Short delay before restarting
