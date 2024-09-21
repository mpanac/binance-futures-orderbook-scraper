import os
from dotenv import load_dotenv
import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor
import multiprocessing
import aiohttp
import websockets
import ujson
from numba import njit
from decimal import Decimal
from datetime import datetime, time, timedelta, UTC
import logging
import pyarrow as pa
import pandas as pd
from fastparquet import ParquetFile, write
import numpy as np
import boto3
from botocore.client import Config
import traceback
import queue
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# load env variables
load_dotenv()

# R2 storage configuration
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")

SYMBOLS = ["BTCUSDT", "ETHUSDT", "TONUSDT", "BNBUSDT", "SOLUSDT", "SUIUSDT"]
SNAPSHOT_INTERVAL = 30  # seconds
BUFFER_FLUSH_INTERVAL = 60  # seconds
BUFFER_SIZE_THRESHOLD = 1000  # number of items

@njit
def aggregate_order_book_numba(prices, quantities, levels=100, chunk_size=10):
    sorted_indices = np.argsort(prices)
    if len(prices) > 0 and prices[0] > prices[-1]:  # if it's bids, we want descending order
        sorted_indices = sorted_indices[::-1]
    
    aggregated = np.zeros((levels, 2), dtype=np.float64)
    current_level = 0
    
    for i in range(0, min(len(prices), levels * chunk_size), chunk_size):
        chunk_indices = sorted_indices[i:i+chunk_size]
        if len(chunk_indices) > 0:
            price = prices[chunk_indices[0]]
            total_qty = np.sum(quantities[chunk_indices])
            aggregated[current_level] = [price, total_qty]
            current_level += 1
            if current_level == levels:
                break
    
    return aggregated[:current_level]

class OrderBook:
    def __init__(self, symbol, max_levels=1000):
        self.symbol = symbol
        self.max_levels = max_levels
        self.bids = {}
        self.asks = {}
        self.last_liquidation = None
        self.mark_price = None
        self.index_price = None
        self.last_update_id = 0
        self.previous_final_update_id = 0
        self.buffer_queue = asyncio.Queue()
        self.last_save_time = datetime.now(UTC)
        self.spot_data = {'vwap': 'NaN', 'buy_volume': Decimal('0'), 'sell_volume': Decimal('0')}
        self.futures_data = {'vwap': 'NaN', 'buy_volume': Decimal('0'), 'sell_volume': Decimal('0')}
        self.open_interest = None
        self.funding_rate = None
        self.last_snapshot_time = datetime.now(UTC)
        self.last_agg_trade_update = datetime.now(UTC)
        self.spot_buffer = []
        self.futures_buffer = []
        self.last_depth_update = datetime.now(UTC)
        self.local_file_path = f"orderbook_data/{symbol}_{{date}}.parquet"
        self.executor = ThreadPoolExecutor(max_workers=1)  
        self.depth_update_buffer = []
        self.last_processed_depth_update = datetime.now(UTC)
        self.parquet_writer = None
        self.parquet_schema = None
        self.current_file_path = None
        self.depth_queue = queue.Queue()
        self.depth_processor_thread = threading.Thread(target=self.depth_processor, daemon=True)
        self.depth_processor_thread.start()
        
        # New attributes for 30-second volume aggregation
        self.volume_window = 30  # 30 seconds
        self.last_volume_update = datetime.now(UTC)
        self.aggregated_volumes = {
            'spot': {'buy': Decimal('0'), 'sell': Decimal('0')},
            'futures': {'buy': Decimal('0'), 'sell': Decimal('0')}
        }
        # Add attributes for continuous volume aggregation
        self.continuous_volumes = {
            'spot': {'buy': Decimal('0'), 'sell': Decimal('0')},
            'futures': {'buy': Decimal('0'), 'sell': Decimal('0')}
        }
        
        self.initialize_parquet_writer()
            
    def depth_processor(self):
        while True:
            try:
                event = self.depth_queue.get(timeout=1)  # 1 second timeout
                self.process_depth_event_logic(event)
                self.depth_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in depth processor: {str(e)}")

    def initialize_parquet_writer(self):
        self.parquet_schema = pa.schema([
            ('tm', pa.int64()),
            ('b', pa.string()),
            ('a', pa.string()),
            ('snp', pa.bool_()),
            ('mk_p', pa.string()),
            ('idx_p', pa.string()),
            ('liq', pa.string()),
            ('s_buy_v', pa.string()),
            ('s_sell_v', pa.string()),
            ('s_vwap', pa.string()),
            ('f_buy_v', pa.string()),
            ('f_sell_v', pa.string()),
            ('f_vwap', pa.string()),
            ('oi', pa.string()),
            ('fr', pa.string())
        ])
    
        current_date = datetime.now(UTC).date()
        self.current_file_path = self.local_file_path.format(date=current_date.strftime('%Y-%m-%d'))

    async def get_snapshot(self):
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={self.symbol}&limit=1000"
        open_interest_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={self.symbol}"
        funding_rate_url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={self.symbol}"

        async with aiohttp.ClientSession() as session:
            tasks = [
                session.get(url),
                session.get(open_interest_url),
                session.get(funding_rate_url)
            ]
            responses = await asyncio.gather(*tasks)

            if all(response.status == 200 for response in responses):
                data = await responses[0].json()
                open_interest_data = await responses[1].json()
                funding_rate_data = await responses[2].json()

                self.bids = {Decimal(price): Decimal(qty) for price, qty in data['bids']}
                self.asks = {Decimal(price): Decimal(qty) for price, qty in data['asks']}
                self.last_update_id = data['lastUpdateId']
                self.previous_final_update_id = 0

                if open_interest_data:
                    self.open_interest = Decimal(open_interest_data['openInterest'])
                    
                if funding_rate_data:
                    self.funding_rate = Decimal(funding_rate_data['lastFundingRate'])

                self.spot_data.update({
                    'buy_volume': self.continuous_volumes['spot']['buy'],
                    'sell_volume': self.continuous_volumes['spot']['sell']
                })
                self.futures_data.update({
                    'buy_volume': self.continuous_volumes['futures']['buy'],
                    'sell_volume': self.continuous_volumes['futures']['sell']
                })

                # Calculate VWAP and last price
                self.calculate_vwap_and_last_price()

                # Reset continuous volumes after snapshot
                for market in self.continuous_volumes:
                    for direction in self.continuous_volumes[market]:
                        self.continuous_volumes[market][direction] = Decimal('0')

                self.last_snapshot_time = datetime.now(UTC)
                
                await self.add_to_buffer(is_snapshot=True)
            else:
                logger.warning(f"Failed to get snapshot or additional data. Status codes: {[response.status for response in responses]}")

                
    def calculate_vwap_and_last_price(self):
        current_time = datetime.now(UTC)
        window_start = self.last_snapshot_time
        window_end = current_time

        self.spot_data.update(self.aggregate_trade_data(self.spot_buffer, window_start, window_end))
        self.futures_data.update(self.aggregate_trade_data(self.futures_buffer, window_start, window_end))

        # Clear processed trades from buffers
        window_end_ms = int(window_end.timestamp() * 1000)
        self.spot_buffer = [trade for trade in self.spot_buffer if trade['timestamp'] > window_end_ms]
        self.futures_buffer = [trade for trade in self.futures_buffer if trade['timestamp'] > window_end_ms]

    def update_aggregated_volumes(self):
        self.spot_data.update({
            'buy_volume': self.aggregated_volumes['spot']['buy'],
            'sell_volume': self.aggregated_volumes['spot']['sell']
        })
        self.futures_data.update({
            'buy_volume': self.aggregated_volumes['futures']['buy'],
            'sell_volume': self.aggregated_volumes['futures']['sell']
        })
        # Reset aggregated volumes
        for market in self.aggregated_volumes:
            for direction in self.aggregated_volumes[market]:
                self.aggregated_volumes[market][direction] = Decimal('0')
                  
    def process_depth_event(self, event):
        self.depth_update_buffer.append(event)
        current_time = datetime.now(UTC)
        if current_time - self.last_processed_depth_update >= timedelta(milliseconds=500):
            self.process_depth_buffer()
            self.last_processed_depth_update = current_time

    def process_depth_buffer(self):
        for event in self.depth_update_buffer:
            if self.process_depth_event_logic(event):
                self.limit_order_book()
        self.depth_update_buffer.clear()

        # After processing the depth buffer, we add a non-snapshot update to the buffer
        asyncio.create_task(self.add_to_buffer(is_snapshot=False))

    def process_depth_event_logic(self, event):
        if event['u'] <= self.last_update_id:
            return False

        if self.previous_final_update_id == 0:
            if event['U'] <= self.last_update_id + 1 <= event['u']:
                self.previous_final_update_id = event['u']
            else:
                return False
        elif event['pu'] != self.previous_final_update_id:
            return False

        for bid in event['b']:
            price, qty = Decimal(bid[0]), Decimal(bid[1])
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty

        for ask in event['a']:
            price, qty = Decimal(ask[0]), Decimal(ask[1])
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty

        self.previous_final_update_id = event['u']
        self.limit_order_book()
        return True

    def limit_order_book(self):
        self.bids = dict(sorted(self.bids.items(), reverse=True)[:self.max_levels])
        self.asks = dict(sorted(self.asks.items())[:self.max_levels])

    def aggregate_order_book(self, levels=100):
        bids_prices = np.array([float(price) for price in self.bids.keys()])
        bids_quantities = np.array([float(qty) for qty in self.bids.values()])
        asks_prices = np.array([float(price) for price in self.asks.keys()])
        asks_quantities = np.array([float(qty) for qty in self.asks.values()])

        aggregated_bids = aggregate_order_book_numba(bids_prices, bids_quantities, levels, 10)
        aggregated_asks = aggregate_order_book_numba(asks_prices, asks_quantities, levels, 10)

        return (
            [(f"{price:.4f}", f"{qty:.2f}") for price, qty in aggregated_bids],
            [(f"{price:.4f}", f"{qty:.2f}") for price, qty in aggregated_asks]
        )

    def process_liquidation_event(self, event):
        self.last_liquidation = {
            's': event['o']['S'],
            'q': event['o']['q'],
            'p': event['o']['p'],
            't': event['E']
        }

    def process_mark_price_event(self, event):
        self.mark_price = event['p']
        self.index_price = event['i']
        # Assuming 'r' is the key for funding rate in the event
        if 'r' in event:
            # Convert to Decimal without rounding
            funding_rate = Decimal(event['r'])
            if funding_rate != Decimal('0'):
                self.funding_rate = funding_rate
            else:
                self.funding_rate = None

    def process_agg_trade(self, data, is_futures=False):
        qty = Decimal(data['q'])
        price = Decimal(data['p'])
        is_buyer_maker = data['m']
        timestamp = int(data['T'])

        market_type = 'futures' if is_futures else 'spot'
        if is_buyer_maker:
            self.continuous_volumes[market_type]['sell'] += qty
        else:
            self.continuous_volumes[market_type]['buy'] += qty

        # Keep the trade data in buffers for VWAP calculation
        trade_data = {
            'qty': qty,
            'price': price,
            'is_buyer_maker': is_buyer_maker,
            'timestamp': timestamp
        }

        if is_futures:
            self.futures_buffer.append(trade_data)
        else:
            self.spot_buffer.append(trade_data)

    def aggregate_trade_data(self, trade_buffer, window_start, window_end):
        window_start_ms = int(window_start.timestamp() * 1000)
        window_end_ms = int(window_end.timestamp() * 1000)

        filtered_trades = [trade for trade in trade_buffer if window_start_ms <= trade['timestamp'] < window_end_ms]

        if not filtered_trades:
            return {'vwap': 'NaN', 'last_price': 'NaN'}

        vwap_numerator = Decimal('0')
        vwap_denominator = Decimal('0')

        for trade in filtered_trades:
            qty = trade['qty']
            price = trade['price']
            
            vwap_numerator += price * qty
            vwap_denominator += qty

        vwap = str(round(vwap_numerator / vwap_denominator, 5)) if vwap_denominator > 0 else 'NaN'
        return {'vwap': vwap}
        
    async def add_to_buffer(self, is_snapshot):
        aggregated_bids, aggregated_asks = self.aggregate_order_book()
        timestamp = int(datetime.now(UTC).timestamp() * 1000)
        data = {
            'tm': timestamp,
            'b': aggregated_bids,
            'a': aggregated_asks,
            'snp': is_snapshot,
            'mk_p': str(self.mark_price) if self.mark_price is not None else 'NaN',
            'idx_p': str(self.index_price) if self.index_price is not None else 'NaN',
            'liq': ujson.dumps(self.last_liquidation) if self.last_liquidation else 'NaN',
        }

        if is_snapshot:
            data.update({
                's_buy_v': str(self.spot_data['buy_volume']),
                's_sell_v': str(self.spot_data['sell_volume']),
                'f_buy_v': str(self.futures_data['buy_volume']),
                'f_sell_v': str(self.futures_data['sell_volume']),
                'oi': str(self.open_interest) if self.open_interest is not None else 'NaN',
                's_vwap': self.spot_data['vwap'],
                'f_vwap': self.futures_data['vwap'],
                'fr': f"{self.funding_rate:.8f}" if self.funding_rate is not None else 'NaN',
            })
        else:
            data.update({
                's_buy_v': 'NaN',
                's_sell_v': 'NaN',
                'f_buy_v': 'NaN',
                'f_sell_v': 'NaN',
                'oi': 'NaN',
                's_vwap': 'NaN',
                'f_vwap': 'NaN',
                'fr': 'NaN',
            })

        await self.buffer_queue.put(data)
        self.last_liquidation = None
        
        current_time = datetime.now(UTC)
        if self.buffer_queue.qsize() >= BUFFER_SIZE_THRESHOLD or (current_time - self.last_save_time).total_seconds() >= BUFFER_FLUSH_INTERVAL:
            asyncio.create_task(self.flush_buffer())

    def save_to_parquet(self, data, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        if self.parquet_schema is None:
            self.initialize_parquet_writer()
        
        # Convert nested lists to JSON strings
        for item in data:
            item['b'] = ujson.dumps(item['b'])
            item['a'] = ujson.dumps(item['a'])
        
        # Convert data to pandas DataFrame
        df = pd.DataFrame(data)
        
        # Define column types
        column_types = {
            'tm': 'int64',
            'b': 'string',
            'a': 'string',
            'snp': 'bool',
            'mk_p': 'string',
            'idx_p': 'string',
            'liq': 'string',
            's_buy_v': 'string',
            's_sell_v': 'string',
            's_vwap': 'string',
            'f_buy_v': 'string',
            'f_sell_v': 'string',
            'f_vwap': 'string',
            'oi': 'string',
            'fr': 'string'
        }
        
        # Apply column types
        for col, dtype in column_types.items():
            df[col] = df[col].astype(dtype)
        
        # Convert string columns to categorical for better compression
        for col in df.select_dtypes(include=['object']):
            df[col] = df[col].astype('category')
        
        # Define compression options
        compression_opts = {
            'compression': 'ZSTD',
            'compression_level': 9,
            'write_index': False,
            'object_encoding': 'json'
        }
        
        # Check if file exists
        if os.path.exists(file_path):
            # If file exists, append to it
            pf = ParquetFile(file_path)
            write(file_path, df, 
                compression=compression_opts['compression'],
                write_index=compression_opts['write_index'],
                object_encoding=compression_opts['object_encoding'],
                row_group_offsets=1048576,  # 1MB row groups
                append=True,
                file_scheme=pf.file_scheme)
        else:
            # If file doesn't exist, create a new one
            write(file_path, df, 
                compression=compression_opts['compression'],
                write_index=compression_opts['write_index'],
                object_encoding=compression_opts['object_encoding'],
                row_group_offsets=1048576)  # 1MB row groups
        
    async def flush_buffer(self):
        if self.buffer_queue.empty():
            return

        data = []
        while not self.buffer_queue.empty():
            try:
                data.append(self.buffer_queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        self.last_save_time = datetime.now(UTC)
        current_date = self.last_save_time.date()
        file_path = self.local_file_path.format(date=current_date.strftime('%Y-%m-%d'))

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self.executor, self.save_to_parquet, data, file_path)
        except Exception as e:
            logger.error(f"Error saving to parquet for {self.symbol}: {str(e)}")
            # If there's an error, put the data back in the queue
            for item in data:
                await self.buffer_queue.put(item)

class MultiSymbolOrderBookManager:
    def __init__(self, symbols):
        self.symbols = symbols
        self.orderbooks = {symbol: OrderBook(symbol) for symbol in symbols}
        self.futures_websocket_url = f"wss://fstream.binance.com/stream?streams={'/'.join([f'{symbol.lower()}@depth@500ms/{symbol.lower()}@forceOrder/{symbol.lower()}@markPrice@1s/{symbol.lower()}@aggTrade' for symbol in symbols])}"
        self.spot_websocket_url = f"wss://stream.binance.com:9443/ws/{'/'.join([f'{symbol.lower()}@aggTrade' for symbol in symbols])}"
        
        # Use multiprocessing.cpu_count() to get the number of CPUs
        cpu_count = multiprocessing.cpu_count()
        self.executor = ThreadPoolExecutor(max_workers=cpu_count)
        
        self.depth_update_queue = asyncio.Queue()
        self.agg_trade_queue = asyncio.Queue()

    async def futures_message_handler(self, message):
        event = ujson.loads(message)
        stream = event['stream']
        data = event['data']
        symbol = stream.split('@')[0].upper()
        
        if symbol in self.orderbooks:
            if 'depth' in stream:
                self.orderbooks[symbol].process_depth_event(data)
            elif 'forceOrder' in stream:
                self.orderbooks[symbol].process_liquidation_event(data)
            elif 'markPrice' in stream:
                self.orderbooks[symbol].process_mark_price_event(data)
            elif 'aggTrade' in stream:
                await self.agg_trade_queue.put((symbol, data, True))

    async def spot_message_handler(self, message):
        data = ujson.loads(message)
        symbol = data['s']
        if symbol in self.orderbooks:
            await self.agg_trade_queue.put((symbol, data, False))

    async def process_depth_updates(self):
        while True:
            batch = []
            try:
                for _ in range(100):  # Process up to 100 updates at once
                    symbol, data = await asyncio.wait_for(self.depth_update_queue.get(), timeout=0.1)
                    batch.append((symbol, data))
            except asyncio.TimeoutError:
                pass
            
            for symbol, data in batch:
                self.orderbooks[symbol].process_depth_event(data)
                self.depth_update_queue.task_done()
            
            if not batch:
                await asyncio.sleep(0.01)  # Avoid busy waiting

    async def process_agg_trades(self):
        while True:
            batch = []
            try:
                for _ in range(100):  # Process up to 100 trades at once
                    symbol, data, is_futures = await asyncio.wait_for(self.agg_trade_queue.get(), timeout=0.1)
                    batch.append((symbol, data, is_futures))
            except asyncio.TimeoutError:
                pass
            
            for symbol, data, is_futures in batch:
                self.orderbooks[symbol].process_agg_trade(data, is_futures)
                self.agg_trade_queue.task_done()
            
            if not batch:
                await asyncio.sleep(0.01)  # Avoid busy waiting

    async def get_periodic_snapshots(self):
        while True:
            for orderbook in self.orderbooks.values():
                await orderbook.get_snapshot()
            await asyncio.sleep(SNAPSHOT_INTERVAL)

    async def save_buffers_periodically(self):
        while True:
            await asyncio.sleep(BUFFER_FLUSH_INTERVAL)
            tasks = []
            for orderbook in self.orderbooks.values():
                if not orderbook.buffer_queue.empty():
                    tasks.append(asyncio.create_task(orderbook.flush_buffer()))
            if tasks:
                await asyncio.gather(*tasks)
    
    async def save_buffer(self, symbol, orderbook):
        if orderbook.buffer_queue.empty():
            return

        data = []
        while not orderbook.buffer_queue.empty():
            try:
                data.append(orderbook.buffer_queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        current_date = datetime.now(UTC).date()
        file_path = orderbook.local_file_path.format(date=current_date.strftime('%Y-%m-%d'))

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self.executor, orderbook.save_to_parquet, data, file_path)
        except Exception as e:
            logger.error(f"Error saving to parquet for {symbol}: {str(e)}")
            for item in data:
                await orderbook.buffer_queue.put(item)

    async def run(self):
        futures_ws = WebSocketManager(self.futures_websocket_url, "Futures", self.futures_message_handler)
        spot_ws = WebSocketManager(self.spot_websocket_url, "Spot", self.spot_message_handler)

        snapshot_task = asyncio.create_task(self.get_periodic_snapshots())
        futures_stream_task = asyncio.create_task(futures_ws.run())
        spot_stream_task = asyncio.create_task(spot_ws.run())
        buffer_save_task = asyncio.create_task(self.save_buffers_periodically())
        
        # Remove the depth processing tasks as we're now using threads
        agg_trade_processing_tasks = [asyncio.create_task(self.process_agg_trades()) for _ in range(2)]

        try:
            await asyncio.gather(
                snapshot_task, futures_stream_task, spot_stream_task, 
                buffer_save_task, *agg_trade_processing_tasks
            )
        except Exception as e:
            logger.error(f"Main loop error: {traceback.format_exc()}")
        finally:
            for task in [snapshot_task, futures_stream_task, spot_stream_task, 
                         buffer_save_task, *agg_trade_processing_tasks]:
                task.cancel()
            await asyncio.gather(*[task for task in [snapshot_task, futures_stream_task, spot_stream_task, 
                                                     buffer_save_task, *agg_trade_processing_tasks] 
                                   if not task.done()], return_exceptions=True)
            for orderbook in self.orderbooks.values():
                if not orderbook.buffer_queue.empty():
                    await orderbook.flush_buffer()
            
class WebSocketManager:
    def __init__(self, url, name, message_handler):
        self.url = url
        self.name = name
        self.ws = None
        self.last_pong = None
        self.pong_timeout = timedelta(seconds=30)
        self.ping_interval = timedelta(seconds=15)
        self.message_handler = message_handler
        self.reconnect_delay = 5
        self.max_reconnect_delay = 300  # 5 minutes

    async def connect(self):
        while True:
            try:
                self.ws = await websockets.connect(self.url)
                self.last_pong = datetime.now(UTC)
                logger.info(f"{self.name} WebSocket connected.")
                return
            except Exception as e:
                logger.error(f"Failed to connect to {self.name} WebSocket: {str(e)}")
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)

    async def send_ping(self):
        while True:
            try:
                await asyncio.sleep(self.ping_interval.total_seconds())
                if self.ws.open:
                    pong_waiter = await self.ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self.pong_timeout.total_seconds())
                    self.last_pong = datetime.now(UTC)
                else:
                    break
            except asyncio.TimeoutError:
                logger.warning(f"Ping timeout for {self.name} WebSocket.")
                await self.ws.close()
                break
            except Exception as e:
                logger.error(f"Error in send_ping for {self.name} WebSocket: {str(e)}")
                break

    async def receive_messages(self):
        try:
            while True:
                try:
                    message = await asyncio.wait_for(self.ws.recv(), timeout=self.pong_timeout.total_seconds())
                    await self.message_handler(message)
                except asyncio.TimeoutError:
                    if datetime.now(UTC) - self.last_pong > self.pong_timeout:
                        logger.warning(f"No message received from {self.name} WebSocket for {self.pong_timeout}.")
                        await self.ws.close()
                        break
        except websockets.exceptions.ConnectionClosed:
            logger.warning(f"{self.name} WebSocket connection closed.")
        except Exception as e:
            logger.error(f"Error in receive_messages for {self.name} WebSocket: {str(e)}")
        finally:
            if self.ws.open:
                await self.ws.close()
                
    async def run(self):
        while True:
            try:
                await self.connect()
                ping_task = asyncio.create_task(self.send_ping())
                receive_task = asyncio.create_task(self.receive_messages())
                await asyncio.gather(ping_task, receive_task)
            except Exception as e:
                logger.error(f"Error in {self.name} WebSocket run: {str(e)}")
            finally:
                self.reconnect_delay = 5  # Reset reconnect delay
            logger.info(f"Reconnecting {self.name} WebSocket in {self.reconnect_delay} seconds...")
            await asyncio.sleep(self.reconnect_delay)

async def upload_to_r2(local_file_path: str, symbol: str, file_name: str):
    s3 = boto3.client('s3',
                      endpoint_url=R2_ENDPOINT_URL,
                      aws_access_key_id=R2_ACCESS_KEY_ID,
                      aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                      config=Config(signature_version='s3v4'))

    try:
        with open(local_file_path, 'rb') as f_in:
            data = f_in.read()
        
        r2_object_key = f"{symbol}-M/{file_name}"
        
        s3.put_object(Bucket=R2_BUCKET_NAME, Key=r2_object_key, Body=data)
        logger.info(f"Successfully uploaded compressed {local_file_path} to R2 storage: {r2_object_key}")
        return True
    except Exception as e:
        logger.error(f"Failed to upload {local_file_path} to R2 storage: {str(e)}")
        return False

async def shutdown(signal, loop):
    print(f"Received exit signal {signal.name}...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def upload_previous_day_files(manager):
    try:
        current_time = datetime.now(UTC)
        upload_time = time(0, 5, tzinfo=UTC)
        
        if current_time.time().replace(tzinfo=UTC) < upload_time:
            # If it's before 00:05 UTC, wait until 00:05
            wait_time = (datetime.combine(current_time.date(), upload_time, tzinfo=UTC) - current_time).total_seconds()
            await asyncio.sleep(wait_time)
        
        previous_day = (current_time - timedelta(days=1)).date()
        previous_day_str = previous_day.strftime('%Y-%m-%d')
        
        for symbol in manager.symbols:
            local_file_path = manager.orderbooks[symbol].local_file_path.format(date=previous_day_str)
            if os.path.exists(local_file_path):
                file_name = f"{previous_day_str}.parquet"
                success = await upload_to_r2(local_file_path, symbol, file_name)
                if success:
                    logger.info(f"Successfully uploaded {symbol} file for {previous_day_str} to R2")
                else:
                    logger.error(f"Failed to upload {symbol} file for {previous_day_str} to R2")
        
        # Schedule the next upload for the following day
        next_upload_time = datetime.combine(current_time.date() + timedelta(days=1), upload_time, tzinfo=UTC)
        await asyncio.sleep((next_upload_time - datetime.now(UTC)).total_seconds())
        asyncio.create_task(upload_previous_day_files(manager))
    except Exception as e:
        logger.error(f"Error in upload_previous_day_files: {str(e)}")
        # Reschedule the task after a delay in case of an error
        await asyncio.sleep(300)  # Wait for 5 minutes before trying again
        asyncio.create_task(upload_previous_day_files(manager))

async def main():
    manager = MultiSymbolOrderBookManager(SYMBOLS)
    upload_task = asyncio.create_task(upload_previous_day_files(manager))
    await asyncio.gather(manager.run(), upload_task)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop)))
    
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()