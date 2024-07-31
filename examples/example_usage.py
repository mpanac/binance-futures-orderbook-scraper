
# Necessary imports
import asyncio
from src.scrape_orderbook import main as scrape_orderbook
from src.realtime_orderbook import main as realtime_orderbook

# Example usage of scrape_orderbook
async def run_scrape_orderbook(symbol):
    await scrape_orderbook(symbol)

# !! Pass BTCUSDT, ETHUSDT, BNBUSDT or SOLUSDT ( for more you have to adjust the script as noted in README ) !!
if __name__ == "__main__":
    asyncio.run(run_scrape_orderbook('BTCUSDT'))
    
    

# Example usage of realtime_orderbook
async def run_realtime_orderbook(symbol):
    await realtime_orderbook(symbol)

# !! Pass BTCUSDT, ETHUSDT, BNBUSDT or SOLUSDT ( for more you have to adjust the script as noted in README ) !!   
if __name__ == "__main__":
    asyncio.run(run_realtime_orderbook('ETHUSDT'))


# Example usage of realtime_plot - Go to src/realtime_plot.ipynb, change to your desired interval in minutes (e.g. - 5min) and also your symbol and then run it. (make sure you point to your .parquet file correctly)