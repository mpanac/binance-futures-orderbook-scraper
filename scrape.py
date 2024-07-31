# Necessary imports
import asyncio
from src.scrape_orderbook import main as scrape_orderbook

# Example usage of scrape_orderbook
async def run_scrape_orderbook(symbol):
    await scrape_orderbook(symbol)

# !! Pass BTCUSDT, ETHUSDT, BNBUSDT or SOLUSDT ( for more you have to adjust the script as noted in README ) !!
if __name__ == "__main__":
    asyncio.run(run_scrape_orderbook('BTCUSDT'))