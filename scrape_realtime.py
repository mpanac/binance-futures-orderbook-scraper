# Necessary imports
import asyncio
from src.realtime_orderbook import main as realtime_orderbook

# Example usage of realtime_orderbook
async def run_realtime_orderbook(symbol):
    await realtime_orderbook(symbol)

# !! Pass BTCUSDT, ETHUSDT, BNBUSDT or SOLUSDT ( for more you have to adjust the script as noted in README ) !!   
if __name__ == "__main__":
    asyncio.run(run_realtime_orderbook('ETHUSDT'))