# Binance Futures Orderbook Scraper

<div align="center">

![Binance Futures Orderbook Scraper Logo](https://img.shields.io/badge/Binance-Orderbook%20Scraper-yellow?style=for-the-badge&logo=binance&logoColor=white)

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/mpanac)

</div>

A powerful tool for scraping and analyzing orderbook data from Binance Futures, featuring real-time updates and visualization capabilities.

## 📚 Table of Contents

- [Features](#-features)
- [Installation](#-installation)
- [Usage](#-usage)
- [Examples](#-examples)
- [Data Format](#-data-format)
- [Data Testing](#-data-testing)
- [Contributing](#-contributing)
- [Support the Project](#-support-the-project)
- [License](#-license)

## 🚀 Features

- Scrape orderbook data from Binance Futures at 1-second intervals (with maximal allowed depth of 1000 levels aggregated into 100)
- Save orderbook data in Parquet format:
  - Every 60 seconds (`scrape_orderbook.py`)
  - Every 1 second for near real-time updates (`realtime_orderbook.py`)
  - RestAPI snapshot taken every 30 seconds to capture maximum possible depth of 1000 levels. (is_snapshot = True)
  - Websocket stream received every 1second updating the local orderbook. (is_snapshot = False)
- Real-time orderbook updates with immediate data availability
- Real-time plotting of orderbook data
- Capable or running 24/7 on VPS with low CPU and Memory usage (tested)
- Configurable data collection intervals
- CloudFront R2 storage integration
- Support for BTCUSDT, ETHUSDT, SOLUSDT, and BNBUSDT trading pairs
- Aggregation of 1000 most granular data points into 100 levels

## ⚠️ Important Note

This script is optimized for BTCUSDT, ETHUSDT, SOLUSDT, and BNBUSDT pairs, using 1000 levels aggregated into 100 levels. For other coins, adjust the script by adding the coin and appropriate granularity (10x larger than Binance's smallest granularity).

Data is stored in .parquet files. At 00:00 UTC daily, a new file with the correct date prefix is created, and the old file is sent to CloudFlare R2 storage (or your chosen alternative).

## 🛠 Installation

```bash
git clone https://github.com/mpanac/binance-orderbook-scraper.git
cd binance-orderbook-scraper
pip install -r requirements.txt
```

## 📊 Usage

### Scraping Orderbook Data

To scrape orderbook data (appended to .parquet file every 60-seconds):

```python
import asyncio
from src.scrape_orderbook import main as scrape_orderbook

async def run_scrape_orderbook(symbol):
    await scrape_orderbook(symbol)

if __name__ == "__main__":
    asyncio.run(run_scrape_orderbook('BTCUSDT'))
```

### Real-time Orderbook Data

To collect real-time orderbook data (appended to .parquet file every 1-second):

```python
import asyncio
from src.realtime_orderbook import main as realtime_orderbook

async def run_realtime_orderbook(symbol):
    await realtime_orderbook(symbol)

if __name__ == "__main__":
    asyncio.run(run_realtime_orderbook('ETHUSDT'))
```

Data is saved to `orderbook_data/SYMBOL_YYYY-MM-DD.parquet`.

### Real-time Visualization

For real-time visualization:

1. Open `src/realtime_plot.ipynb`
2. Adjust the interval (e.g. 10s, 1min, 30min, etc ... ) and symbol (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT) as needed
3. Ensure the correct .parquet file path is specified
4. Run the notebook

## 📁 Examples

The `/examples` folder contains reference files to help you get started:

1. `example_usage.py`: Demonstrates how to use the `scrape_orderbook` and `realtime_orderbook` functions.
2. `example_data_BTCUSDT-M.parquet`: A sample .parquet file showing the structure and format of the collected orderbook data.

To run the examples:

```bash
python examples/example_usage.py
```

## 📈 Data Format

The collected data is stored in .parquet files with the following structure:

| Column         | Type                | Description                                         |
|----------------|---------------------|-----------------------------------------------------|
| timestamp      | datetime64[ns, UTC] | Timestamp of the orderbook update                   |
| bids           | object              | JSON array of bid prices and quantities (100 levels)|
| asks           | object              | JSON array of ask prices and quantities (100 levels)|
| is_snapshot    | bool                | True if this is a snapshot (RestApi), False for updates (Websocket) |
| last_update_id | int64               | Last update ID from Binance                         |
| buy_volume     | float64             | Total buy volume                                    |
| sell_volume    | float64             | Total sell volume                                   |
| vwap           | float64             | Volume-weighted average price                       |
| total_trades   | int64               | Total number of trades                              |

Example of a single row (representing a 1s websocket update):

```json
{
  "timestamp": 1722426734175,
  "bids": [[66059, 32.171], [66056, 0.227], [66053, 0.002], ...],
  "asks": [[66060, 0.063], [66062, 0.002], [66064, 0.032], ...],
  "is_snapshot": false,
  "last_update_id": 5053696068387,
  "buy_volume": 0.012,
  "sell_volume": 0,
  "vwap": 66057.1,
  "total_trades": 1
}
```

Note: The `bids` and `asks` arrays contain up to 100 levels each.

You can explore the data structure by examining the `example_data_BTCUSDT-M.parquet` file in the `/examples` folder.

## ☁️ Configuring R2 CloudFront Storage

To save data to R2 CloudFront storage:

1. Open `scrape_orderbook.py` and `realtime_orderbook.py`
2. Locate the R2 CloudFront configuration section
3. Replace the placeholder credentials with your own:

```python
R2_ENDPOINT_URL = "https://<accountid>.r2.cloudflarestorage.com"
R2_ACCESS_KEY_ID = "<access_key_id>"
R2_SECRET_ACCESS_KEY = "<access_key_secret>"
R2_BUCKET_NAME = "<your_bucket_name>"
```

Make sure to keep your credentials secure and never share them publicly.

## 🔄 Adding New Symbols

To add support for new trading pairs:

1. Open `scrape_orderbook.py` or `realtime_orderbook.py`
2. Locate the `main` function
3. Add a new entry to the `granularity` dictionary:

```python
async def main(symbol: str):
    granularity = {
        "BTCUSDT": 1,
        "ETHUSDT": 0.1,
        "BNBUSDT": 0.1,
        "SOLUSDT": 0.01,
        "NEWCOINUSDT": 0.1  # Add your new symbol here
    }.get(symbol, 0.01)  # Default to 0.01 if symbol not in the list
```

Note: The granularity must be 10x larger than the smallest granularity provided by Binance for that specific coin. This ensures accurate calculations.

## 🧪 Data Testing

To test your data:

1. Open `test/test_data.ipynb`
2. Update the .parquet file path
3. Run the notebook to view results

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## ☕ Support the Project

If you find this project helpful, consider buying me a coffee!

<div align="center">

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/mpanac)

</div>

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.