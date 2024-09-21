# Binance Futures Orderbook Scraper

<div align="center">

![Binance Futures Orderbook Scraper Logo](https://img.shields.io/badge/Binance-Orderbook%20Scraper-yellow?style=for-the-badge&logo=binance&logoColor=white)

[![Python Version](https://img.shields.io/badge/python-3.7%2B-blue?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/mpanac)

</div>

## 📚 Table of Contents

- [🌟 About the Project](#-about-the-project)
- [✨ Features](#-features)
- [🚀 Getting Started](#-getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [🖥️ Usage](#️-usage)
- [⚙️ Configuration](#️-configuration)
- [📊 Data Structure](#-data-structure)
- [📈 VWAP Calculation](#-vwap-calculation)
- [💾 Data Storage and Continuity](#-data-storage-and-continuity)
- [☁️ Cloudflare R2 Storage](#️-cloudflare-r2-storage)
- [🖥️ VPS Deployment](#️-vps-deployment)
- [📄 License](#-license)
- [📞 Contact](#-contact)
- [🙏 Acknowledgements](#-acknowledgements)

## 🌟 About the Project

The Binance Futures Orderbook Scraper is a high-performance Python script designed to collect and process real-time orderbook data from Binance Futures markets. It uses WebSocket connections to stream live data, processes it efficiently, and stores it in Parquet format for further analysis.

This script is ideal for researchers, traders, and data scientists who need access to high-quality, real-time market data from Binance Futures.

## ✨ Features

- Real-time orderbook data collection for multiple symbols
- Efficient data processing using multithreading and asyncio
- Data aggregation and VWAP calculation
- Periodic snapshots of the full orderbook
- Automatic reconnection and error handling
- Data storage in Parquet format for efficient querying and analysis
- Data appended to Parquet files every 1 minute inside of /orderbook_data folder
- Automatic uploading of data to Cloudflare R2 storage
- Configurable symbol list for data collection
- Continuous data collection with restart capability

## 🚀 Getting Started

### Prerequisites

- Python 3.7+
- pip (Python package installer)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/mpanac/binance-futures-orderbook-scraper.git
   ```

2. Navigate to the project directory:
   ```
   cd binance-futures-orderbook-scraper
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

4. Set up your environment variables (see [Configuration](#️-configuration) section)

## 🖥️ Usage

To start the scraping, run:

```
python scrape_data.py
```

The script will start collecting data for the configured symbols and store it in Parquet files. It will also automatically upload the previous day's data to Cloudflare R2 storage at 00:05 UTC daily.

> **Note:** It might take a few seconds until you see the INFO about Spot and Futures websocket connection. If everything runs fine, you should see .parquet files appear in the /orderbook_data folder in around a minute from start.

## ⚙️ Configuration

1. Rename the `.env.example` file to `.env` in the project root directory:

```
mv .env.example .env
```

2. Open the `.env` file and replace the placeholder values with your actual Cloudflare R2 credentials:

```
R2_ENDPOINT_URL=https://your-account.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_NAME=your_bucket_name
```

3. Modify the `SYMBOLS` list in the `scrape_data.py` script to add or remove pairs you want to scrape:

```python
SYMBOLS = ["BTCUSDT", "ETHUSDT", "TONUSDT", "BNBUSDT", "SOLUSDT", "SUIUSDT"]
```

Add or remove symbol pairs as needed.

## 📊 Data Structure

The collected data is stored in Parquet files with the following schema:

- `tm`: Timestamp (int64) - every ~500ms
- `b`: Aggregated bids (string, JSON-encoded) - every ~500ms
- `a`: Aggregated asks (string, JSON-encoded) - every ~500ms
- `snp`: Is snapshot (boolean) - Set to True every 30s, False otherwise
- `mk_p`: Mark price (string) - every 1s
- `idx_p`: Index price (string) - every 1s
- `liq`: Last liquidation (string, JSON-encoded) - BUY/SELL, Price, Quantity, Timestamp
- `s_buy_v`: Spot buy volume (string) - every 30s
- `s_sell_v`: Spot sell volume (string) - every 30s
- `s_vwap`: Spot VWAP (string) - every 30s
- `f_buy_v`: Futures buy volume (string) - every 30s
- `f_sell_v`: Futures sell volume (string) - every 30s
- `f_vwap`: Futures VWAP (string) - every 30s
- `oi`: Open interest (string) - every 30s
- `fr`: Funding rate (string) - every 30s

## 📈 VWAP Calculation

The Volume Weighted Average Price (VWAP) is calculated separately for spot and futures markets. The calculation is performed over a 30-second window and updated with each snapshot. Here's how it's done:

1. For each trade in the 30-second window:
   - Multiply the trade price by the trade quantity
   - Sum these values (price * quantity) for all trades
   - Sum the quantities of all trades

2. VWAP is then calculated as:
   ```
   VWAP = Sum(price * quantity) / Sum(quantity)
   ```

This calculation provides a volume-weighted average price that reflects the trading activity over the past 30 seconds, giving a more accurate representation of the market price than a simple average.

## 💾 Data Storage and Continuity

The script is designed to handle interruptions gracefully. In case of disconnection or script restart:

- The script will find existing Parquet files and continue appending data without overwriting already scraped information.
- This ensures data continuity and prevents data loss during unexpected interruptions.

## ☁️ Cloudflare R2 Storage

We've chosen Cloudflare R2 for data storage due to its cost-effectiveness and flexibility:

- Zero egress fees, making it extremely economical for frequent data uploads and downloads.
- Pay only for the storage space you use, keeping costs minimal.
- The script handles structured storing in R2, maintaining a well-organized data hierarchy for easy navigation and retrieval.

## 🖥️ VPS Deployment

For 24/7 operation, we recommend running the script on a VPS:

- The script is optimized for CPU usage and stability, tested extensively on VPS environments.
- Recommended: Use supervisorctl for managing the script as a background process.
- A budget-friendly option is Hetzner's shared CPU VPS, costing around $5 per month.
- For larger-scale operations (scraping many pairs), monitor CPU usage as you may need a more powerful VPS.

Example supervisorctl configuration:

```ini
[program:binance_scraper]
command=/path/to/python /path/to/scrape_data.py
directory=/path/to/script/directory
user=your_username
autostart=true
autorestart=true
stderr_logfile=/var/log/binance_scraper.err.log
stdout_logfile=/var/log/binance_scraper.out.log
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Contact

Project Link: [https://github.com/mpanac/binance-futures-orderbook-scraper](https://github.com/mpanac/binance-futures-orderbook-scraper)

## 🙏 Acknowledgements

- [Binance](https://www.binance.com/)
- [WebSocket-client](https://developers.binance.com/docs/derivatives/Introduction)
- [pandas](https://pandas.pydata.org/)
- [pyarrow](https://arrow.apache.org/docs/python/)
- [Cloudflare R2](https://www.cloudflare.com/products/r2/)

## ☕ Support the Project

If you find this project helpful, consider buying me a coffee!

<div align="center">

[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-FFDD00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/mpanac)

</div>
