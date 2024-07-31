from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="binance-orderbook-scraper",
    version="0.1.0",
    author="mpanac",
    author_email="ofalgos@gmail.com",
    description="A powerful tool for scraping and analyzing Binance Futures orderbook data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mpanac/binance-orderbook-scraper",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
    install_requires=[
        "aiohttp",
        "asyncio",
        "boto3",
        "ipython",
        "pandas",
        "pyarrow",
        "plotly",
        "numpy",
        "websockets",
        "ujson",
        "sortedcontainers",
        "matplotlib",
    ],
    entry_points={
        "console_scripts": [
            "scrape-orderbook=src.scrape_orderbook:main",
            "realtime-orderbook=src.realtime_orderbook:main",
        ],
    },
    project_urls={
        "Bug Tracker": "https://github.com/mpanac/binance-orderbook-scraper/issues",
        "Documentation": "https://github.com/mpanac/binance-orderbook-scraper/blob/main/README.md",
        "Source Code": "https://github.com/mpanac/binance-orderbook-scraper",
    },
)