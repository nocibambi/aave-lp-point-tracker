import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from dotenv import load_dotenv

from utils import date_str_to_posix, save_data

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


baseurl = "https://api.coingecko.com/api/v3"
api_key = os.environ["COINGECKO_API_KEY"]
with open(f"{os.environ['DATA_PATH']}/reserve_assets.json") as f:
    reserve_assets = json.load(f)


network = "ethereum"
headers = {
    "accept": "application/json",
    "x-cg-demo-api-key": f"{api_key}",
}

params: dict[str, str | float] = {
    "vs_currency": "usd",
    "from": date_str_to_posix("2024-09-01", buffer="early"),
    # Coingecko requires a pro plan to set the interval
    # but returns daily data points if the date range is greater than 90 days
    "to": date_str_to_posix(
        (datetime.strptime("2024-09-01", "%Y-%m-%d") + timedelta(days=91)).strftime(
            "%Y-%m-%d"
        ),
        buffer="late",
    ),
}


reserves_asset_prices = {}

for reserve in reserve_assets:
    logger.debug(f"Fetching prices for {reserve['symbol']}.")

    contract_address = reserve["underlyingAsset"]
    url = f"{baseurl}/coins/{network}/contract/{contract_address}/market_chart/range"

    delay = 1
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            break
        time.sleep(delay)
        delay *= 2
        logger.debug(f"Delaying for {delay} second...")

    prices = [
        [
            datetime.fromtimestamp(price[0] / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            price[1],
        ]
        for price in response.json()["prices"]
    ]

    reserves_asset_prices[reserve["symbol"]] = prices

save_data(reserves_asset_prices, "reserves_asset_prices")


pd.DataFrame.from_dict(reserves_asset_prices)
pd.DataFrame.from_records(
    reserves_asset_prices["WETH"], columns=["date", "price"]
).set_index("date")

{asset: len(reserves_asset_prices[asset]) for asset in reserves_asset_prices}

len(response.json()["prices"])

[
    [
        datetime.fromtimestamp(price[0] / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        price[1],
    ]
    for price in prices
]
