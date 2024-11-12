import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

from aave_point_tracker.utils.utils import (
    datetime_to_posix,
    save_data,
    date_str_to_datetime,
    load_configs,
)

load_dotenv()
configs = load_configs()

FIRST_DATE: str = configs["first_date"]
LAST_DATE: str = configs["last_date"]

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
    "from": datetime_to_posix(
        date_str_to_datetime(FIRST_DATE).astimezone(timezone.utc), buffer="early"
    ),
    "to": datetime_to_posix(
        date_str_to_datetime(LAST_DATE).astimezone(timezone.utc) + timedelta(days=1),
        buffer="late",
    ),
}

reserve_asset_prices = {}
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
        logger.debug(f"Delaying for {delay} seconds...")

    prices = [
        [
            datetime.fromtimestamp(price[0] / 1000, tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            price[1],
        ]
        for price in response.json()["prices"]
    ]
    reserve_asset_prices[contract_address] = prices

save_data(reserve_asset_prices, "reserve_asset_prices", data_layer="raw")
