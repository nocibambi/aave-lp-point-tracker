import logging

import requests

from aave_point_tracker.utils.utils import save_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def fetch_aave_addresses() -> None:
    aave_addresses_url = (
        "https://raw.githubusercontent.com/bgd-labs/aave-address-book/main/safe.csv"
    )
    response = requests.get(aave_addresses_url)
    aave_addresses = [record.split(",")[0] for record in response.text.split("\n")[1:]]
    save_data(data_to_save=aave_addresses, filename="aave_addresses", data_layer="raw")
