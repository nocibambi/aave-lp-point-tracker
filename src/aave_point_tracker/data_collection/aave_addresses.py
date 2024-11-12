import requests
import logging
from aave_point_tracker.utils.utils import save_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def _fetch_aave_addresses():
    aave_addresses_url = (
        "https://raw.githubusercontent.com/bgd-labs/aave-address-book/main/safe.csv"
    )
    response = requests.get(aave_addresses_url)
    return [record.split(",")[0] for record in response.text.split("\n")[1:]]


aave_addresses = _fetch_aave_addresses()
save_data(data_to_save=aave_addresses, filename="aave_addresses", data_layer="raw")