from datetime import datetime, timezone

from web3 import Web3
import logging
from aave_point_tracker.utils.utils import load_data, save_data

web3_client = Web3()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

########## LOAD DATA ##########

starting_balances: list[dict] = load_data("starting_balances", data_layer="raw")
reserve_assets: list[dict] = load_data("reserve_assets", data_layer="raw")
reserve_asset_prices: dict[str, list[dict]] = load_data(
    "reserve_asset_prices", data_layer="raw"
)
reserve_liquidity_index_histories: dict[str, list[dict]] = load_data(
    "reserve_liquidity_index_histories", data_layer="raw"
)
atoken_balance_histories: list[dict] = load_data(
    "atoken_balance_histories", data_layer="raw"
)
aave_addresses: list[str] = load_data("aave_addresses", data_layer="raw")

########## PREPARE DATA ##########

user_starting_balances: dict[str, list] = {}
for starting_balance in starting_balances:
    user_id: str = web3_client.to_checksum_address(starting_balance["id"])
    if user_id in aave_addresses:
        continue

    for reserve in starting_balance["reserves"]:
        if not reserve["aTokenBalanceHistory"]:
            continue
        scaled_atoken_balance = reserve["aTokenBalanceHistory"][0][
            "scaledATokenBalance"
        ]
        if scaled_atoken_balance == "0":
            continue
        if user_id not in user_starting_balances:
            user_starting_balances[user_id] = []
        token_id: str = web3_client.to_checksum_address(
            reserve["reserve"]["underlyingAsset"]
        )
        logger.debug(
            len(user_starting_balances), user_id, token_id, scaled_atoken_balance
        )
        user_starting_balances[user_id].append([token_id, scaled_atoken_balance])
save_data(user_starting_balances, "user_starting_balances", data_layer="prepared")

liquidity_indexes: dict[str, list] = {}
for asset in reserve_liquidity_index_histories:
    asset_checksummed = web3_client.to_checksum_address(asset)
    liquidity_indexes[asset_checksummed] = []
    for record in reserve_liquidity_index_histories[asset]:
        liquidity_indexes[asset_checksummed].append(
            [
                record["timestamp"],
                record["liquidityIndex"],
            ]
        )
save_data(liquidity_indexes, "liquidity_indexes", data_layer="prepared")

asset_decimals: dict[str, int] = {
    web3_client.to_checksum_address(asset["underlyingAsset"]): asset["decimals"]
    for asset in reserve_assets
}
save_data(asset_decimals, "asset_decimals", data_layer="prepared")

asset_prices = {
    web3_client.to_checksum_address(asset): prices
    for asset, prices in reserve_asset_prices.items()
}
save_data(asset_prices, "asset_prices", data_layer="prepared")

user_atoken_balance_histories: dict[str, list] = {}
for history_item in atoken_balance_histories:
    user_id = web3_client.to_checksum_address(history_item["userReserve"]["user"]["id"])
    if user_id in aave_addresses:
        continue
    if user_id not in user_atoken_balance_histories:
        user_atoken_balance_histories[user_id] = []
    user_atoken_balance_histories[user_id].append(
        [
            history_item["timestamp"],
            web3_client.to_checksum_address(
                history_item["userReserve"]["reserve"]["underlyingAsset"]
            ),
            history_item["scaledATokenBalance"],
        ]
    )
    logger.debug(
        {
            "user_id": user_id,
            "number of records": len(user_atoken_balance_histories[user_id]),
        }
    )
save_data(
    user_atoken_balance_histories,
    "user_atoken_balance_histories",
    data_layer="prepared",
)
