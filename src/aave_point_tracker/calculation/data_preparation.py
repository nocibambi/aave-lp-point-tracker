import logging

from web3 import Web3

from aave_point_tracker.utils.utils import load_data, save_data

web3_client = Web3()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


def _prepare_starting_balances() -> None:
    starting_balances: list[dict] = load_data("starting_balances", data_layer="raw")
    aave_addresses: list[str] = load_data("aave_addresses", data_layer="raw")
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
            token_id: str = web3_client.to_checksum_address(
                reserve["reserve"]["underlyingAsset"]
            )
            if user_id not in user_starting_balances:
                user_starting_balances[user_id] = []
            user_starting_balances[user_id].append([token_id, scaled_atoken_balance])
    logger.debug(
        f"number of starting balances:  {len(user_starting_balances)}",
    )
    save_data(user_starting_balances, "user_starting_balances", data_layer="prepared")


def _prepare_reserve_liquidity_indexes() -> None:
    reserve_liquidity_index_histories: dict[str, list[dict]] = load_data(
        "reserve_liquidity_index_histories", data_layer="raw"
    )
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


def _prepare_asset_decimals() -> None:
    reserve_assets: list[dict] = load_data("reserve_assets", data_layer="raw")
    asset_decimals: dict[str, int] = {
        web3_client.to_checksum_address(asset["underlyingAsset"]): asset["decimals"]
        for asset in reserve_assets
    }
    save_data(asset_decimals, "asset_decimals", data_layer="prepared")


def _prepare_asset_prices() -> None:
    reserve_asset_prices: dict[str, list[dict]] = load_data(
        "reserve_asset_prices", data_layer="raw"
    )
    asset_prices = {
        web3_client.to_checksum_address(asset): prices
        for asset, prices in reserve_asset_prices.items()
    }
    save_data(asset_prices, "asset_prices", data_layer="prepared")


def _prepare_user_atoken_balance_histories() -> None:
    atoken_balance_histories: list[dict] = load_data(
        "atoken_balance_histories", data_layer="raw"
    )
    aave_addresses: list[str] = load_data("aave_addresses", data_layer="raw")
    user_atoken_balance_histories: dict[str, list] = {}
    for history_item in atoken_balance_histories:
        user_id = web3_client.to_checksum_address(
            history_item["userReserve"]["user"]["id"]
        )
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
        f"Number of user aToken histories: {len(user_atoken_balance_histories)}"
    )
    save_data(
        user_atoken_balance_histories,
        "user_atoken_balance_histories",
        data_layer="prepared",
    )


def prepare_datasets() -> None:
    """
    Prepares and processes datasets required for TVL calculation.

    This function orchestrates the preparation of various datasets
    used in the TVL calculation.

    Each helper function loads raw data, processes it, and saves the prepared
    data for use in further calculations.
    """
    _prepare_starting_balances()
    _prepare_reserve_liquidity_indexes()
    _prepare_asset_decimals()
    _prepare_asset_prices()
    _prepare_user_atoken_balance_histories()
