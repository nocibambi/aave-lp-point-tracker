from decimal import Decimal
from datetime import datetime, timezone
from aave_point_tracker.utils.utils import load_data, load_configs

configs = load_configs()
RAY_DECIMALS: int = configs["ray_decimals"]

starting_balances: list[dict] = load_data("starting_balances")
reserve_assets: list[dict] = load_data("reserve_assets")
reserve_asset_prices: dict[str, list[dict]] = load_data("reserve_asset_prices")
reserve_liquidity_index_histories: dict[str, list[dict]] = load_data(
    "reserve_liquidity_index_histories"
)
atoken_balance_histories = load_data("atoken_balance_histories")

user_starting_balances: dict[str, list] = {}
for starting_balance in starting_balances:
    user_id: str = starting_balance["id"]

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
        token_id: str = reserve["reserve"]["underlyingAsset"]
        print(len(user_starting_balances), user_id, token_id, scaled_atoken_balance)
        user_starting_balances[user_id].append([token_id, scaled_atoken_balance])
user_starting_balances

sum([len(user_starting_balances[user]) for user in user_starting_balances])
len(user_starting_balances)
# 37452, 28310

liquidity_indexes: dict[str, list] = {}
for asset in reserve_liquidity_index_histories:
    liquidity_indexes[asset] = []
    for record in reserve_liquidity_index_histories[asset]:
        liquidity_indexes[asset].append(
            [
                datetime.fromtimestamp(record["timestamp"], tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                record["liquidityIndex"],
            ]
        )
liquidity_indexes

reserve_asset_prices

asset_decimals: dict[str, int] = {
    asset["underlyingAsset"]: asset["decimals"] for asset in reserve_assets
}
asset_decimals

user_atoken_balance_histories: dict[str, list] = {}
for history_item in atoken_balance_histories:
    user_id = history_item["userReserve"]["user"]["id"]
    if user_id not in user_atoken_balance_histories:
        user_atoken_balance_histories[user_id] = []
    user_atoken_balance_histories[user_id].append(
        [
            datetime.fromtimestamp(history_item["timestamp"], tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            history_item["userReserve"]["reserve"]["underlyingAsset"],
            history_item["scaledATokenBalance"],
        ]
    )
user_atoken_balance_histories


def calculate_currentATokenBalance(
    scaled_atoken_balance, liquidity_index, token_decimals
):
    return (Decimal(scaled_atoken_balance) / Decimal(10**token_decimals)) * (
        Decimal(liquidity_index) / Decimal(10**RAY_DECIMALS)
    )


# Price
# pd.DataFrame.from_dict(reserves_asset_prices)
# pd.DataFrame.from_records(
#     reserves_asset_prices["WETH"], columns=["date", "price"]
# ).set_index("date")

# {asset: len(reserves_asset_prices[asset]) for asset in reserves_asset_prices}

# len(response.json()["prices"])

# [
#     [
#         datetime.fromtimestamp(price[0] / 1000, tz=timezone.utc).strftime(
#             "%Y-%m-%d %H:%M:%S"
#         ),
#         price[1],
#     ]
#     for price in prices
# ]

# balance
# (Decimal(user_reserve["scaledATokenBalance"]) / Decimal(token_decimals)) * (
#     Decimal(str(normalized_income)) / Decimal(ray_decimals)
# )
