from decimal import Decimal, getcontext, FloatOperation
from datetime import datetime, timezone
from aave_point_tracker.utils.utils import load_data, load_configs
import pandas as pd
import requests
from web3 import Web3
import pandera

configs = load_configs()
RAY_DECIMALS: int = configs["ray_decimals"]
FIRST_DATE: str = configs["first_date"]
LAST_DATE: str = configs["last_date"]


class DecimalSeries(pandera.SeriesSchema):
    dtype = Decimal


decimal_context = getcontext()
decimal_context.traps[FloatOperation] = True

web3 = Web3()

starting_balances: list[dict] = load_data("starting_balances")
reserve_assets: list[dict] = load_data("reserve_assets")
reserve_asset_prices: dict[str, list[dict]] = load_data("reserve_asset_prices")
reserve_liquidity_index_histories: dict[str, list[dict]] = load_data(
    "reserve_liquidity_index_histories"
)
atoken_balance_histories: list[dict] = load_data("atoken_balance_histories")

aave_addresses_url = (
    "https://raw.githubusercontent.com/bgd-labs/aave-address-book/main/safe.csv"
)
response = requests.get(aave_addresses_url)
aave_addresses = [record.split(",")[0] for record in response.text.split("\n")[1:]]


user_starting_balances: dict[str, list] = {}
for starting_balance in starting_balances:
    user_id: str = web3.to_checksum_address(starting_balance["id"])
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
        token_id: str = web3.to_checksum_address(reserve["reserve"]["underlyingAsset"])
        print(len(user_starting_balances), user_id, token_id, scaled_atoken_balance)
        user_starting_balances[user_id].append([token_id, scaled_atoken_balance])

liquidity_indexes: dict[str, list] = {}
for asset in reserve_liquidity_index_histories:
    asset_checksummed = web3.to_checksum_address(asset)
    liquidity_indexes[asset_checksummed] = []
    for record in reserve_liquidity_index_histories[asset]:
        liquidity_indexes[asset_checksummed].append(
            [
                datetime.fromtimestamp(record["timestamp"], tz=timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                record["liquidityIndex"],
            ]
        )

asset_decimals: dict[str, int] = {
    web3.to_checksum_address(asset["underlyingAsset"]): asset["decimals"]
    for asset in reserve_assets
}

asset_prices = {
    web3.to_checksum_address(asset): prices
    for asset, prices in reserve_asset_prices.items()
}

user_atoken_balance_histories: dict[str, list] = {}
for history_item in atoken_balance_histories:
    user_id = web3.to_checksum_address(history_item["userReserve"]["user"]["id"])
    if user_id in aave_addresses:
        continue
    if user_id not in user_atoken_balance_histories:
        user_atoken_balance_histories[user_id] = []
    user_atoken_balance_histories[user_id].append(
        [
            datetime.fromtimestamp(history_item["timestamp"], tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            web3.to_checksum_address(
                history_item["userReserve"]["reserve"]["underlyingAsset"]
            ),
            history_item["scaledATokenBalance"],
        ]
    )
    print(user_id, len(user_atoken_balance_histories[user_id]))

############################# USER CALCULATION #############################


def print_user_reserve_id(user_id, asset):
    user_reserve_id = (
        user_id + asset + "0x2f39d218133afab8f2b819b1066c7e434ad94e9e"
    ).lower()
    print(user_reserve_id)


user_id = "0xb7884a472Caeb66Fc65d1A77113dE9809D5DCA0f"
starting_balances: list[list] = user_starting_balances[user_id]
balance_histories: list[list[str]] = user_atoken_balance_histories[user_id]

starting_assets = set([balance[0] for balance in starting_balances])
updated_assets = set([record[1] for record in balance_histories])

assets_held: set = starting_assets | updated_assets

# asset: str = "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0"
asset: str = "0x514910771AF9Ca656af840dff83E8264EcF986CA"

print_user_reserve_id(user_id, asset)

period_dates = pd.date_range(
    start=FIRST_DATE,
    end=LAST_DATE,
    freq="D",
    tz=timezone.utc,
)
period_dates

starting_balance = (
    pd.Series(
        {
            "date": period_dates[0],
            "asset": asset,
            "balance": (
                0
                if asset not in starting_assets
                else next(filter(lambda x: x[0] == asset, starting_balances))[1]
            ),
            "day_fraction": 0,
            "liquidity_index": None,
            "price": None,
        }
    )
    .to_frame()
    .T
)
balance_updates = pd.DataFrame.from_records(
    [
        {
            "date": record[0],
            "asset": asset,
            "balance": record[2],
            "day_fraction": 0,
            "liquidity_index": None,
            "price": None,
        }
        for record in list(filter(lambda record: record[1] == asset, balance_histories))
    ]
)
closing_balance = pd.DataFrame(
    data=[
        {
            "date": period_dates[-1],
            "asset": asset,
            "balance": balance_updates.sort_values("date")["balance"].iloc[-1],
            "day_fraction": 0,
            "liquidity_index": None,
            "price": None,
        }
    ]
)
balance_history = pd.concat([starting_balance, balance_updates, closing_balance])


def median_decimals(index_history: DecimalSeries) -> Decimal:
    index_history = index_history.sort_index()
    n_records = len(index_history)

    if n_records == 0:
        return None

    if n_records % 2 == 0:
        return Decimal(
            (
                index_history.iloc[n_records // 2 - 1]
                + index_history.iloc[len(index_history) // 2]
            )
            / Decimal(2)
        )
    else:
        return Decimal(index_history.iloc[(n_records + 1) // 2 - 1])


def interpolate_decimals(decimal_series: DecimalSeries) -> DecimalSeries:
    decimal_series = decimal_series.sort_index()
    indexes_w_nan = decimal_series[decimal_series.isna().values].index
    indexes_w_vals = decimal_series[~decimal_series.isna().values].index

    for index_w_nan in indexes_w_nan:
        first_idx_w_val = indexes_w_vals[indexes_w_vals < index_w_nan][-1]
        last_idx_w_val = indexes_w_vals[indexes_w_vals > index_w_nan][0]

        first_val = decimal_series.loc[first_idx_w_val]
        last_val = decimal_series.loc[last_idx_w_val]

        dist_bw_vals = (last_idx_w_val - first_idx_w_val).total_seconds() / 60 / 60
        if not dist_bw_vals.is_integer():
            raise ValueError(f"dist_bw_vals is not an integer: {dist_bw_vals}")
        dist_bw_vals = int(dist_bw_vals)

        index_win_nans = (index_w_nan - first_idx_w_val).total_seconds() / 60 / 60
        if not index_win_nans.is_integer():
            raise ValueError(f"index_win_nans is not an integer: {index_win_nans}")
        index_win_nans = int(index_win_nans)

        decimal_series.loc[index_w_nan] = first_val + (last_val - first_val) * Decimal(
            index_win_nans
        ) / Decimal(dist_bw_vals)

    return decimal_series


liquidity_index_resampled = (
    (
        pd.DataFrame(liquidity_indexes[asset], columns=["date", "liquidity_index"])
        .pipe(
            lambda x: x.assign(
                date=pd.to_datetime(x["date"], utc=True, format="%Y-%m-%d %H:%M:%S"),
                liquidity_index=x["liquidity_index"].apply(Decimal),
            )
        )
        .pipe(
            lambda x: x.loc[
                (x["date"] >= pd.to_datetime(FIRST_DATE, utc=True, format="%Y-%m-%d"))
                & (x["date"] <= pd.to_datetime(LAST_DATE, utc=True, format="%Y-%m-%d"))
            ]
        )
        .resample(
            "1h",
            on="date",
        )
    )
    .apply(median_decimals)
    .apply(interpolate_decimals)
)
liquidity_index_resampled

asset_prices[asset]

# balance
# (Decimal(user_reserve["scaledATokenBalance"]) / Decimal(token_decimals)) * (
#     Decimal(str(normalized_income)) / Decimal(ray_decimals)
# )


# def calculate_currentATokenBalance(
#     scaled_atoken_balance, liquidity_index, token_decimals
# ):
#     return (Decimal(scaled_atoken_balance) / Decimal(10**token_decimals)) * (
#         Decimal(liquidity_index) / Decimal(10**RAY_DECIMALS)
#     )

# def get_user_reserve_stats(user_starting_balances, user_atoken_balance_histories):
#     return pd.DataFrame(
#         sorted(
#             [
#                 [
#                     user,
#                     (
#                         len(user_starting_balances[user])
#                         if user in user_starting_balances
#                         else 0
#                     ),
#                     len(set(record[1] for record in balance_history)),
#                     len(balance_history),
#                 ]
#                 for user, balance_history in user_atoken_balance_histories.items()
#             ],
#             key=lambda user: user[3],
#             reverse=True,
#         ),
#         columns=["user", "starting assets", "updated assets", "updates"],
#     )
# get_user_reserve_stats(user_starting_balances, user_atoken_balance_histories).tail(30)
