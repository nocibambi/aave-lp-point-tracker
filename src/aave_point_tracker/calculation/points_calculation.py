from datetime import timezone, datetime
from decimal import Decimal, FloatOperation, getcontext

import pandas as pd
import pandera
from web3 import Web3
import logging
from aave_point_tracker.utils.utils import load_configs, load_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

configs = load_configs()
RAY_DECIMALS: int = configs["ray_decimals"]
FIRST_DATE: str = configs["first_date"]
LAST_DATE: str = configs["last_date"]


DecimalSeriesSchema = pandera.SeriesSchema(Decimal)


decimal_context = getcontext()
decimal_context.traps[FloatOperation] = True

web3 = Web3()

# ######### LOAD DATA ##########
user_starting_balances = load_data("user_starting_balances", data_layer="prepared")
liquidity_indexes = load_data("liquidity_indexes", data_layer="prepared")
asset_decimals = load_data("asset_decimals", data_layer="prepared")
asset_prices = load_data("asset_prices", data_layer="prepared")
user_atoken_balance_histories = load_data(
    "user_atoken_balance_histories", data_layer="prepared"
)

# ############################ USER CALCULATION #############################


def print_user_reserve_id(user_id, asset):
    user_reserve_id = (
        user_id + asset + configs["aave"]["ethereum_v3_main"]["pool_address_provider"]
    ).lower()
    logger.debug(user_reserve_id)


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


def day_fraction(timestamp: float) -> float:
    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    return (dt - datetime.combine(dt, dt.min.time(), tzinfo=dt.tzinfo)).seconds / (
        24 * 60 * 60
    )


starting_balance = pd.DataFrame(
    data=[
        {
            "date": period_dates[0],
            "asset": asset,
            "balance": (
                0
                if asset not in starting_assets
                else next(filter(lambda x: x[0] == asset, starting_balances))[1]
            ),
            "day_fraction": 0,
        }
    ]
)

balance_updates = pd.DataFrame(
    [
        {
            # "date": record[0],
            "date": datetime.fromtimestamp(record[0], tz=timezone.utc),
            "asset": asset,
            "balance": record[2],
            "day_fraction": day_fraction(record[0]),
        }
        for record in list(filter(lambda record: record[1] == asset, balance_histories))
    ]
)


closing_balance = pd.DataFrame(
    data=[
        {
            "date": period_dates[-1] + pd.Timedelta(days=1),
            "asset": asset,
            "balance": balance_updates.sort_values("date")["balance"].iloc[-1],
            "day_fraction": 0,
        }
    ]
)
balance_history = (
    pd.concat([starting_balance, balance_updates, closing_balance])
    .astype(
        {
            "date": "datetime64[ns, UTC]",
            "asset": str,
            "balance": str,
            "day_fraction": float,
        }
    )
    .set_index("date")
)
balance_history


def interpolate_decimals(decimal_series: pd.DataFrame) -> pd.Series:
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
                date=datetime.fromtimestamp(x["date"], tz=timezone.utc),
                # date=pd.to_datetime(x["date"], utc=True, format="%Y-%m-%d %H:%M:%S"),
                liquidity_index=x["liquidity_index"].apply(Decimal),
            )
        )
        .pipe(
            lambda x: x.loc[
                (x["date"] >= pd.to_datetime(FIRST_DATE, utc=True, format="%Y-%m-%d"))
                & (
                    x["date"]
                    <= pd.to_datetime(LAST_DATE, utc=True, format="%Y-%m-%d")
                    + pd.Timedelta(days=1)
                )
            ]
        )
        .resample(
            "1d",
            on="date",
        )
    )
    .median()
    .apply(interpolate_decimals)
)
liquidity_index_resampled

price_resampled = (
    pd.DataFrame(asset_prices[asset], columns=["date", "price"])
    .pipe(
        lambda x: x.assign(
            date=pd.to_datetime(x["date"], utc=True, format="%Y-%m-%d %H:%M:%S"),
            price=x["price"].apply(lambda x: Decimal(str(x))),
        )
    )
    .resample("1h", on="date")
    .median()
    .sort_index()
    .pipe(lambda x: x.loc[x.index.isin(period_dates), :])
)
price_resampled

pd.merge(
    price_resampled,
    liquidity_index_resampled,
    how="outer",
    left_index=True,
    right_index=True,
)

balance_history.join(liquidity_index_resampled, how="outer").join(
    price_resampled, how="outer"
)

balance_history


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
