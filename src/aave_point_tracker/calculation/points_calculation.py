import logging
from datetime import datetime, timezone
from decimal import Decimal, FloatOperation, getcontext, localcontext

import pandas as pd
from web3 import Web3

from aave_point_tracker.utils.utils import load_configs, load_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

configs = load_configs()
RAY_DECIMALS: int = configs["ray_decimals"]
FIRST_DATE: str = configs["first_date"]
LAST_DATE: str = configs["last_date"]

PERIOD_DATES: pd.DatetimeIndex = pd.date_range(
    start=FIRST_DATE,
    end=LAST_DATE,
    freq="D",
    tz=timezone.utc,
)

decimal_context = getcontext()
decimal_context.traps[FloatOperation] = True

web3 = Web3()

# ######### LOAD DATASETS ##########
user_starting_balances: dict[str, list] = load_data(
    "user_starting_balances", data_layer="prepared"
)
liquidity_indexes: dict[str, list] = load_data(
    "liquidity_indexes", data_layer="prepared"
)
asset_decimals: dict[str, int] = load_data("asset_decimals", data_layer="prepared")
asset_prices: dict[str, list] = load_data("asset_prices", data_layer="prepared")
user_atoken_balance_histories: dict[str, list] = load_data(
    "user_atoken_balance_histories", data_layer="prepared"
)


# ########## METHODS ##########
def _day_fraction(timestamp: float) -> float:
    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    return (dt - datetime.combine(dt, dt.min.time(), tzinfo=dt.tzinfo)).seconds / (
        24 * 60 * 60
    )


def _collect_balance_history(
    starting_balances: list, balance_histories: list, asset: str, starting_assets: set
) -> pd.DataFrame:
    balance_history = pd.DataFrame(
        data=[
            {
                "date": PERIOD_DATES[0],
                "balance": Decimal(
                    0
                    if asset not in starting_assets
                    else next(filter(lambda x: x[0] == asset, starting_balances))[1]
                ),
                "day_fraction": 0,
            }
        ]
    )
    if balance_histories:
        balance_history = pd.concat(
            [
                balance_history,
                pd.DataFrame(
                    [
                        {
                            "date": datetime.fromtimestamp(record[0], tz=timezone.utc),
                            "balance": Decimal(record[2]),
                            "day_fraction": _day_fraction(record[0]),
                        }
                        for record in list(
                            filter(lambda record: record[1] == asset, balance_histories)
                        )
                    ]
                ),
            ]
        )
    balance_history = (
        pd.concat(
            [
                balance_history,
                pd.DataFrame(
                    data=[
                        {
                            "date": PERIOD_DATES[-1] + pd.Timedelta(days=1),
                            "balance": Decimal(
                                balance_history.sort_values("date")["balance"].iloc[-1]
                            ),
                            "day_fraction": 0,
                        }
                    ]
                ),
            ]
        )
        .assign(balance=lambda x: x["balance"].apply(Decimal))
        .astype(
            {
                "date": "datetime64[ns, UTC]",
                "day_fraction": float,
            }
        )
        .set_index("date")
    )
    assert {
        col: balance_history[col].apply(type).unique().item() for col in balance_history
    }["balance"] is Decimal
    return balance_history


def _median_decimals(index_history: pd.Series) -> Decimal | None:
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


def _interpolate_decimals(decimal_series: pd.Series) -> pd.Series:
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


def _resample_liquidity_index(liquidity_indexes, asset) -> pd.DataFrame:
    return (
        pd.DataFrame(liquidity_indexes[asset], columns=["date", "liquidity_index"])
        .pipe(
            lambda x: x.assign(
                date=pd.to_datetime(x["date"], utc=True, unit="s"),
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
        .apply(_median_decimals)
        .apply(_interpolate_decimals)
        .join(pd.DataFrame(index=PERIOD_DATES), how="outer")
        .bfill()
        .ffill()
    )


def _resample_price(asset_prices: dict, asset: str) -> pd.DataFrame:
    return (
        pd.DataFrame(asset_prices[asset], columns=["date", "price"])
        .pipe(
            lambda x: x.assign(
                date=pd.to_datetime(x["date"], utc=True, unit="ms"),
                price=x["price"].apply(lambda x: Decimal(str(x))),
            )
        )
        .resample("1h", on="date")
        .apply(_median_decimals)
        .sort_index()
        .pipe(lambda x: x.loc[x.index.isin(PERIOD_DATES), :])
        .join(pd.DataFrame(index=PERIOD_DATES), how="outer")
        .bfill()
        .ffill()
    )


def _get_build_reserve_history(
    balance_history, liquidity_index_resampled, price_resampled
) -> pd.DataFrame:
    return (
        balance_history.join(liquidity_index_resampled, how="outer")
        .join(price_resampled, how="outer")
        .pipe(
            lambda x: x.assign(
                balance=x["balance"].ffill(),
                liquidity_index=x["liquidity_index"].ffill(),
                price=x["price"].ffill(),
                day_fraction=x["day_fraction"]
                .fillna(0)
                .diff()
                .pipe(lambda x: x.add(x.lt(0).mul(1)))
                .shift(-1)
                .pipe(lambda x: x.add(x.mul(-1).add(1).floordiv(1)))
                .astype(str)
                .apply(Decimal),
            )
        )
        .iloc[:-1]
    )


def _get_user_reserve_tvl(reserve_history, asset) -> Decimal:
    with localcontext(prec=42) as _:
        reserve_points = reserve_history.pipe(
            lambda x: x["balance"]
            .div(Decimal(10 ** asset_decimals[asset]))
            .mul(x["day_fraction"])
            .mul(x["liquidity_index"])
            .div(10 ** Decimal(RAY_DECIMALS))
            .mul(x["price"])
        ).sum()
    reserve_points = +reserve_points
    return reserve_points


# ########## USER CALCULATION ##########


def calculate_user_tvls() -> dict[str, float]:
    user_ids = user_starting_balances.keys() | user_atoken_balance_histories.keys()
    user_tvls = {}

    for i, user_id in enumerate(user_ids, start=1):
        if user_id in user_tvls:
            continue
        try:
            starting_balances: list[list] = user_starting_balances[user_id]
            starting_assets = set([balance[0] for balance in starting_balances])
        except KeyError:
            starting_balances = []
            starting_assets = set()

        try:
            balance_histories: list[list[str]] = user_atoken_balance_histories[user_id]
            updated_assets = set([record[1] for record in balance_histories])
        except KeyError:
            balance_histories = []
            updated_assets = set()

        assets_held: set = starting_assets | updated_assets

        user_tvl = Decimal(0)
        for asset in assets_held:
            balance_history = _collect_balance_history(
                starting_balances=starting_balances,
                balance_histories=balance_histories,
                asset=asset,
                starting_assets=starting_assets,
            )
            try:
                liquidity_index_resampled = _resample_liquidity_index(
                    liquidity_indexes=liquidity_indexes, asset=asset
                )
            except KeyError:
                logger.debug(f"Failed to resample liquidity index for {asset}")
                continue

            price_resampled = _resample_price(asset_prices=asset_prices, asset=asset)
            reserve_history = _get_build_reserve_history(
                balance_history=balance_history,
                liquidity_index_resampled=liquidity_index_resampled,
                price_resampled=price_resampled,
            )
            reserve_tvl = _get_user_reserve_tvl(
                reserve_history=reserve_history, asset=asset
            )
            user_tvl += reserve_tvl
        user_tvls[user_id] = float(user_tvl)
        logger.debug(f"{i}. {user_id}: {user_tvl}")

    return user_tvls
