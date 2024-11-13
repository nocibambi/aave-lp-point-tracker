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
    """
    Given a timestamp in seconds, returns the fraction of the day that has elapsed at the given timestamp.

    :param timestamp: The timestamp in seconds.
    :return: The fraction of the day that has elapsed, as a float between 0 and 1.
    """
    dt = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
    return (dt - datetime.combine(dt, dt.min.time(), tzinfo=dt.tzinfo)).seconds / (
        24 * 60 * 60
    )


def _collect_balance_history(
    starting_balances: list, balance_histories: list, asset: str, starting_assets: set
) -> pd.DataFrame:
    """
    Collect and prepare the balance history for a specific asset.

    Constructs a DataFrame representing the history of balances
    for a given asset, combining starting balances and balance histories. It
    includes the date, balance, and day fraction for each relevant record,
    and converts the balance to a Decimal type.

    Args:
        starting_balances (list): A list of tuples containing starting balance
            information, where each tuple consists of an asset identifier and
            its corresponding starting balance.
        balance_histories (list): A list of records representing balance history
            data, where each record includes a timestamp, asset identifier, and
            balance.
        asset (str): The asset identifier for which the balance history is being
            collected.
        starting_assets (set): A set of asset identifiers that are considered as
            starting assets.

    Returns:
        pd.DataFrame: A DataFrame with the balance history for the specified
        asset, indexed by date, and including columns for 'balance' and
        'day_fraction'.
    """
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
    """
    Compute the median of the given index history, returning `None` if it is empty.
    """
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
    """
    Linearly interpolate missing values in a Pandas Series of Decimals.

    This function takes a Pandas Series containing Decimal numbers and interpolates
    missing values (NaNs) by performing linear interpolation between the closest
    non-NaN values before and after each NaN. The interpolation is done based on
    the time difference between the indices of the non-NaN values.

    Args:
        decimal_series (pd.Series): A Pandas Series with a datetime index, containing
                                    Decimal values with potential NaNs.

    Returns:
        pd.Series: A Pandas Series with the same index as `decimal_series` where
                   all NaN values have been filled through linear interpolation.

    Raises:
        ValueError: If the time difference between indices is not an integer number
                    of hours, or if the time difference from a non-NaN value to a NaN
                    index is not an integer number of hours.
    """
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
    """
    Resample the liquidity index data for a given asset on a daily basis.

    Resamples the liquidity index data to a daily frequency. The data is filtered to
    the specified range, and missing values are interpolated. The resulting DataFrame is
    backfilled and forward-filled to ensure continuity.

    Args:
        liquidity_indexes (dict): A dictionary containing liquidity index data with timestamps.
        asset (str): The identifier of the asset to resample the liquidity index data for.

    Returns:
        pd.DataFrame: A DataFrame containing the resampled liquidity index data with 'date' as
                      the index and 'liquidity_index' as the column, adjusted to the specified
                      frequency and date range.
    """
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
    """
    Resample the price data for a given asset on an hourly basis.

    Resamples the price data to an hourly frequency, computes the median decimals,
    and then filteres to the specified dates.
    Missing values in the resulting DataFrame are backfilled and forward-filled to ensure continuity.

    Args:
        asset_prices (dict): A dictionary containing asset price data with timestamps.
        asset (str): The identifier of the asset to resample the price data for.

    Returns:
        pd.DataFrame: A DataFrame containing the resampled price data with the 'date' as the index
                      and 'price' as the column, adjusted to the specified frequency and date range.
    """
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
    """
    Combine the balance history with the resampled liquidity index and price data
    and build the reserve history for a user.

    The balance history is joined with the resampled liquidity index and price data
    using an outer join. The resulting dataframe is then modified to fill in any
    missing values for the balance, liquidity index, and price.

    Day fraction is calculated by taking the difference of the day fraction column and
    then shifting the result up by one row. The result is then modified to be a
    positive number between 0 and 1 by taking the absolute value and then subtracting
    the result from 1 if the result is greater than 1. The result is then converted to a
    Decimal object.

    The last row of the dataframe is then dropped.

    Parameters
    ----------
    balance_history : pd.DataFrame
        The balance history for the user.
    liquidity_index_resampled : pd.DataFrame
        The resampled liquidity index data for the reserve.
    price_resampled : pd.DataFrame
        The resampled price data for the reserve.

    Returns
    -------
    pd.DataFrame
        The reserve history for the user.
    """
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
    """
    Calculate the total value locked (TVL) for a user's reserve.

    This function computes the TVL for a specific asset by processing the reserve
    history DataFrame. It takes into account the user's balance, the liquidity
    index, the asset price, and the day fraction.

    Args:
        reserve_history (pd.DataFrame): A DataFrame containing the reserve history
            with columns 'balance', 'day_fraction', 'liquidity_index', and 'price'.
        asset (str): The asset identifier used to access asset-specific details
            like decimals.

    Returns:
        Decimal: The calculated TVL for the user's reserve in the specified asset.
    """
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
    """
    Calculate total value locked (TVL) for each user.

    Iterate through all users, collecting their starting balances and aToken balance histories.
    For each asset held by a user, resample the liquidity index and price data.
    Calculate the TVL for each reserve and sum them to get the final TVL for the user.
    """

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
