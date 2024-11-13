from aave_point_tracker.calculation.data_preparation import prepare_datasets
from aave_point_tracker.calculation.points_calculation import calculate_user_tvls
from aave_point_tracker.data_collection.aave_addresses import fetch_aave_addresses
from aave_point_tracker.data_collection.atoken_balance_histories import (
    fetch_atoken_balance_histories,
)
from aave_point_tracker.data_collection.reserve_asset_prices import (
    fetch_reserve_asset_prices,
)
from aave_point_tracker.data_collection.reserve_assets import fetch_reserve_assets
from aave_point_tracker.data_collection.reserve_liquidity_indexes import (
    fetch_reserve_liquidity_indexes,
)
from aave_point_tracker.data_collection.starting_balances import fetch_starting_balances
from aave_point_tracker.utils.utils import save_data


def main():
    fetch_aave_addresses()
    fetch_atoken_balance_histories()
    fetch_reserve_asset_prices()
    fetch_reserve_assets()
    fetch_reserve_liquidity_indexes()
    fetch_starting_balances()

    prepare_datasets()

    user_tvls = calculate_user_tvls()

    save_data(user_tvls, "user_tvls", data_layer="calculated")


if __name__ == "__main__":
    main()
