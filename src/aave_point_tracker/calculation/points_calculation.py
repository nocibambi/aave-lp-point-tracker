from decimal import Decimal

from aave_point_tracker.utils.utils import load_data

starting_balances = load_data("users_starting_balances")


[for starting_balances]



(Decimal(user_reserve["scaledATokenBalance"]) / Decimal(token_decimals)) * (
    Decimal(str(normalized_income)) / Decimal(ray_decimals)
)
