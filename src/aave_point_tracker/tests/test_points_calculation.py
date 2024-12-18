from decimal import Decimal

from pandas import DataFrame, Timestamp

from aave_point_tracker.calculation.points_calculation import _get_user_reserve_tvl

sample_data = {
    "balance": {
        Timestamp("2024-09-27 23:05:35+0000", tz="UTC"): Decimal(
            "6117000000000000000000"
        ),
        Timestamp("2024-09-28 00:00:00+0000", tz="UTC"): Decimal(
            "6117000000000000000000"
        ),
        Timestamp("2024-09-29 00:00:00+0000", tz="UTC"): Decimal(
            "6117000000000000000000"
        ),
        Timestamp("2024-09-30 00:00:00+0000", tz="UTC"): Decimal(
            "6117000000000000000000"
        ),
        Timestamp("2024-10-01 00:00:00+0000", tz="UTC"): Decimal(
            "6117000000000000000000"
        ),
    },
    "day_fraction": {
        Timestamp("2024-09-27 23:05:35+0000", tz="UTC"): Decimal("0.03778935185185184"),
        Timestamp("2024-09-28 00:00:00+0000", tz="UTC"): Decimal("1.0"),
        Timestamp("2024-09-29 00:00:00+0000", tz="UTC"): Decimal("1.0"),
        Timestamp("2024-09-30 00:00:00+0000", tz="UTC"): Decimal("1.0"),
        Timestamp("2024-10-01 00:00:00+0000", tz="UTC"): Decimal("1.0"),
    },
    "liquidity_index": {
        Timestamp("2024-09-27 23:05:35+0000", tz="UTC"): Decimal(
            "1000000000000000000000000000"
        ),
        Timestamp("2024-09-28 00:00:00+0000", tz="UTC"): Decimal(
            "1000000000000000000000000000"
        ),
        Timestamp("2024-09-29 00:00:00+0000", tz="UTC"): Decimal(
            "1000000000000000000000000000"
        ),
        Timestamp("2024-09-30 00:00:00+0000", tz="UTC"): Decimal(
            "1000000000000000000000000000"
        ),
        Timestamp("2024-10-01 00:00:00+0000", tz="UTC"): Decimal(
            "1000000000000000000000000000"
        ),
    },
    "price": {
        Timestamp("2024-09-27 23:05:35+0000", tz="UTC"): Decimal("165.08624119469505"),
        Timestamp("2024-09-28 00:00:00+0000", tz="UTC"): Decimal("169.73486415906967"),
        Timestamp("2024-09-29 00:00:00+0000", tz="UTC"): Decimal("163.94127780669695"),
        Timestamp("2024-09-30 00:00:00+0000", tz="UTC"): Decimal("161.4588368374629"),
        Timestamp("2024-10-01 00:00:00+0000", tz="UTC"): Decimal("161.4588368374629"),
    },
}


def test_get_user_reserve_tvl():
    """
    Test _get_user_reserve_tvl function.

    Test case: asset is AAVE, reserve history is a DataFrame of sample data.
    Expected result: Decimal("4054545.287340917089598411247")
    """
    asset = "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9"  # AAVE
    assert _get_user_reserve_tvl(
        reserve_history=DataFrame(sample_data), asset=asset
    ) == Decimal("4054545.287340917089598411247")
