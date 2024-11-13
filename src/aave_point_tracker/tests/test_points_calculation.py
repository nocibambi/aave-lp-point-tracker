from pandas import Timestamp, DataFrame
from decimal import Decimal

from aave_point_tracker.calculation.points_calculation import get_user_reserve_tvl

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
    assert get_user_reserve_tvl(DataFrame(sample_data)) == Decimal(
        "4054545.287340917089598411247"
    )
