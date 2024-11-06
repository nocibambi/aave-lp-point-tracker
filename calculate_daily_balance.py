scaled_token_balance = "1"

previous_timestamp = 1727551654
timestamp = 1727551655
period_in_days = timestamp - previous_timestamp

def total_borrows(timestamp):
    return total_stable_borrows(timestamp) + total_variable_borrows(timestamp)

def borrow_rate(timestamp):
    return (
        variable_borrow_rate(timestamp) * total_variable_borrows(timestamp)
        + stable_borrow_rate(timestamp) * total_stable_borrows(timestamp)
    ) / total_borrows(timestamp))


def utilization_rate(timestamp):
    return total_borrows(timestamp) / total_liquidity(timestamp)


def liquidity_rate(timestamp):
    return borrow_rate(timestamp) + utilization_rate(timestamp)


def liquidity_index(timestamp, previous_liquidity_index):
    return (liquidity_rate(timestamp) * period_in_days + 1) * previous_liquidity_index


def normalized_income(timestamp, previous_timestamp):
    return (liquidity_rate(timestamp) * period_in_days + 1) * liquidity_index(
        previous_timestamp
    )


def actual_balance(scaled_token_balance, normalized_income):
    return scaled_token_balance * normalized_income
