def get_user_reserve_stats(user_starting_balances, user_atoken_balance_histories):
    return pd.DataFrame(
        sorted(
            [
                [
                    user,
                    (
                        len(user_starting_balances[user])
                        if user in user_starting_balances
                        else 0
                    ),
                    len(set(record[1] for record in balance_history)),
                    len(balance_history),
                ]
                for user, balance_history in user_atoken_balance_histories.items()
            ],
            key=lambda user: user[3],
            reverse=True,
        ),
        columns=["user", "starting assets", "updated assets", "updates"],
    )


get_user_reserve_stats(user_starting_balances, user_atoken_balance_histories).pipe(
    lambda x: x[x["updates"].gt(5) & x["starting assets"].gt(0)]
).tail(10)


def print_user_reserve_id(user_id, asset):
    user_reserve_id = (
        user_id + asset + configs["aave"]["ethereum_v3_main"]["pool_address_provider"]
    ).lower()
    logger.debug(user_reserve_id)
