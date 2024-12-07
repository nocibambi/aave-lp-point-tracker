# AAVE Deposit TVL point tracker

An off-chain points tracker for liquidity providers on-chain. Tracks all TVL, in USD over time held by each depositor on Aave for the month of September (1st - 30th inclusive). Presents the numbers in an API. Each USD worth of TVL (any asset) held for 1 day counts as 1 point for the respective user.

Current implementation works with Aave V3 on the Ethereum mainnet.

## Instructions

### Requirements

python >=3.13

- fastapi[standard]==0.115.5
- requests==2.32.3
- pandas==2.2.3
- python-dotenv==1.0.1
- web3==7.5.0

### Environment variables

The ETL requires CoinGecko and TheGraph API token access keys.

Rename the `.env.sample` file to `.env` and fill out with the required path and api token information.

### Package installation

Setup the environment with the following command:

```shell
make setup
```

### ETL pipeline

Start the ETL pipeline with the following command:

```shell
make run
```

This pull all the necessary data files and calculate the points for the depositors. Takes around two hours in its current state.

### Start the API server

Start the server:

```shell
make server
```

Test the API at the following endpoint: <http://0.0.0.0:8000/tvl/{user_id}>

Example: <http://0.0.0.0:8000/tvl/0x6C53339048AE6B7de97440a38DF3164B4f456650>

Can also access it via the docs page: <http://0.0.0.0:8000/docs#/default/get_tvl_tvl__user_id__get>

## ETL Overview

The code relies mostly on TheGraph subgraphs for aave-related information (e.g. user balances, reserves and assets, reserve liquidity indexes). It pulls asset prices from Coingecko.

### TVL Calculation

The code calculates depositor TVL based on the aToken assets held in each depositor's reserves.

For each reserve and for each day within the examined period (over the month of September) it calculates the daily reserve TVL:

`scaled balance` x `liquidity index` x `market price` x `day fraction`

A user's final TVL is the sum of their daily TVLs over all their reserves.

### Possible Improvement & Extensions

- Pull data for other networks (currently works only on Ethereum mainnet)
- Examine possible differences from Aave V2 protocol transactions
- Filter for recursive looping
- Price data is not available for all assets for all the dates (e.g. cbBTC)
- Liquidity index data is not available for all assets & dates (e.g. Stargate Token)
- Unit & integration tests
- Explicitly manage precision during calculation and for the Coingecko price feed
- Use pydantic/pandera for explicit schema validation
- Store data in database
- Improve data pull and process performance
