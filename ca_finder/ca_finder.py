# -*- coding: utf-8 -*-

import pandas as pd
import polars as pl
import json
import requests
import re
import os
from functools import lru_cache
from enum import Enum
from pathlib import Path
from tqdm import tqdm
from time import sleep
from datetime import timedelta


data = pd.read_csv(Path(__file__).parent / "data" / "all_kol_tweets.csv")
data["CreatedAt"] = pd.to_datetime(data["CreatedAt"])


BIRD_KEY = open(os.path.expanduser("~/.birdeye_key")).read().strip()


class Chain(Enum):
    SOLANA = "solana"
    ETHEREUM = "ethereum"
    SUI = "sui"


class Confidence(Enum):
    InTweet = "in tweet"
    InUser = "in user"
    HasTicker = "has ticker"


@lru_cache(maxsize=None)
def query_birdeye_api_ticker(address: str, chain: Chain):
    # if len(address_list) == 1:
    url = "https://public-api.birdeye.so/defi/token_overview"
    payload = address
    params = {
        "address": payload
    }

    headers = {
        "accept": "application/json",
        "x-chain": chain.value,
        "X-API-KEY": BIRD_KEY
    }

    # Print full request for debugging purposes
    # print(f"Request: {url} with params: {params}")
    response = requests.get(url, params=params, headers=headers)

    try:
        data = response.json()
        if not data.get("success", True):
            raise ValueError(f"error msg: {data.get('message')}, Chain: {chain}, Payload: {payload}")
        return data
    except ValueError:
        print("Response content is not valid JSON:", response.text)


# Example usage:
addresses = [
    "0x92b2927fb83c0f5925598dc333dfa8dab0ea02a3",
    "So11111111111111111111111111111111111111112",
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So"
]

query_birdeye_api_ticker(addresses[0], Chain.ETHEREUM)["data"]["symbol"]
query_birdeye_api_ticker(addresses[1], Chain.SOLANA)["data"]["symbol"]

data = pl.DataFrame(data)

# --- Updated regex patterns without look-around (since Polars' regex engine doesn’t support them) ---
evm_re = r"\b0x[a-fA-F0-9]{40}\b"
sui_strict_re = r"\b0x[a-fA-F0-9]{64}\b"
sol_re = r"\b[1-9A-HJ-NP-Za-km-z]{43,45}\b"


class Confidence(Enum):
    HasTicker = "HasTicker"
    InUser = "InUser"
    InTweet = "InTweet"


# --- Helper Function ---
def handle_content(contents: list[str], pattern: str):
    ret = []
    for c in contents:
        found = re.findall(pattern, c)
        if found:
            ret += found
    return ret


def eager_df(df):
    return df.collect() if isinstance(df, pl.LazyFrame) else df


def find_relevant_adderess_outside_user_context(ticker: str, created_at, days: int = 30):
    """
    Searches the entire dataset (outside the user context) for tweets that mention
    the ticker and contain one of the blockchain address patterns.
    The search is limited to tweets whose CreatedAt date is within `days` of the given `created_at`.
    """
    # Ensure we are working with an eager DataFrame.
    df = eager_df(data)

    threshold = timedelta(days=days)
    # Create a regex pattern for the ticker.
    ticker_pattern = fr"(?:^|\s|$){re.escape(ticker)}(?:^|\s|$)"

    tweets_with_token = df.filter(
        pl.col("Content").str.contains(ticker_pattern)
    )

    # Convert threshold and the reference datetime to nanoseconds.
    threshold_ns = int(threshold.total_seconds() * 1e9)
    created_at_ns = int(created_at.timestamp() * 1e9)

    # Filter by CreatedAt: cast to Int64 (nanoseconds), subtract the reference, then take abs() and compare.
    tweets_with_token = tweets_with_token.filter(
        (pl.col("CreatedAt").cast(pl.Int64) - created_at_ns).abs() <= threshold_ns
    )

    # Filter further based on the address regexes.
    all_user_sols = tweets_with_token.filter(
        pl.col("Content").str.contains(sol_re)
    ).select(["Content", "TweetID"])

    all_user_evms = tweets_with_token.filter(
        pl.col("Content").str.contains(evm_re)
    ).select(["Content", "TweetID"])

    all_user_suis = tweets_with_token.filter(
        pl.col("Content").str.contains(sui_strict_re)
    ).select(["Content", "TweetID"])

    if all_user_sols.height > 0:
        return (handle_content(all_user_sols["Content"].to_list(), sol_re),
                Chain.SOLANA,
                Confidence.HasTicker,
                all_user_sols)
    elif all_user_evms.height > 0:
        return (handle_content(all_user_evms["Content"].to_list(), evm_re),
                Chain.ETHEREUM,
                Confidence.HasTicker,
                all_user_evms)
    elif all_user_suis.height > 0:
        return (handle_content(all_user_suis["Content"].to_list(), sui_strict_re),
                Chain.SUI,
                Confidence.HasTicker,
                all_user_suis)

    return None


def find_relevant_adderess_outside_content(ticker: str, user_id: str, created_at):
    """
    Searches within tweets from the specified user for addresses when none
    are found in the tweet content.
    """
    df = eager_df(data)

    ticker_pattern = fr"(?:^|\s|$){re.escape(ticker)}(?:^|\s|$)"
    tweets_with_token = df.filter(
        pl.col("Content").str.contains(ticker_pattern)
    )

    # Filter tweets belonging to the specified user.
    user_tweets = tweets_with_token.filter(
        pl.col("TwitterUsername") == user_id
    )

    user_sols = user_tweets.filter(
        pl.col("Content").str.contains(sol_re)
    ).select(["Content", "TweetID"])

    user_evms = user_tweets.filter(
        pl.col("Content").str.contains(evm_re)
    ).select(["Content", "TweetID"])

    user_suis = user_tweets.filter(
        pl.col("Content").str.contains(sui_strict_re)
    ).select(["Content", "TweetID"])

    if user_sols.height > 0:
        return (handle_content(user_sols["Content"].to_list(), sol_re),
                Chain.SOLANA,
                Confidence.InUser,
                user_sols)
    elif user_evms.height > 0:
        return (handle_content(user_evms["Content"].to_list(), evm_re),
                Chain.ETHEREUM,
                Confidence.InUser,
                user_evms)
    elif user_suis.height > 0:
        return (handle_content(user_suis["Content"].to_list(), sui_strict_re),
                Chain.SUI,
                Confidence.InUser,
                user_suis)

    # Fall back to the full dataset search.
    return find_relevant_adderess_outside_user_context(ticker, created_at, days=30)


def find_relevant_adderess(tweet_content: str, ticker: str, user_id: str, created_at):
    """
    First, checks if the given tweet content contains any addresses.
    If not, falls back to searching within the user's tweets.
    """
    evms = re.findall(evm_re, tweet_content)
    suis = re.findall(sui_strict_re, tweet_content)
    sols = re.findall(sol_re, tweet_content)

    # Create a one-row DataFrame with tweet meta-data.
    tweet_meta_df = pl.DataFrame([{"TweetID": "", "Content": tweet_content}])

    if sols:
        return sols, Chain.SOLANA, Confidence.InTweet, tweet_meta_df
    elif evms:
        return evms, Chain.ETHEREUM, Confidence.InTweet, tweet_meta_df
    elif suis:
        return suis, Chain.SUI, Confidence.InTweet, tweet_meta_df

    # Fallback to the user tweet search.
    return find_relevant_adderess_outside_content(ticker, user_id, created_at)


# --- Example Usage ---

# (Even if your original data was a LazyFrame, the functions now ensure it is collected.)
created_at_value = data["CreatedAt"].to_list()[2]           # third row's CreatedAt
twitter_username_value = data["TwitterUsername"].to_list()[-1]  # last row's TwitterUsername

result1 = find_relevant_adderess_outside_user_context("$SOL", created_at_value, 100)
result2 = find_relevant_adderess_outside_content("$SOL", twitter_username_value, created_at_value)
tweet_content_example = "Check out $SOL and 3P9j73U3dXqPj6Nf6Q3KsZK7HrpJ87YPYUL9rWZkUDXcL8sNj!"
result3 = find_relevant_adderess(tweet_content_example, "$SOL", twitter_username_value, created_at_value)

print("Result 1 (Outside user context):")
addresses, chain, conf, match_df = result1
assert addresses == ['5MfwpEF6XPBDaBBGsiEviNe8sMeF7DZCdQeC5mdrP1pt', '79uMpZYpTVVB15FuDmGubUSWbmyMEm4dZXwbFgcA7uN5', '79uMpZYpTVVB15FuDmGubUSWbmyMEm4dZXwbFgcA7uN5']
assert chain == Chain.SOLANA
assert conf == Confidence.HasTicker
for addr in addresses:
  assert any(addr in tweet for tweet in match_df["Content"].to_list())
print(f"Match DataFrame:\n{match_df}")
print("\nResult 2 (Inside user context):")

addresses, chain, conf, match_df = result2

assert addresses == ['F7mJkAQToYB61vcusXmgZSCQ8n6mTSD2AsG1RYC2xHFn', 'BcQT21yyc3ray8aojb7qMtMn1884mTshqQgUqut6pump'], addresses
assert chain == Chain.SOLANA
assert conf == Confidence.InUser
for addr in addresses:
    assert any(addr in tweet for tweet in match_df["Content"].to_list())
print(f"Match DataFrame:\n{match_df}")

print("\nResult 3 (From tweet content):")
addresses, chain, conf, match_df = result3
assert addresses == ['F7mJkAQToYB61vcusXmgZSCQ8n6mTSD2AsG1RYC2xHFn', 'BcQT21yyc3ray8aojb7qMtMn1884mTshqQgUqut6pump'], addresses
assert chain
assert conf
for addr in addresses:
    assert any(addr in tweet for tweet in match_df["Content"].to_list())
print(f"Match DataFrame:\n{match_df}")


def dump_founds(founds):
    dump = f"founds_{len(founds)}.json"
    with open(dump, "w") as f:
        json.dump(founds, f, sort_keys=True, indent=2)


def process_row(row):
    """
    Process a single row (provided as a dictionary) and return any found results.
    """
    founds_local = {}
    tweet_id = row["TweetID"]
    token_re = r"\$[A-Z]{2,10}"
    content = row["Content"]
    tokens = re.findall(token_re, content)
    created = row["CreatedAt"]

    for t in tokens:
        # Skip tokens we don't want to process
        if t in ["$SOL", "$BTC", "$ETH"]:
            continue

        # Find relevant address (user-defined function)
        found = find_relevant_adderess(content, t, row["TwitterUsername"], created_at=created)
        if found is None:
            continue

        addresses, chain, conf, match_df = found
        sleep(0.1)  # to not overwhelm the API (remove if unnecessary)

        # Loop over unique addresses
        for i, addr in enumerate(addresses):
            api_ret = query_birdeye_api_ticker(addr, chain)
            if not (api_ret and api_ret.get("success")):
                print(f"Error..., {addr}, {chain}, err: {api_ret}")
                continue
            if "symbol" in api_ret["data"]:
                # Check that the API returned the expected token symbol (removing the $)
                if api_ret["data"]["symbol"] == t[1:]:
                    # Save the result using tweet_id as key.
                    founds_local[tweet_id] = {
                        t: {
                            "api_match": api_ret["data"]["symbol"],
                            "address": addr,
                            "chain": chain.value,  # assuming chain is an Enum
                            "conf": conf.value,    # assuming conf is an Enum
                            "match_tweed_id": str(match_df["TweetID"][0]),
                            "match_tweet_content": str(match_df["Content"][0]),
                            "original_content": content
                        }
                    }
                    json.dumps(founds_local) # make sure it parses
    return founds_local


def main():
    data = pd.read_csv(Path(__file__).parent / "data" / "all_kol_tweets.csv")
    data["CreatedAt"] = pd.to_datetime(data["CreatedAt"])
    # IMPORTANT: if 'data' is a pandas DataFrame, convert rows to a list of dictionaries.
    data = pl.DataFrame(data)
    rows = data.to_dicts()
    combined_founds = {}
    for row in tqdm(rows):
        result = process_row(row)
        combined_founds.update(result)

        # if result and len(combined_founds) % 10_000 == 0:
        #     print(f"found: {len(combined_founds)} items.")
        #     dump_founds(combined_founds)

    # Dump the final combined results.
    dump_founds(combined_founds)
    print(f"All Done, found: {len(combined_founds)} items.")


if __name__ == "__main__":
    main()
