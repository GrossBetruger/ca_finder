import pandas as pd


from ca_finder.ca_finder import Chain, Confidence, find_relevant_adderess
from ca_finder.ca_finder import find_relevant_adderess_outside_user_context
from ca_finder.ca_finder import find_relevant_adderess_outside_content


def test_ca_finder():
    created_at_value = pd.to_datetime("2023-01-01 00:23:46+00:00")
    twitter_username_value = "0xprimdotfun"
    result1 = find_relevant_adderess_outside_user_context(
        "$SOL", 
        created_at_value, 
        100
    )
    result2 = find_relevant_adderess_outside_content(
        "$SOL",
        twitter_username_value, 
        created_at_value
    )
    tweet_content_example = (
        "Check out $SOL and "
        "3P9j73U3dXqPj6Nf6Q3KsZK7HrpJ87YPYUL9rWZkUDXcL8sNj!"
    )
    result3 = find_relevant_adderess(
        tweet_content_example,
        "$SOL",
        twitter_username_value,
        created_at_value
    )

    print("Result 1 (Outside user context):")
    addresses, chain, conf, match_df = result1
    assert addresses == [
        '5MfwpEF6XPBDaBBGsiEviNe8sMeF7DZCdQeC5mdrP1pt',
        '79uMpZYpTVVB15FuDmGubUSWbmyMEm4dZXwbFgcA7uN5', 
        '79uMpZYpTVVB15FuDmGubUSWbmyMEm4dZXwbFgcA7uN5'
    ]
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
