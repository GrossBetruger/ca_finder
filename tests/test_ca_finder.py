import pandas as pd


from ca_finder.ca_finder import Chain, Confidence, find_relevant_adderess
from ca_finder.ca_finder import find_relevant_adderess_outside_user_context
from ca_finder.ca_finder import find_relevant_adderess_outside_content
from ca_finder.ca_finder import query_birdeye_api_ticker


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


def test_bird_api():
    ca_address_1 = "0xBADFf0eF41D2A68F22De21EAbca8A59Aaf495cF0"
    result1 = query_birdeye_api_ticker(ca_address_1, Chain.ETHEREUM)
    assert result1 is not None
    assert result1["success"] is True
    assert "data" in result1
    assert "address" in result1["data"]
    assert result1["data"]["address"] == ca_address_1
    assert result1["data"]["symbol"] == "KABOSU"

    ca_address_2 = "0xDe5e66482A436fd437dE2cdEAc39aA212F0F10C1"
    result2 = query_birdeye_api_ticker(ca_address_2, Chain.ETHEREUM)
    assert result2 is not None
    assert result2["success"] is True
    assert "data" in result2
    assert "address" in result2["data"]
    assert result2["data"]["address"] == ca_address_2
    assert result2["data"]["symbol"] == "FLOKI"

    ca_address_3 = "7BPAjesYS2FCY84gzBmMbukFQqC6rLgtYP9twMvji6Pr"
    result3 = query_birdeye_api_ticker(ca_address_3, Chain.SOLANA)
    assert result3 is not None
    assert result3["success"] is True
    assert "data" in result3
    assert "address" in result3["data"]
    assert result3["data"]["address"] == ca_address_3
    assert result3["data"]["symbol"] == "SHIB"
