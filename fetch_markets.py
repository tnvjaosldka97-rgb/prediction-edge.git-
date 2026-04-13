import httpx
import json

def get_markets():
    url = "https://clob.polymarket.com/markets"
    params = {"limit": 10, "active": "true"}

    response = httpx.get(url, params=params)
    markets = response.json()

    for market in markets.get("data", []):
        print(f"시장: {market.get('question', 'N/A')}")
        print(f"  토큰: {market.get('tokens', [])}")
        print(f"  활성: {market.get('active', False)}")
        print("-" * 60)

if __name__ == "__main__":
    get_markets()
