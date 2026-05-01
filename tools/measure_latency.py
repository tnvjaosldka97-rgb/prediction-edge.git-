"""
실제 네트워크 latency 베이스라인 측정.

각 endpoint 100회 ping → mean/p50/p95/p99 분포.
이게 우리가 라이브 가는 환경에서 봇이 직면할 진짜 latency.

측정 대상:
- Polymarket Gamma API (마켓 메타 — REST GET)
- Polymarket CLOB API (호가 — REST GET)
- Polygon RPC (chain_id 호출 — JSON-RPC)
- Anthropic API (헬스체크)
"""
import asyncio
import statistics
import time
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import config


async def measure_endpoint(name: str, fn, n: int = 100):
    """fn() async 함수 N회 호출 → 통계."""
    import httpx
    latencies_ms = []
    errors = 0
    for i in range(n):
        t0 = time.time()
        try:
            await fn()
            latencies_ms.append((time.time() - t0) * 1000)
        except Exception:
            errors += 1
        await asyncio.sleep(0.05)    # 50ms 간격 — rate limit 방어

    if not latencies_ms:
        return {"name": name, "n": 0, "errors": errors, "p50": 0, "p95": 0, "p99": 0, "mean": 0}

    return {
        "name": name,
        "n": len(latencies_ms),
        "errors": errors,
        "mean": statistics.mean(latencies_ms),
        "median": statistics.median(latencies_ms),
        "p50": sorted(latencies_ms)[int(len(latencies_ms) * 0.50)],
        "p95": sorted(latencies_ms)[int(len(latencies_ms) * 0.95)],
        "p99": sorted(latencies_ms)[int(len(latencies_ms) * 0.99)] if len(latencies_ms) >= 100 else max(latencies_ms),
        "min": min(latencies_ms),
        "max": max(latencies_ms),
    }


async def gamma_api_call():
    import httpx
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://gamma-api.polymarket.com/markets?limit=1")
        r.raise_for_status()


async def clob_api_call():
    import httpx
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get("https://clob.polymarket.com/markets?next_cursor=&limit=1")
        r.raise_for_status()


async def polygon_rpc_call():
    import httpx
    rpc = config.POLYGON_RPC
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(rpc, json={
            "jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 1
        })
        r.raise_for_status()


async def main():
    print("=" * 60)
    print("  Network latency baseline (100 reqs each)")
    print("=" * 60)
    print(f"  POLYGON_RPC: {config.POLYGON_RPC[:50]}...")
    print()

    tests = [
        ("Polymarket Gamma API", gamma_api_call),
        ("Polymarket CLOB API", clob_api_call),
        ("Polygon RPC (chain_id)", polygon_rpc_call),
    ]

    for name, fn in tests:
        print(f"Measuring {name}...")
        result = await measure_endpoint(name, fn, n=100)
        print(f"  N: {result['n']}, errors: {result['errors']}")
        print(f"  mean: {result['mean']:.1f}ms")
        print(f"  median: {result['median']:.1f}ms")
        print(f"  p95: {result['p95']:.1f}ms")
        print(f"  p99: {result['p99']:.1f}ms")
        print(f"  min/max: {result['min']:.1f}ms / {result['max']:.1f}ms")
        print()

    print("=" * 60)
    print("해석:")
    print("  - 평균 < 200ms: 좋음 (우리 default friction와 비슷)")
    print("  - 평균 200~500ms: 우리 모델 약간 낙관적")
    print("  - 평균 > 500ms: AWS Seoul 등으로 옮겨야 함")
    print("  - p99: 꼬리 분포. 1% 케이스에서 timeout 나는지")
    print()


if __name__ == "__main__":
    asyncio.run(main())
