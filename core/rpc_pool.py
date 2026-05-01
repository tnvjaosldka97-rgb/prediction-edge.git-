"""
Multi-RPC failover pool.

단일 RPC가 죽으면 자동으로 다음 노드로 회전. 라이브 운영 중 RPC outage 방어.

POLYGON_RPC가 단일 URL이면 그것만 사용.
POLYGON_RPC_POOL이 ","로 구분된 여러 URL이면 라운드로빈 + circuit breaker.

각 RPC별 fail count 추적. 5회 연속 실패 시 5분간 격리.
"""
from __future__ import annotations
import os
import time
from typing import Optional


class RpcPool:
    DEFAULT_PUBLIC = [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon-rpc.com",
        "https://rpc.ankr.com/polygon",
        "https://1rpc.io/matic",
        "https://polygon.llamarpc.com",
    ]

    def __init__(self, urls: list[str] | None = None):
        if urls is None:
            urls = self._load_from_env()
        self.urls = urls or self.DEFAULT_PUBLIC
        self._fails: dict[str, int] = {u: 0 for u in self.urls}
        self._isolated_until: dict[str, float] = {u: 0 for u in self.urls}
        self._idx = 0

    @classmethod
    def _load_from_env(cls) -> list[str]:
        primary = os.getenv("POLYGON_RPC", "").strip()
        pool = os.getenv("POLYGON_RPC_POOL", "").strip()
        urls = []
        if pool:
            urls.extend([u.strip() for u in pool.split(",") if u.strip()])
        elif primary:
            urls.append(primary)
        return urls

    def get(self) -> str:
        """현재 활성 URL 반환. 격리된 노드는 스킵."""
        now = time.time()
        n = len(self.urls)
        for _ in range(n):
            url = self.urls[self._idx % n]
            if self._isolated_until[url] <= now:
                return url
            self._idx += 1
        # 전부 격리됐으면 가장 빨리 풀리는 거
        url = min(self.urls, key=lambda u: self._isolated_until[u])
        return url

    def report_failure(self, url: str) -> None:
        self._fails[url] = self._fails.get(url, 0) + 1
        if self._fails[url] >= 5:
            self._isolated_until[url] = time.time() + 300  # 5분 격리
            self._fails[url] = 0
            self._idx += 1  # 다음 노드로

    def report_success(self, url: str) -> None:
        self._fails[url] = 0

    def status(self) -> list[dict]:
        now = time.time()
        return [
            {
                "url": u,
                "fails": self._fails.get(u, 0),
                "isolated_for_sec": max(0, self._isolated_until.get(u, 0) - now),
            }
            for u in self.urls
        ]


_pool: Optional[RpcPool] = None


def get_pool() -> RpcPool:
    global _pool
    if _pool is None:
        _pool = RpcPool()
    return _pool


def get_rpc_url() -> str:
    return get_pool().get()


def call_with_failover(method_name: str, *args, **kwargs):
    """web3 호출을 failover 풀로 wrapping. 단일 메서드 호출 retry.

    사용 예:
        block = call_with_failover('eth.block_number')
        balance = call_with_failover('eth.get_balance', '0x...')
    """
    from web3 import Web3
    pool = get_pool()
    last_exc = None
    for _ in range(len(pool.urls)):
        url = pool.get()
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 5}))
            obj = w3
            for part in method_name.split("."):
                obj = getattr(obj, part)
            if callable(obj):
                result = obj(*args, **kwargs)
            else:
                result = obj
            pool.report_success(url)
            return result
        except Exception as e:
            last_exc = e
            pool.report_failure(url)
            continue
    raise last_exc or RuntimeError("all RPCs failed")


def get_w3():
    """현재 활성 RPC로 Web3 인스턴스 생성. 호출자가 try/except 처리."""
    from web3 import Web3
    return Web3(Web3.HTTPProvider(get_rpc_url(), request_kwargs={"timeout": 10}))
