"""
Latency model — submit_ts → fill_ts 분포.

Polymarket CLOB 관측 분포는 log-normal에 가까움
(짧은 RTT가 다수, 가끔 긴 꼬리). p_timeout으로 진짜 죽는 경우 처리.
"""
from __future__ import annotations
import math
import random
from dataclasses import dataclass


@dataclass
class LatencySample:
    delay_ms: float
    timed_out: bool


class LatencyModel:
    """
    Default 200ms는 USA/EU 콜로 환경 가정값이었는데,
    2026-04-30 한국 PC 실측 결과 Polymarket CLOB API ~600ms.
    → mu=500, sigma=200으로 업데이트 (Railway에서 자동 캘리브레이션이
       라이브 trace 누적 후 더 정확한 값으로 갱신).
    """
    def __init__(
        self,
        mu_ms: float = 500.0,
        sigma_ms: float = 200.0,
        p_timeout: float = 0.01,
        timeout_ms: float = 10_000.0,
    ):
        self.mu_ms = mu_ms
        self.sigma_ms = sigma_ms
        self.p_timeout = p_timeout
        self.timeout_ms = timeout_ms

    def sample(self, rng: random.Random | None = None) -> LatencySample:
        r = rng or random
        if r.random() < self.p_timeout:
            return LatencySample(self.timeout_ms, timed_out=True)
        # log-normal: 평균이 mu_ms가 되도록 파라미터 조정
        sigma_log = math.log(1 + (self.sigma_ms / self.mu_ms) ** 2) ** 0.5
        mu_log = math.log(self.mu_ms) - sigma_log ** 2 / 2
        delay = math.exp(r.gauss(mu_log, sigma_log))
        return LatencySample(min(delay, self.timeout_ms), timed_out=False)

    def calibrate(self, observed_ms: list[float]) -> None:
        """라이브 trace로 μ, σ 갱신 (timeout 샘플은 제외)."""
        valid = [x for x in observed_ms if 0 < x < self.timeout_ms]
        if len(valid) < 10:
            return
        log_obs = [math.log(x) for x in valid]
        mu_log = sum(log_obs) / len(log_obs)
        var_log = sum((x - mu_log) ** 2 for x in log_obs) / len(log_obs)
        sigma_log = var_log ** 0.5
        # 역변환
        self.mu_ms = math.exp(mu_log + sigma_log ** 2 / 2)
        self.sigma_ms = self.mu_ms * (math.exp(sigma_log ** 2) - 1) ** 0.5
        # timeout 빈도
        n_timeout = sum(1 for x in observed_ms if x >= self.timeout_ms)
        self.p_timeout = n_timeout / len(observed_ms) if observed_ms else self.p_timeout

    def to_dict(self) -> dict:
        return {
            "mu_ms": self.mu_ms,
            "sigma_ms": self.sigma_ms,
            "p_timeout": self.p_timeout,
            "timeout_ms": self.timeout_ms,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "LatencyModel":
        return cls(**d)
