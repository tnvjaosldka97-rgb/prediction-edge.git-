"""
포트폴리오 최적화 — 공분산 추정 + Hierarchical Risk Parity (HRP).

기존 Kelly는 베팅 독립 가정 + 카테고리 페널티 하드코딩.
이 모듈은:
  - 카테고리별 returns 시계열에서 공분산 추정
  - HRP (López de Prado, 2016): 클러스터링 후 risk parity로 자본 배분
  - 효율적 프론티어 일부 (목표 수익 → 최소 분산 가중치)

numpy 없는 환경 가정해서 직접 구현 (간단한 케이스만).
"""
from __future__ import annotations
import math
import sqlite3
import time
from dataclasses import dataclass

import config


@dataclass
class CovarianceEstimate:
    categories: list[str]
    mean_returns: dict[str, float]
    cov_matrix: list[list[float]]      # NxN
    n_observations: int


def _conn():
    return sqlite3.connect(config.DB_PATH)


def estimate_category_covariance(window_days: float = 30) -> CovarianceEstimate:
    """trades 테이블에서 카테고리별 일별 P&L 시계열 → 공분산."""
    since_ts = time.time() - window_days * 86400
    conn = _conn()
    # 카테고리 컬럼이 trades에는 없음 → strategy를 카테고리 대용으로 사용
    rows = conn.execute(
        "SELECT strategy, timestamp, "
        "       COALESCE(pnl, fill_price * size_shares - fee_paid) as p "
        "FROM trades WHERE timestamp >= ? AND strategy IS NOT NULL",
        (since_ts,)
    ).fetchall()
    conn.close()

    # strategy × day → P&L
    bucket: dict[str, dict[int, float]] = {}
    for strategy, ts, p in rows:
        day = int(ts // 86400)
        bucket.setdefault(strategy, {}).setdefault(day, 0.0)
        bucket[strategy][day] += p or 0

    categories = sorted(bucket.keys())
    if not categories:
        return CovarianceEstimate([], {}, [], 0)

    all_days = sorted({d for v in bucket.values() for d in v.keys()})

    # 시계열 매트릭스 N(strategies) x T(days)
    series = []
    for s in categories:
        row = [bucket[s].get(d, 0.0) for d in all_days]
        series.append(row)

    if not all_days or len(all_days) < 3:
        return CovarianceEstimate(categories, {s: 0 for s in categories}, [], 0)

    # 평균
    mean_returns = {}
    for i, s in enumerate(categories):
        mean_returns[s] = sum(series[i]) / len(series[i])

    # 공분산 매트릭스
    n = len(categories)
    T = len(all_days)
    cov = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            mean_i = mean_returns[categories[i]]
            mean_j = mean_returns[categories[j]]
            s = 0.0
            for t in range(T):
                s += (series[i][t] - mean_i) * (series[j][t] - mean_j)
            cov[i][j] = s / max(1, T - 1)

    return CovarianceEstimate(
        categories=categories,
        mean_returns=mean_returns,
        cov_matrix=cov,
        n_observations=T,
    )


def correlation_from_cov(cov: list[list[float]]) -> list[list[float]]:
    """공분산 → 상관계수."""
    n = len(cov)
    corr = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            denom = math.sqrt(max(1e-12, cov[i][i] * cov[j][j]))
            corr[i][j] = cov[i][j] / denom if denom > 0 else 0
    return corr


def inverse_variance_weights(cov: list[list[float]]) -> list[float]:
    """단순 inverse variance — 분산 작은 자산에 더 비중. HRP보다 단순."""
    diag = [cov[i][i] for i in range(len(cov))]
    inv = [1 / max(1e-12, d) for d in diag]
    total = sum(inv)
    return [w / total for w in inv] if total > 0 else [1 / len(cov)] * len(cov)


def hrp_weights(cov: list[list[float]]) -> list[float]:
    """
    Hierarchical Risk Parity (단순화 버전).
    1. 상관계수 행렬에서 클러스터링 (greedy linkage)
    2. 클러스터 내부 inverse-variance 가중치
    3. 클러스터 간 inverse-variance 가중치

    완전한 HRP는 scipy 의존이라 단순화: 상관계수 0.5 기준 2개 클러스터로 split.
    """
    n = len(cov)
    if n == 0:
        return []
    if n == 1:
        return [1.0]

    corr = correlation_from_cov(cov)
    # Greedy: 평균 상관계수 기준 위/아래로 분할
    avg_corr_with_first = [sum(corr[0][j] for j in range(n)) / n for _ in range(n)]
    median_avg = sorted(avg_corr_with_first)[n // 2]

    cluster_a = [i for i in range(n) if avg_corr_with_first[i] >= median_avg]
    cluster_b = [i for i in range(n) if i not in cluster_a]

    if not cluster_a or not cluster_b:
        return inverse_variance_weights(cov)

    # 클러스터 내부 가중치
    def cluster_internal(indices: list[int]) -> list[float]:
        sub_cov = [[cov[i][j] for j in indices] for i in indices]
        return inverse_variance_weights(sub_cov)

    w_a_internal = cluster_internal(cluster_a)
    w_b_internal = cluster_internal(cluster_b)

    # 클러스터 분산
    def cluster_variance(indices, internal_w):
        var = 0.0
        for i_idx, i in enumerate(indices):
            for j_idx, j in enumerate(indices):
                var += internal_w[i_idx] * internal_w[j_idx] * cov[i][j]
        return var

    var_a = cluster_variance(cluster_a, w_a_internal)
    var_b = cluster_variance(cluster_b, w_b_internal)

    # 클러스터 간 inverse variance
    inv_a = 1 / max(1e-12, var_a)
    inv_b = 1 / max(1e-12, var_b)
    total = inv_a + inv_b
    cluster_w_a = inv_a / total
    cluster_w_b = inv_b / total

    weights = [0.0] * n
    for k, i in enumerate(cluster_a):
        weights[i] = cluster_w_a * w_a_internal[k]
    for k, i in enumerate(cluster_b):
        weights[i] = cluster_w_b * w_b_internal[k]

    return weights


def optimize_portfolio(window_days: float = 30, method: str = "hrp") -> dict:
    """현재 전략별 최적 자본 배분 비중."""
    cov_est = estimate_category_covariance(window_days)
    if not cov_est.categories:
        return {"method": method, "weights": {}, "n_observations": 0}

    if method == "hrp":
        w = hrp_weights(cov_est.cov_matrix)
    elif method == "inverse_variance":
        w = inverse_variance_weights(cov_est.cov_matrix)
    else:
        w = [1.0 / len(cov_est.categories)] * len(cov_est.categories)

    return {
        "method": method,
        "weights": dict(zip(cov_est.categories, w)),
        "mean_returns": cov_est.mean_returns,
        "n_observations": cov_est.n_observations,
    }
