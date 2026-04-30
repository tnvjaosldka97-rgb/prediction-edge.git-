"""
Walk-forward + Purged Cross-Validation 백테스트 프레임워크.

핵심 원칙 (López de Prado, "Advances in Financial ML"):
1. **시간 누수 금지**: train/test 사이 buffer (purge)로 IID 가정 거짓 차단
2. **Walk-forward**: 시간 순으로 expanding/sliding window
3. **Combinatorial Purged CV (CPCV)**: 다중 train/test 조합으로 분산 측정
4. **Embargo**: test 이후 일정 시간 train 불가 (정보가 흘러들 수 있는 시간)

기존 realistic_engine.py는 "전체 기간 한 번에 평가" → look-ahead 의심.
이 모듈은 "미래 정보 절대 못 보는" 엄격한 평가.
"""
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Callable, Optional
from itertools import combinations


@dataclass
class FoldResult:
    fold_idx: int
    train_start: float
    train_end: float
    test_start: float
    test_end: float
    n_train: int
    n_test: int
    metric: float          # PnL, Sharpe, etc.


@dataclass
class CVReport:
    fold_results: list[FoldResult]
    mean_metric: float
    std_metric: float
    sharpe_of_metrics: float    # 폴드별 metric 분산 → 일관성 지표
    p_value: float              # mean > 0 검정


def _sharpe(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    std = math.sqrt(var)
    return mean / std if std > 0 else 0.0


def _t_stat(values: list[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    se = math.sqrt(var / n)
    return mean / se if se > 0 else 0.0


def _t_to_p_two_tailed(t: float, df: int) -> float:
    """Student's t → 2-tailed p-value. df 큰 경우 정규근사."""
    if df < 1:
        return 1.0
    # df > 30이면 정규근사 (충분히 정확)
    if df >= 30:
        from math import erf, sqrt
        z = abs(t)
        p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
        return p
    # 작은 df: 보수적으로 z ≈ t로 처리하되 df 보정
    # 정확한 t-CDF는 외부 의존 없이 어려우니 근사
    z = abs(t) * (1 - 1 / (4 * df))   # Welch-Satterthwaite 근사 단순화
    from math import erf, sqrt
    p = 2 * (1 - 0.5 * (1 + erf(z / sqrt(2))))
    return min(1.0, p)


def walk_forward_split(
    n_periods: int,
    train_window: int,
    test_window: int,
    step: int,
    embargo: int = 0,
) -> list[tuple[int, int, int, int]]:
    """
    n_periods: 전체 시간 단위 수
    train_window: train 폭 (단위 수)
    test_window: test 폭
    step: 슬라이딩 간격
    embargo: train과 test 사이 버퍼 (시간 누수 방지)

    Returns: [(train_start, train_end, test_start, test_end), ...]
    """
    splits = []
    pos = 0
    while pos + train_window + embargo + test_window <= n_periods:
        train_start = pos
        train_end = pos + train_window
        test_start = train_end + embargo
        test_end = test_start + test_window
        splits.append((train_start, train_end, test_start, test_end))
        pos += step
    return splits


def purged_kfold_split(
    n_periods: int,
    k: int,
    purge_periods: int = 0,
    embargo_periods: int = 0,
) -> list[tuple[list[int], list[int]]]:
    """
    Purged K-Fold CV.
    각 fold는 test 구간 양옆을 train에서 제거 (purge) + 뒤쪽 embargo.

    Returns: [(train_indices, test_indices), ...]
    """
    fold_size = n_periods // k
    splits = []
    for i in range(k):
        test_start = i * fold_size
        test_end = (i + 1) * fold_size if i < k - 1 else n_periods
        purge_start = max(0, test_start - purge_periods)
        purge_end = min(n_periods, test_end + embargo_periods)

        train_indices = [j for j in range(n_periods) if j < purge_start or j >= purge_end]
        test_indices = list(range(test_start, test_end))
        splits.append((train_indices, test_indices))
    return splits


def combinatorial_purged_cv(
    n_periods: int,
    n_groups: int,
    n_test_groups: int,
    purge_periods: int = 0,
    embargo_periods: int = 0,
) -> list[tuple[list[int], list[int]]]:
    """
    CPCV — N-group split, 각 조합에 n_test_groups 만큼 test.
    polynomial 수의 폴드 → 더 강건한 분산 추정.

    예: n_groups=6, n_test_groups=2 → C(6,2) = 15 폴드.
    """
    group_size = n_periods // n_groups
    boundaries = [(i * group_size, (i + 1) * group_size if i < n_groups - 1 else n_periods)
                  for i in range(n_groups)]

    splits = []
    for test_combo in combinations(range(n_groups), n_test_groups):
        test_indices = []
        purge_intervals = []
        for tg in test_combo:
            ts, te = boundaries[tg]
            test_indices.extend(range(ts, te))
            purge_intervals.append((max(0, ts - purge_periods), min(n_periods, te + embargo_periods)))
        train_indices = []
        for j in range(n_periods):
            if j in test_indices:
                continue
            if any(s <= j < e for s, e in purge_intervals):
                continue
            train_indices.append(j)
        splits.append((train_indices, test_indices))
    return splits


def evaluate_strategy(
    data: list,                         # 시간 정렬된 ticks/events
    strategy_fn: Callable,              # (train_data, test_data) -> per-test-period PnL list
    cv_method: str = "walk_forward",
    cv_params: Optional[dict] = None,
) -> CVReport:
    """전략을 CV로 평가.

    strategy_fn(train, test) -> list[float]: test 기간 동안의 trade-by-trade 또는 per-period PnL.
    """
    cv_params = cv_params or {}
    n = len(data)

    if cv_method == "walk_forward":
        splits = walk_forward_split(
            n,
            train_window=cv_params.get("train_window", n // 4),
            test_window=cv_params.get("test_window", n // 8),
            step=cv_params.get("step", n // 8),
            embargo=cv_params.get("embargo", 0),
        )
        # walk_forward returns 4-tuples — convert to (train_idx, test_idx)
        index_splits = [(list(range(ts, te)), list(range(es, ee))) for ts, te, es, ee in splits]
    elif cv_method == "purged_kfold":
        index_splits = purged_kfold_split(n, cv_params.get("k", 5),
                                           cv_params.get("purge_periods", 0),
                                           cv_params.get("embargo_periods", 0))
    elif cv_method == "cpcv":
        index_splits = combinatorial_purged_cv(
            n,
            cv_params.get("n_groups", 6),
            cv_params.get("n_test_groups", 2),
            cv_params.get("purge_periods", 0),
            cv_params.get("embargo_periods", 0),
        )
    else:
        raise ValueError(f"unknown cv_method: {cv_method}")

    fold_results = []
    fold_metrics = []
    for i, (train_idx, test_idx) in enumerate(index_splits):
        train_data = [data[j] for j in train_idx]
        test_data = [data[j] for j in test_idx]
        if not test_data:
            continue
        pnls = strategy_fn(train_data, test_data)
        metric = sum(pnls) if pnls else 0.0  # 단순 합. Sharpe 등 다른 지표로 교체 가능
        fold_metrics.append(metric)
        fold_results.append(FoldResult(
            fold_idx=i,
            train_start=train_idx[0] if train_idx else 0,
            train_end=train_idx[-1] if train_idx else 0,
            test_start=test_idx[0],
            test_end=test_idx[-1],
            n_train=len(train_idx),
            n_test=len(test_idx),
            metric=metric,
        ))

    if not fold_metrics:
        return CVReport([], 0, 0, 0, 1.0)

    mean_m = sum(fold_metrics) / len(fold_metrics)
    var_m = sum((x - mean_m) ** 2 for x in fold_metrics) / max(1, len(fold_metrics) - 1)
    std_m = math.sqrt(var_m)
    sharpe_m = mean_m / std_m if std_m > 0 else 0.0
    t = _t_stat(fold_metrics)
    p_val = _t_to_p_two_tailed(t, len(fold_metrics) - 1)

    return CVReport(
        fold_results=fold_results,
        mean_metric=mean_m,
        std_metric=std_m,
        sharpe_of_metrics=sharpe_m,
        p_value=p_val,
    )
