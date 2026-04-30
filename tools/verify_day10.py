"""Day 10 모듈 빠른 동작 검증."""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

files = [
    "backtest/walk_forward.py",
    "risk/var.py",
    "sizing/portfolio_optimizer.py",
    "core/data_quality.py",
    "experiments/ab.py",
    "data/orderbook_l2.py",
    "signals/ml_oracle.py",
    "core/strategy_versioning.py",
]

print("=" * 50)
print("Syntax check")
print("=" * 50)
for f in files:
    p = ROOT / f
    with open(p, encoding="utf-8") as fh:
        ast.parse(fh.read())
    print(f"  {f}: OK")

print()
print("=" * 50)
print("Functional smoke tests")
print("=" * 50)

# walk_forward
from backtest.walk_forward import walk_forward_split, purged_kfold_split, combinatorial_purged_cv
print(f"  walk_forward(100, 40, 10, 10) splits: {len(walk_forward_split(100, 40, 10, 10))}")
print(f"  purged_kfold(100, k=5) splits:        {len(purged_kfold_split(100, 5, 2, 2))}")
print(f"  cpcv(100, 6, 2) splits:               {len(combinatorial_purged_cv(100, 6, 2))}")

# orderbook_l2
from data.orderbook_l2 import L2OrderBook
b = L2OrderBook(token_id="test")
b.apply_snapshot([(0.49, 100), (0.48, 200)], [(0.51, 100), (0.52, 200)])
print(f"  L2OrderBook mid: {b.mid:.4f}")
print(f"  depth to ask 0.52: ${b.depth_to_price('BUY', 0.52):.2f}")

# ml_oracle
from signals.ml_oracle import _hash_embedding, cosine_similarity
v1 = _hash_embedding("Will Trump win 2024 election")
v2 = _hash_embedding("Will Trump win 2024")
v3 = _hash_embedding("Bitcoin price prediction")
print(f"  sim(Trump v Trump): {cosine_similarity(v1, v2):.3f}")
print(f"  sim(Trump v BTC):   {cosine_similarity(v1, v3):.3f}")

# var (without portfolio)
from risk.var import historical_var, parametric_var, run_all_stress_tests
hr = historical_var(0.95, 1, 30, 100)
pr = parametric_var(0.95, 1, 30, 100)
print(f"  historical_var(95%, 1d): n={hr.n_observations} var={hr.var_pct*100:.2f}%")
print(f"  parametric_var(95%, 1d): n={pr.n_observations} var={pr.var_pct*100:.2f}%")

# stress test (empty portfolio mock)
class _EmptyP: positions = {}
stress = run_all_stress_tests(_EmptyP())
print(f"  stress scenarios: {len(stress)}")

# portfolio optimizer
from sizing.portfolio_optimizer import optimize_portfolio
opt = optimize_portfolio(window_days=30, method="hrp")
print(f"  optimize_portfolio HRP: n_obs={opt['n_observations']}, weights={list(opt.get('weights', {}).keys())[:3]}")

# experiments
from experiments.ab import assign_arm
print(f"  ab assign_arm test1: {assign_arm('exp1', 'token123')}")
print(f"  ab assign_arm test2: {assign_arm('exp1', 'token456')}")

# strategy versioning
from core.strategy_versioning import save_version, list_versions
vid = save_version("test_strategy", {"foo": 1}, "test", notes="smoke test")
print(f"  saved version: {vid[:30]}...")
versions = list_versions("test_strategy")
print(f"  list_versions(test_strategy): {len(versions)}")

print()
print("✅ All Day 10 modules verified.")
