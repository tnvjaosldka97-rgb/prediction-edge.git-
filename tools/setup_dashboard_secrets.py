"""
대시보드 인증·세션 비밀값 생성 + .env 업데이트.

생성:
- DASHBOARD_SESSION_SECRET : 64자 랜덤 hex
- ADMIN_PASSWORD            : 4단어 + 숫자 (기억 가능한 강력 비밀번호)
- ADMIN_PASSWORD_HASH       : sha256 hash of password

password는 한 번만 화면에 출력 (사용자 메모용).
hash + secret는 .env에 자동 저장.
"""
from __future__ import annotations
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / ".env"
sys.path.insert(0, str(ROOT))

from dashboard.auth import hash_password_for_env


# 4단어 + 숫자 — 외우기 쉽고 강력
WORDS = [
    "ember", "thunder", "raven", "compass", "spark", "vortex", "phantom", "amber",
    "glacier", "crimson", "tundra", "cipher", "obsidian", "saffron", "pearl", "willow",
    "mercury", "horizon", "echo", "lattice", "summit", "delta", "cobalt", "nova",
]


def make_password() -> str:
    sysrand = secrets.SystemRandom()
    chosen = sysrand.sample(WORDS, 3)
    n = sysrand.randint(10, 99)
    return "-".join(chosen) + f"-{n}"


def update_env(updates: dict[str, str]) -> None:
    if not ENV.exists():
        ENV.write_text("\n".join(f"{k}={v}" for k, v in updates.items()) + "\n", encoding="utf-8")
        return
    lines = ENV.read_text(encoding="utf-8").splitlines()
    seen = set()
    out = []
    for line in lines:
        replaced = False
        for k in updates:
            if line.startswith(f"{k}="):
                out.append(f"{k}={updates[k]}")
                seen.add(k)
                replaced = True
                break
        if not replaced:
            out.append(line)
    for k, v in updates.items():
        if k not in seen:
            out.append(f"{k}={v}")
    ENV.write_text("\n".join(out) + "\n", encoding="utf-8")


def main() -> int:
    session_secret = secrets.token_hex(32)
    password = make_password()
    pw_hash = hash_password_for_env(password)

    update_env({
        "ADMIN_PASSWORD_HASH": pw_hash,
        "DASHBOARD_SESSION_SECRET": session_secret,
        "ADMIN_IPS": "",  # 비워두면 모든 IP 허용 (선생님이 본인 IP 알면 거기서 채워도 됨)
    })

    print("=" * 60)
    print("  대시보드 비밀값 생성 완료")
    print("=" * 60)
    print()
    print("저장됨 (.env):")
    print(f"  ADMIN_PASSWORD_HASH      = {pw_hash[:24]}...")
    print(f"  DASHBOARD_SESSION_SECRET = {session_secret[:16]}... (64자)")
    print(f"  ADMIN_IPS                = (빈 값 = 모든 IP)")
    print()
    print("=" * 60)
    print("  ⚠️  대시보드 로그인 비밀번호 ⚠️")
    print("=" * 60)
    print()
    print(f"        {password}")
    print()
    print("이 비밀번호:")
    print("  - 메모장 또는 종이에 적어두세요")
    print("  - 채팅창·SNS·이메일·클라우드에 공유 X")
    print("  - Railway 대시보드 로그인 시 입력")
    print()
    print("Railway 환경변수에 그대로 복사할 값 (위 .env에서 복사):")
    print(f"  ADMIN_PASSWORD_HASH      = (위 hash)")
    print(f"  DASHBOARD_SESSION_SECRET = (위 secret)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
