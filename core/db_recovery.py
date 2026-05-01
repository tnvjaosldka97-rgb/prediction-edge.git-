"""
DB Disaster Recovery — corrupt 감지 + 백업에서 자동 복원.

감지:
- SQLite "database disk image is malformed" 에러
- "no such table" 에러 (스키마 손상)
- DB 파일 0 byte (디스크 풀로 잘림)

복원:
- backup_db.py가 만든 가장 최근 백업 (`/data/backups/db_*.db.gz`)
- gunzip 후 prediction_edge.db로 교체
- main.py 재시작 (sys.exit(1) → Railway 자동 재시작)

복원 후:
- 최근 백업과 현재 시각 사이의 trade는 잃을 수 있음
- 텔레그램 CRITICAL 알림으로 사람 점검 권고
"""
from __future__ import annotations
import gzip
import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import config


BACKUP_DIR = Path("/data/backups") if Path("/data").exists() else Path("backups")


def is_db_corrupt() -> tuple[bool, str]:
    """현재 DB가 corrupt인지 quick check."""
    db_path = Path(config.DB_PATH)

    if not db_path.exists():
        return True, "db_file_missing"

    if db_path.stat().st_size == 0:
        return True, "db_file_empty"

    try:
        conn = sqlite3.connect(str(db_path), timeout=2.0)
        # 핵심 테이블 존재 확인
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        if "trades" not in tables or "signals" not in tables:
            return True, "missing_critical_tables"
        return False, ""
    except sqlite3.DatabaseError as e:
        return True, f"sqlite_error: {str(e)[:60]}"
    except Exception as e:
        return True, f"unknown: {str(e)[:60]}"


def get_latest_backup() -> Path | None:
    """가장 최근 백업 파일 (db_YYYYMMDD_HHMMSS.db.gz)."""
    if not BACKUP_DIR.exists():
        return None
    backups = sorted(BACKUP_DIR.glob("db_*.db.gz"), reverse=True)
    return backups[0] if backups else None


def restore_from_backup(backup_path: Path) -> bool:
    """백업에서 DB 복원. 기존 DB 파일은 .corrupt로 백업."""
    db_path = Path(config.DB_PATH)

    # 기존 corrupt DB 보존
    if db_path.exists():
        corrupt_backup = db_path.with_suffix(".db.corrupt." + str(int(time.time())))
        try:
            shutil.move(str(db_path), str(corrupt_backup))
        except Exception:
            db_path.unlink(missing_ok=True)

    # gz 압축 풀기
    try:
        with gzip.open(backup_path, "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        return True
    except Exception:
        return False


def auto_recover_if_corrupt() -> bool:
    """corrupt 감지 시 자동 복원. 성공 시 True. 호출 후 sys.exit(1) 권장."""
    is_bad, reason = is_db_corrupt()
    if not is_bad:
        return False

    from core.logger import log
    log.error(f"[db_recovery] CORRUPT DB: {reason}")

    backup = get_latest_backup()
    if not backup:
        log.error("[db_recovery] no backup available, cannot recover")
        try:
            from notifications.telegram import notify
            notify("CRITICAL", "DB 손상 + 백업 없음 — 수동 복구 필요", {
                "reason": reason,
                "action": "Railway 볼륨 확인, 백업 폴더 점검",
            })
        except Exception:
            pass
        return False

    log.info(f"[db_recovery] restoring from {backup.name}")
    if restore_from_backup(backup):
        log.info("[db_recovery] restored. Restart recommended.")
        try:
            from notifications.telegram import notify
            notify("CRITICAL", "DB 자동 복원됨", {
                "backup": backup.name,
                "reason": reason,
                "action": "복원 후 restart. 백업~현재 사이 trade는 손실 가능. 점검 필요.",
            })
        except Exception:
            pass
        return True
    return False


async def db_health_loop(interval_sec: int = 300):
    """5분마다 DB 헬스 체크 + 자동 복원."""
    import asyncio
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            is_bad, reason = is_db_corrupt()
            if is_bad:
                log.error(f"[db_health] DB problem: {reason}")
                if auto_recover_if_corrupt():
                    log.info("[db_health] recovered, exiting for restart")
                    sys.exit(1)    # Railway가 자동 재시작
        except Exception as e:
            from core.logger import log
            log.warning(f"[db_health] {e}")
            await asyncio.sleep(60)
