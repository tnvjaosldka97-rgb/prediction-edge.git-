"""
DB 자동 백업.

매일 prediction_edge.db → 압축 + 7일 보관.
Railway 영구 볼륨 (/data) 사용. 디스크 가득 차면 가장 오래된 자동 삭제.

수동 실행: venv/Scripts/python tools/backup_db.py
자동 실행: main.py 백그라운드 루프 (24h 간격)
"""
from __future__ import annotations
import asyncio
import gzip
import os
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import config


BACKUP_DIR = Path("/data/backups") if Path("/data").exists() else Path("backups")
KEEP_DAYS = 7


def ensure_dir():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def backup_now() -> Path:
    """현재 DB → 압축 백업. 반환: 백업 파일 경로."""
    ensure_dir()
    src = Path(config.DB_PATH)
    if not src.exists():
        raise FileNotFoundError(f"DB not found: {src}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest_temp = BACKUP_DIR / f"db_{ts}.tmp"
    dest = BACKUP_DIR / f"db_{ts}.db.gz"

    # SQLite는 hot copy 가능 — backup API 사용 (안전)
    src_conn = sqlite3.connect(str(src))
    dest_conn = sqlite3.connect(str(dest_temp))
    src_conn.backup(dest_conn)
    src_conn.close()
    dest_conn.close()

    # 압축
    with open(dest_temp, "rb") as f_in:
        with gzip.open(dest, "wb", compresslevel=6) as f_out:
            shutil.copyfileobj(f_in, f_out)
    dest_temp.unlink()

    return dest


def cleanup_old(keep_days: int = KEEP_DAYS) -> int:
    """오래된 백업 삭제. 반환: 삭제 개수."""
    ensure_dir()
    cutoff = time.time() - keep_days * 86400
    removed = 0
    for f in BACKUP_DIR.glob("db_*.db.gz"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                removed += 1
        except Exception:
            pass
    return removed


def list_backups() -> list[dict]:
    """현재 백업 목록."""
    ensure_dir()
    out = []
    for f in sorted(BACKUP_DIR.glob("db_*.db.gz"), reverse=True):
        try:
            stat = f.stat()
            out.append({
                "name": f.name,
                "size_mb": stat.st_size / 1_000_000,
                "modified": stat.st_mtime,
            })
        except Exception:
            continue
    return out


async def backup_loop(interval_sec: int = 86400):
    """24시간마다 자동 백업 + 정리."""
    from core.logger import log
    while True:
        try:
            await asyncio.sleep(interval_sec)
            backup_path = backup_now()
            removed = cleanup_old()
            log.info(f"[backup] saved {backup_path.name} ({backup_path.stat().st_size/1e6:.1f}MB), cleaned {removed} old")
            try:
                from notifications.telegram import notify
                notify("INFO", "DB 백업 완료", {
                    "file": backup_path.name,
                    "size_mb": f"{backup_path.stat().st_size / 1e6:.1f}",
                    "kept_count": len(list_backups()),
                })
            except Exception:
                pass
        except Exception as e:
            from core.logger import log
            log.warning(f"[backup] {e}")
            await asyncio.sleep(3600)


if __name__ == "__main__":
    print("Running manual backup...")
    p = backup_now()
    print(f"Saved: {p} ({p.stat().st_size/1e6:.1f}MB)")
    removed = cleanup_old()
    print(f"Cleaned {removed} old backups")
    print(f"Total backups: {len(list_backups())}")
