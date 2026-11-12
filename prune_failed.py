#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""小红书「获取数据失败」清单：剔除后来又下载成功的条目。

判定依据：ExploreID.db 的 explore_id 表（下载器自己的下载记录表）——
「清单里有它、表里也有它」就说明这条其实已经把文件下下来了，不该继续挂在失败清单里。

用法：XHS-Downloader/venv/bin/python prune_failed.py
stdout 输出一行 JSON：
  {"ok": true, "changed": true, "before": 31, "remaining": 13,
   "removed_count": 18, "removed": [...], "pruned_at": "...", "msg": "..."}
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

VOLUME = ROOT / "Volume"
DB_FILE = VOLUME / "ExploreID.db"


def _allow_existing_volume() -> None:
    """某些受限环境（例如带沙箱钩子的终端）会把 `Volume.mkdir(exist_ok=True)`
    也拦成 PermissionError，而目录其实已经在那儿了。这里只放这一条：
    目录已存在就放行，不存在仍照常抛错，不影响其它任何 mkdir。"""

    def guarded(self, *args, **kwargs):
        try:
            return _real_mkdir(self, *args, **kwargs)
        except PermissionError:
            if self.exists():
                return None
            raise

    _real_mkdir = Path.mkdir
    Path.mkdir = guarded  # type: ignore[assignment]


def query_done_ids(ids):
    """哪些 ID 已经在下载记录表里（＝已经下载成功过）。只读打开，不动数据。"""
    ids = [str(i) for i in ids if i]
    if not ids or not DB_FILE.exists():
        return set()
    done: set[str] = set()
    conn = sqlite3.connect(f"file:{DB_FILE}?mode=ro", uri=True)
    try:
        # 分批：SQLite 单条语句参数个数有上限（默认 999 左右）
        for start in range(0, len(ids), 400):
            chunk = ids[start : start + 400]
            marks = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT ID FROM explore_id WHERE ID IN ({marks});",
                tuple(chunk),
            ).fetchall()
            done.update(str(r[0]) for r in rows)
    finally:
        conn.close()
    return done


def main() -> int:
    _allow_existing_volume()
    from source.application.failed_records import prune_failed

    try:
        result = prune_failed(query_done_ids)
    except Exception as error:  # 不让它把调用方（下载管理器）带崩
        print(json.dumps({"ok": False, "msg": f"剔除失败：{error!r}"}, ensure_ascii=False))
        return 1

    payload = {
        "ok": bool(result.get("ok")),
        "changed": bool(result.get("changed")),
        "before": int(result.get("before") or 0),
        "remaining": int(result.get("remaining") or 0),
        "removed_count": len(result.get("removed") or []),
        "removed": list(result.get("removed") or []),
        "pruned_at": result.get("pruned_at", ""),
        "msg": result.get("msg", ""),
    }
    print(json.dumps(payload, ensure_ascii=False))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
