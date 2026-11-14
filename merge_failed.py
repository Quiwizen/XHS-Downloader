#!/usr/bin/env python3
"""把历次失败清单合并成一份，并剔除「后来已经下载成功」的条目。

背景：``failed_latest.json`` 只反映最近一轮，而 ``failed_<时间戳>.json`` 留档里
散落着历次失败。想一次性核对 / 重试时，需要把它们合成一份完整清单。

规则：
1. 按 ``note_id`` 去重，同一笔记只保留 ``failed_at`` 最新的一条（原因可能升级）；
2. 剔除 ``ExploreID.db`` 里已有的 ID——有下载记录说明这条其实已经下下来了；
3. 输出 ``failed_merged.{json,md}``；加 ``--apply`` 时同时覆盖 ``failed_latest.*``
   （管理器界面读的是 latest，覆盖后点「⚠️ 失败清单」就能看到合并结果）。

原始留档一份都不删，``failed_<时间戳>.*`` 原样保留。

用法::

    ./venv/bin/python merge_failed.py            # 只生成 failed_merged.*
    ./venv/bin/python merge_failed.py --apply    # 同时让界面显示合并结果
    ./venv/bin/python merge_failed.py --dry-run  # 只报告数量，不写任何文件
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

FOLDER = ROOT / "Volume" / "failed_records"
EXPLORE_DB = ROOT / "Volume" / "ExploreID.db"


def _allow_existing_volume() -> None:
    """受限环境（如带沙箱钩子的终端）会把 ``Volume.mkdir(exist_ok=True)``
    拦成 PermissionError，而目录其实已经存在。只放行这一种情况。"""

    def guarded(self, *args, **kwargs):
        try:
            return _real_mkdir(self, *args, **kwargs)
        except (PermissionError, FileExistsError):
            # 沙箱钩子对已存在的目录有时抛 PermissionError、有时抛 FileExistsError，
            # 两种都只放行「目录确实已存在」这一种情况。
            if self.exists():
                return None
            raise

    _real_mkdir = Path.mkdir
    Path.mkdir = guarded  # type: ignore[assignment]


def _load_archives() -> tuple[list[dict], int]:
    """读全部 failed_<ts>.json，返回 (条目列表, 文件数)。"""
    if not FOLDER.is_dir():
        return [], 0
    files = sorted(p for p in FOLDER.glob("failed_*.json")
                   if p.name != "failed_latest.json")
    items: list[dict] = []
    for p in files:
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        for it in data.get("items") or []:
            if isinstance(it, dict):
                items.append(dict(it, _source=p.name))
    return items, len(files)


def _dedup(items: list[dict]) -> dict[str, dict]:
    """按 note_id 去重，保留 failed_at 最新的一条。"""
    best: dict[str, dict] = {}
    for it in items:
        nid = str(it.get("note_id") or "").strip()
        if not nid:
            continue
        prev = best.get(nid)
        if prev is None or str(it.get("failed_at") or "") >= str(prev.get("failed_at") or ""):
            best[nid] = it
    return best


def _done_ids() -> set[str]:
    """ExploreID.db 里已下载成功的笔记 ID。"""
    if not EXPLORE_DB.exists():
        return set()
    try:
        con = sqlite3.connect(f"file:{EXPLORE_DB}?mode=ro", uri=True, timeout=5)
    except sqlite3.Error:
        return set()
    try:
        return {str(r[0]) for r in con.execute("SELECT ID FROM explore_id") if r and r[0]}
    except sqlite3.Error:
        return set()
    finally:
        con.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="合并所有历史失败清单")
    parser.add_argument("--apply", action="store_true",
                        help="同时覆盖 failed_latest.*（管理器界面读它）")
    parser.add_argument("--dry-run", action="store_true", help="只报告，不写文件")
    args = parser.parse_args()

    # 导入 XHS 侧模块会触发 Volume 目录创建，先放行「目录已存在」
    _allow_existing_volume()
    from source.application.failed_records import _render_md

    raw, file_count = _load_archives()
    if not raw:
        print(json.dumps({"ok": False, "msg": "没有可合并的失败留档"},
                         ensure_ascii=False))
        return 0

    by_id = _dedup(raw)
    done = _done_ids()
    kept = {nid: it for nid, it in by_id.items() if nid not in done}
    dropped = len(by_id) - len(kept)

    items = sorted(kept.values(), key=lambda it: str(it.get("failed_at") or ""))
    for it in items:
        it.pop("_source", None)

    by_reason: dict[str, int] = {}
    for it in items:
        reason = str(it.get("reason") or "unknown")
        by_reason[reason] = by_reason.get(reason, 0) + 1

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "label": f"merged_{stamp}",
        "total_failed": len(items),
        "by_reason": by_reason,
        "items": items,
        "merged_from": file_count,
        "dropped_downloaded": dropped,
        "raw_items": len(raw),
    }

    summary = {
        "ok": True,
        "dry_run": bool(args.dry_run),
        "archives": file_count,
        "raw_items": len(raw),
        "unique_notes": len(by_id),
        "dropped_downloaded": dropped,
        "merged_total": len(items),
        "by_reason": by_reason,
    }

    if args.dry_run:
        print(json.dumps(summary, ensure_ascii=False))
        return 0

    md = _render_md(payload)
    header = (
        f"# 小红书「获取数据失败」清单（合并版）\n\n"
        f"- 合并来源：{file_count} 份留档，原始 {len(raw)} 条\n"
        f"- 按笔记去重后：{len(by_id)} 条\n"
        f"- 剔除已下载成功：{dropped} 条\n"
        f"- 合并结果：**{len(items)} 条**\n\n"
        "> 本文件由 merge_failed.py 生成；原始 failed_<时间戳>.* 留档未改动。\n\n"
    )
    # 去掉原渲染的 H1 标题，避免输出里出现两个一级标题
    lines = md.splitlines()
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    body = "\n".join(lines).lstrip("\n")

    FOLDER.mkdir(parents=True, exist_ok=True)
    (FOLDER / "failed_merged.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (FOLDER / "failed_merged.md").write_text(header + body, encoding="utf-8")

    if args.apply:
        (FOLDER / "failed_latest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        (FOLDER / "failed_latest.md").write_text(header + body, encoding="utf-8")

    summary.update({
        "written": ["failed_merged.json", "failed_merged.md"]
        + (["failed_latest.json", "failed_latest.md"] if args.apply else []),
        "msg": f"合并完成：{len(items)} 条"
               + ("，已同步到界面清单" if args.apply else ""),
    })
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
