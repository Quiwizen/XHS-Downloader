"""「获取数据失败」清单落盘。

背景：小红书对不可访问的笔记也返回 HTTP 200，只是把请求重定向到 /404 落地页，
于是 `app.py::_get_html_data` 只能报一句统一的「获取数据失败」，分不清
①内容本身没了（-510001）②xsec_token 失效/风控（/404/sec_xxx）。

这里把失败时的**原始链接**（带 xsec_token）+ 落地页形态一起记下来，
写到 `Volume/failed_records/`，人工复核时直接看 md 就行。

两种落地页形态（实测区分）：
- `.../404?source=note&noteId=xxx&errorCode=-510001` → 笔记状态异常，内容不可访问
- `.../404?source=/404/sec_xxx?redirectPath=...`     → token 校验没过，重新复制链接可救
"""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from contextlib import suppress
from typing import Any, Callable, Iterable

from ..module import VOLUME

__all__ = [
    "FAILED_DIR",
    "REASON_CONTENT",
    "REASON_TOKEN",
    "REASON_UNKNOWN",
    "REASON_EMPTY",
    "classify",
    "record_failure",
    "append_failure",
    "flush",
    "latest_paths",
    "prune_failed",
    "remove_failure",
]

# 失败清单输出目录
FAILED_DIR: Path = VOLUME / "failed_records"

# 最近一份清单（前端「失败清单」入口读的就是这两个）
LATEST_JSON_NAME = "failed_latest.json"
LATEST_MD_NAME = "failed_latest.md"

REASON_CONTENT = "content_unavailable"  # 笔记状态异常（-510001），内容不可访问
REASON_TOKEN = "token_invalid"  # token 失效 / 风控，重新复制链接可救
REASON_UNKNOWN = "unknown"  # 非 404 落地页，原因不明
REASON_EMPTY = "empty_response"  # 请求返回空（网络异常等）

REASON_LABEL = {
    REASON_CONTENT: "内容不可访问",
    REASON_TOKEN: "Token 失效/风控",
    REASON_UNKNOWN: "原因不明",
    REASON_EMPTY: "请求返回空",
}

# 日志里用的短文案（清单里用上面那份详细的）
REASON_SHORT = {
    REASON_CONTENT: "笔记状态异常 -510001，内容不可访问",
    REASON_TOKEN: "xsec_token 失效/风控，可重新复制链接",
    REASON_UNKNOWN: "原因不明，建议打开链接确认",
    REASON_EMPTY: "请求返回空（可能偶发）",
}

REASON_TEXT = {
    REASON_CONTENT: "笔记状态异常（errorCode=-510001），内容不可访问：已删除 / 转私密 / 被下架。重试无用",
    REASON_TOKEN: "未取到笔记数据（落地页无 errorCode），多为 xsec_token 失效或被风控。重新从 App 复制链接可救",
    REASON_UNKNOWN: "未取到笔记数据，落地页形态未知，建议手动打开链接确认",
    REASON_EMPTY: "请求返回空（网络异常或页面为空），可能偶发，可重试",
}


def classify(final_url: str) -> tuple[str, str]:
    """按最终跳转地址判定失败类型，返回 (reason, 落地页上的 errorCode)。"""
    url = final_url or ""
    if "errorCode=" in url:
        code = ""
        for part in url.split("&"):
            if part.startswith("errorCode="):
                code = part.split("=", 1)[1]
                break
        return REASON_CONTENT, code
    if "xiaohongshu.com/404" in url or "rednote.com/404" in url:
        return REASON_TOKEN, ""
    if not url:
        return REASON_EMPTY, ""
    return REASON_UNKNOWN, ""


def record_failure(
    sink: list[dict[str, Any]],
    url: str,
    note_id: str = "",
    final_url: str = "",
) -> dict[str, Any]:
    """把一条失败记录放进待落盘列表（暂不写文件，由 flush 统一写）。"""
    reason, code = classify(final_url)
    item = {
        "note_id": note_id,
        "url": url,
        "reason": reason,
        "reason_text": REASON_TEXT[reason],
        "reason_short": REASON_SHORT[reason],
        "error_code": code,
        "final_url": final_url,
        "failed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    sink.append(item)
    return item


# 写盘锁：多条笔记可能被并发处理（如 extract_links 的并发请求），避免互相覆盖
_WRITE_LOCK = threading.Lock()


def _payload(items: list[dict[str, Any]], label: str = "") -> dict[str, Any]:
    """按对外字段组装清单 payload（后端 /api/xhs/failed 依赖这些字段）。"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "label": label or f"cli_{ts}",
        "total_failed": len(items),
        "by_reason": {
            reason: sum(1 for i in items if i["reason"] == reason)
            for reason in (REASON_CONTENT, REASON_TOKEN, REASON_UNKNOWN, REASON_EMPTY)
            if any(i["reason"] == reason for i in items)
        },
        "items": items,
    }


def _write_latest(items: list[dict[str, Any]], label: str = "") -> dict[str, Any]:
    """把清单（本地传入的 items 会先与磁盘上已有内容合并去重）写入 failed_latest.*。"""
    items = _dedup(items)
    payload = _payload(items, label)
    _write_files(FAILED_DIR, "failed_latest", payload)
    return payload


def _write_files(folder: Path, name: str, payload: dict[str, Any]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    (folder / f"{name}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (folder / f"{name}.md").write_text(_render_md(payload), encoding="utf-8")


def append_failure(
    sink: list[dict[str, Any]],
    url: str,
    note_id: str = "",
    final_url: str = "",
) -> dict[str, Any]:
    """记录一条失败，并**立刻写盘**。

    为什么不再等任务跑完：批量任务可能跑几小时，中途失败若只在结尾导出，
    一是页面看到的永远是上一轮的旧清单，二是任务被中断（Ctrl+C / 用户停止）
    时这一整轮失败会全部丢失。所以这里每条失败就地落盘，latest 里累积全部。
    """
    item = record_failure(sink, url, note_id=note_id, final_url=final_url)
    with _WRITE_LOCK:
        try:
            latest = FAILED_DIR / LATEST_JSON_NAME
            existing: list[dict[str, Any]] = []
            label = ""
            if latest.exists():
                try:
                    loaded = json.loads(latest.read_text(encoding="utf-8"))
                    loaded_items = loaded.get("items") or []
                    if isinstance(loaded_items, list):
                        existing = loaded_items
                    label = str(loaded.get("label") or "")
                except Exception:
                    existing = []  # 文件损坏也不该影响主流程，按空清单重建
            _write_latest(list(existing) + [item], label)
        except Exception as error:  # 落盘失败只告警，不能打断下载
            with suppress(Exception):
                print(f"[failed_records] 增量写入失败：{error!r}")  # noqa: T201
    return item


def _dedup(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """按笔记 ID 去重，保留首次出现。"""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for i in items:
        key = i.get("note_id") or i.get("url") or str(id(i))
        if key in seen:
            continue
        seen.add(key)
        out.append(i)
    return out


def _render_md(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    items = payload["items"]
    lines.append("# 小红书「获取数据失败」清单")
    lines.append("")
    lines.append(f"- 导出时间：{payload['exported_at']}")
    lines.append(f"- 任务标识：{payload['label']}")
    lines.append(f"- 失败总数：{len(items)}")
    summary = " / ".join(
        f"{REASON_LABEL.get(k, k)} {v} 条" for k, v in payload["by_reason"].items()
    )
    lines.append(f"- 分类：{summary}")
    if payload.get("pruned_at"):
        removed = int(payload.get("pruned_count") or 0)
        lines.append(
            f"- 已剔除：{removed} 条后来已成功下载的作品（{payload['pruned_at']}）"
        )
    lines.append("")
    lines.append(
        "> 小红书对不可访问笔记也返回 HTTP 200（只是跳到 /404 落地页），"
        "所以原来的「获取数据失败」分不清下面两类。本清单按落地页是否带 errorCode 自动分类。"
    )
    lines.append("")

    groups = [
        (REASON_CONTENT, "一、内容不可访问（重试无用，内容确实没了）"),
        (REASON_TOKEN, "二、Token 失效 / 风控（重新复制链接通常能救）"),
        (REASON_UNKNOWN, "三、原因不明"),
        (REASON_EMPTY, "四、请求返回空（可能偶发，可重试）"),
    ]
    for reason, title in groups:
        sub = [i for i in items if i["reason"] == reason]
        if not sub:
            continue
        lines.append(f"## {title} —— {len(sub)} 条")
        lines.append("")
        lines.append("| # | 笔记 ID | 原始链接 | 落地页 errorCode | 失败时间 |")
        lines.append("|---|---|---|---|---|")
        for n, i in enumerate(sub, 1):
            short = i["note_id"] or "-"
            link = f"[打开]({i['url']})" if i["url"] else "-"
            lines.append(
                f"| {n} | `{short}` | {link} | {i['error_code'] or '—'} | {i['failed_at']} |"
            )
        lines.append("")

    lines.append("## 纯链接（可直接复制重跑）")
    lines.append("")
    for i in items:
        lines.append(i["url"])
    lines.append("")
    lines.append("## 落地页地址（排查用）")
    lines.append("")
    lines.append("```")
    for i in items:
        lines.append(f"{i['note_id'] or '-'}\t{i['final_url'] or '(空)'}")
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def flush(sink: list[dict[str, Any]], label: str = "") -> dict[str, Any] | None:
    """把失败清单写出到 Volume/failed_records/，返回写入结果（无失败则返回 None）。"""
    items = _dedup(sink)
    if not items:
        return None

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    written: dict[str, Any] = {"dir": str(FAILED_DIR), "total": len(items)}

    with _WRITE_LOCK:
        # 收尾再落一次 latest：把本轮剩余条目并进已有清单，保证不丢（含任务中途被打断的场景）
        payload = _write_latest(items, label)

        # 带时间戳的留档：同一轮任务一份，便于回看历史
        _write_files(FAILED_DIR, f"failed_{ts}", payload)

        for key, name in (("json_name", f"failed_{ts}.json"), ("md_name", f"failed_{ts}.md")):
            written[key] = name

        # 时间戳留档只保留最近若干份，避免目录越堆越乱（latest 永远保留）
        keep = 20
        for path in sorted(
            FAILED_DIR.glob("failed_*.md"), key=lambda p: p.stat().st_mtime
        )[:-keep]:
            with suppress(OSError):
                path.unlink()
                path.with_suffix(".json").unlink(missing_ok=True)

    # 清掉本轮 sink，避免跨任务重复累积
    sink.clear()

    # 时间戳留档只保留最近若干份，避免目录越堆越乱（latest 永远保留）
    keep = 20
    for path in sorted(
        FAILED_DIR.glob("failed_*.md"), key=lambda p: p.stat().st_mtime
    )[:-keep]:
        with suppress(OSError):
            path.unlink()
            path.with_suffix(".json").unlink(missing_ok=True)

    return written


def _read_latest() -> tuple[list[dict[str, Any]], str]:
    """读回 failed_latest.json 里现有的条目（损坏按空清单处理，不让异常冒到主流程）。"""
    latest = FAILED_DIR / LATEST_JSON_NAME
    if not latest.exists():
        return [], ""
    try:
        loaded = json.loads(latest.read_text(encoding="utf-8"))
    except Exception:
        return [], ""
    items = loaded.get("items") or []
    if not isinstance(items, list):
        items = []
    return items, str(loaded.get("label") or "")


def prune_failed(is_done: Callable[[Iterable[str]], set[str]]) -> dict[str, Any]:
    """把「后来又下载成功」的作品从 failed_latest 里剔除。

    判定依据由调用方给：is_done(ids) -> 这批 ID 里已经成功下载的集合。
     downloader 传的就是 ExploreID.db（程序自己的下载记录表），
     所以「清单里有、DB 里也有」== 这条其实已经下下来了，不该再挂在失败清单里。

    只动 failed_latest.*（页面 / 前端看到的正是这份），带时间戳的历史留档不动，
     随时能回看「那一批到底哪些没下下来」。

    返回 {"ok", "changed", "before", "remaining", "removed": [...], "pruned_at", "msg"}。
    """
    with _WRITE_LOCK:
        items, label = _read_latest()
        if not items:
            return {
                "ok": True,
                "changed": False,
                "before": 0,
                "remaining": 0,
                "removed": [],
                "msg": "清单为空，无需剔除",
            }
        ids = [str(i.get("note_id") or "") for i in items]
        ids = [i for i in ids if i]
        if not ids:
            return {
                "ok": True,
                "changed": False,
                "before": len(items),
                "remaining": len(items),
                "removed": [],
                "msg": "清单里的条目都没有笔记 ID，跳过剔除",
            }
        try:
            raw_done = is_done(ids) or set()
            # 只认真正的集合/序列。调用方若误把判据写成 async def，这里拿到的是
            # coroutine——它不可迭代会直接进 except，绝不能被当成「全都已下载」。
            if not isinstance(raw_done, (set, frozenset, list, tuple)):
                raise TypeError(
                    f"is_done() 返回了 {type(raw_done).__name__}，期望 set/list/tuple"
                    "（判据函数是不是被写成了 async def？）"
                )
            done = {str(x) for x in raw_done}
        except Exception as error:  # 判据本身出错就别乱删，直接放弃这一轮剔除
            return {
                "ok": False,
                "changed": False,
                "before": len(items),
                "remaining": len(items),
                "removed": [],
                "msg": f"剔除失败：{error!r}",
            }
        if not done:
            return {
                "ok": True,
                "changed": False,
                "before": len(items),
                "remaining": len(items),
                "removed": [],
                "msg": "",
            }

        keep: list[dict[str, Any]] = []
        removed: list[dict[str, Any]] = []
        for i in items:
            (removed if str(i.get("note_id") or "") in done else keep).append(i)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = _payload(keep, label)
        payload["pruned_at"] = now
        payload["pruned_count"] = len(removed)
        try:
            _write_files(FAILED_DIR, "failed_latest", payload)
        except Exception as error:
            return {
                "ok": False,
                "changed": False,
                "before": len(items),
                "remaining": len(items),
                "removed": [],
                "msg": f"剔除后写盘失败：{error!r}",
            }
        return {
            "ok": True,
            "changed": True,
            "before": len(items),
            "remaining": len(keep),
            "removed": [str(i.get("note_id") or "") for i in removed],
            "pruned_at": now,
            "msg": f"已剔除 {len(removed)} 条已成功下载的条目，剩余 {len(keep)} 条",
        }


def remove_failure(note_id: str) -> dict[str, Any]:
    """从 failed_latest 里摘掉某一条（作品刚处理成功时调用）。"""
    if not note_id:
        return {"ok": True, "changed": False, "remaining": 0, "removed": []}
    target = str(note_id)
    return prune_failed(lambda ids: {target} if target in set(ids) else set())


def latest_paths() -> dict[str, str]:
    """给外部/页面查询用：返回最近一份清单的两个文件路径（不存在则空串）。"""
    md = FAILED_DIR / LATEST_MD_NAME
    js = FAILED_DIR / LATEST_JSON_NAME
    return {"md": str(md) if md.exists() else "", "json": str(js) if js.exists() else ""}
