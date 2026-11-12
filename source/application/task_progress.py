"""把本轮处理进度写给外部看（downloader-manager 的进度条读这里）。

为什么不用「下载目录里新增的文件数」当进度：
被「存在下载记录，跳过处理」的作品根本不落任何文件，只数文件的话分子永远上不去，
进度条会一直卡在很低的位置（用户实测反馈：「并不真实」）。
真实进度应按 **已处理作品数 = 成功 + 跳过 + 失败** 来算，
这三个口径 XHS 自己的 statistics 里都有。

落盘文件：Volume/task_progress.json，写盘失败绝不影响主流程。
"""

import json
import os
from pathlib import Path

from ..module import VOLUME

__all__ = ["PROGRESS_FILE", "write_progress", "read_progress", "clear_progress"]

PROGRESS_FILE = Path(VOLUME) / "task_progress.json"


def _ensure_dir() -> None:
    """VOLUME 目录若已存在但 mkdir 被受限环境拦下，直接放行（目录存在即可）。"""
    try:
        VOLUME.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        pass


def write_progress(total: int = 0, handled: int = 0,
                   success: int = 0, skip: int = 0, fail: int = 0) -> None:
    """写一份进度快照（原子替换，避免外部读到写了一半的 json）。

    只写当前进程启动时看到的 XHS_TOKEN：外部靠它判断这份快照属于哪一轮，
    防止拿上一轮的进度（残留文件）来冒充本轮。
    """
    payload = {
        "token": os.environ.get("XHS_TOKEN") or "",
        "total": int(total or 0),
        "handled": int(handled or 0),
        "success": int(success or 0),
        "skip": int(skip or 0),
        "fail": int(fail or 0),
    }
    try:
        _ensure_dir()
        tmp = PROGRESS_FILE.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, PROGRESS_FILE)
    except Exception:
        # 进度落盘永远是「锦上添花」，不能反过来拖垮下载
        pass


def read_progress() -> dict | None:
    """读回进度快照；不存在 / 内容非法 / 不是 json 一律返回 None（调用方走兜底）。"""
    try:
        if not PROGRESS_FILE.exists():
            return None
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except Exception:
        return None


def clear_progress() -> None:
    """删除进度快照（新一轮开始前清掉上一轮残留，避免串味）。"""
    try:
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
    except Exception:
        pass
