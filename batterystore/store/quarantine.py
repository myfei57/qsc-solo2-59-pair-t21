"""坏文件隔离区。

现场掉电留下的半截文件必须原样留痕，既不能继续被读到，也不能被悄悄改写或
删除。所有怀疑损坏的文件统一移动到存储根目录下的 ``.quarantine`` 隔离区：
移动用 ``os.replace`` 完成（同盘 rename 原子，证据文件内容一个字节都不动），
旁边放一份同名 ``.meta.json`` 侧记，写清发现时间、损坏原因与原始路径，供人工
巡检追溯。

默认策略就是隔离。隔离区文件不自动清理、不自动重建；重建是需要满足前提条件、
并由调用方显式触发的动作（见 :class:`~batterystore.store.jsonfile.JsonStore`
的 ``rebuild``）。
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

QUARANTINE_DIR = ".quarantine"
META_SUFFIX = ".meta.json"


def quarantine_root(root: str) -> str:
    """返回某个存储根对应的隔离区路径。"""

    return os.path.join(root, QUARANTINE_DIR)


def quarantine_file(
    root: str,
    path: str,
    *,
    reason: str,
    detail: str = "",
    extra: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """把坏文件原样移入隔离区，返回（隔离路径、侧记路径）。

    只做 rename，不读不写文件内容，因此移动过程不可能让证据再坏一次。目标名带
    纳秒时间戳，同一份文档多次损坏也不会互相覆盖。若文件已不存在（被并发处理），
    返回空路径，调用方按"已隔离"对待即可。
    """

    if not os.path.isfile(path):
        return "", ""

    qdir = quarantine_root(root)
    os.makedirs(qdir, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S") + f"-{time.time_ns() % 1_000_000:06d}"
    base = f"{os.path.basename(path)}.{stamp}"
    dest = _unique_dest(os.path.join(qdir, base))

    os.replace(path, dest)

    meta = {
        "original_path": os.path.abspath(path),
        "quarantined_path": os.path.abspath(dest),
        "reason": reason,
        "detail": detail,
        "quarantined_at": time.time(),
    }
    if extra:
        meta.update(extra)
    meta_path = dest + META_SUFFIX

    tmp_path = meta_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(meta, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, meta_path)
    _fsync_dir(qdir)
    return dest, meta_path


def list_quarantined(root: str) -> list[dict[str, Any]]:
    """列出隔离区里的全部坏文件及其侧记（按隔离时间升序）。"""

    qdir = quarantine_root(root)
    if not os.path.isdir(qdir):
        return []
    report: list[dict[str, Any]] = []
    for name in os.listdir(qdir):
        if name.endswith(META_SUFFIX):
            continue
        meta_path = os.path.join(qdir, name + META_SUFFIX)
        item: dict[str, Any] = {"path": os.path.join(qdir, name)}
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as handle:
                    item.update(json.load(handle))
            except (OSError, json.JSONDecodeError):
                item["reason"] = "unknown"
        report.append(item)
    report.sort(key=lambda item: item.get("quarantined_at", 0.0))
    return report


def _unique_dest(dest: str) -> str:
    """目标名撞车时追加序号，保证隔离证据互不覆盖。"""

    if not os.path.exists(dest):
        return dest
    counter = 1
    while True:
        candidate = f"{dest}.{counter}"
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def _fsync_dir(path: str) -> None:
    """把目录项变更刷到盘上（rename 的持久性靠这一步）。"""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # 部分文件系统不支持 fsync 目录，忽略：语义已尽力，不能因此阻断隔离。
        pass
    finally:
        os.close(fd)
