"""掉电安全的底层原语。

三类机制，文档存储与流水共用：

1. **原子替换**：``temp 文件 fsync → os.replace → 目录 fsync``。掉电点无论
   落在三步中的哪一步，盘上要么还是旧文件，要么是完整的新文件，不会出现半截。
2. **隔离区**：坏文件原样改名移动到 ``.quarantine/``，绝不就地删除或覆盖，
   并向 ``.quarantine/manifest.jsonl`` 追加一条带原因的证据记录。
3. **证据清单容错读取**：清单本身也是追加写，容忍最后一行被掉电截断。
"""

from __future__ import annotations

import json
import os
import time

QUARANTINE_DIR = ".quarantine"
MANIFEST_NAME = "manifest.jsonl"


def fsync_dir(path: str) -> None:
    """fsync 一个目录，确保其中的改名/新建已经落盘。"""

    handle = os.open(path, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def atomic_install(temp_path: str, target: str) -> None:
    """把已 fsync 的临时文件原子替换到目标位置，并 fsync 父目录。"""

    parent = os.path.dirname(os.path.abspath(target)) or "."
    os.replace(temp_path, target)
    fsync_dir(parent)


def quarantine_root(root: str) -> str:
    return os.path.join(root, QUARANTINE_DIR)


def quarantine(root: str, source: str, reason: str, **extra: object) -> str:
    """把坏文件原样移入隔离区并记一条证据，返回归档路径。

    移动是同目录改名（隔离区与源文件同在 ``root`` 下），原子且不改动文件内容。
    """

    qdir = quarantine_root(root)
    os.makedirs(qdir, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    base = os.path.basename(source)
    archived = os.path.join(qdir, f"{stamp}-{base}")
    counter = 1
    while os.path.exists(archived):
        archived = os.path.join(qdir, f"{stamp}.{counter}-{base}")
        counter += 1
    os.rename(source, archived)
    fsync_dir(os.path.dirname(os.path.abspath(source)) or ".")
    fsync_dir(qdir)
    record = {"ts": round(time.time(), 3), "source": source, "archived": archived, "reason": reason}
    record.update(extra)
    append_manifest(root, record)
    return archived


def append_manifest(root: str, record: dict) -> None:
    """向隔离区证据清单追加一条记录（fsync 后才返回）。"""

    qdir = quarantine_root(root)
    os.makedirs(qdir, exist_ok=True)
    path = os.path.join(qdir, MANIFEST_NAME)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def read_manifest(root: str) -> list[dict]:
    """读取证据清单；最后一行被掉电截断时丢弃该行，其余全部保留。"""

    path = os.path.join(quarantine_root(root), MANIFEST_NAME)
    if not os.path.isfile(path):
        return []
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as handle:
        lines = handle.readlines()
    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            # 清单的半截行只可能是掉电瞬间的最后一条，跳过即可，不动文件。
            continue
        if isinstance(entry, dict):
            records.append(entry)
    return records
