"""原子 JSON 文档存储。

每个文档一个 ``<doc_id>.json`` 文件，写入遵循
``临时文件 fsync → os.replace → 目录 fsync``，掉电点落在任何一步，盘上都只会
是"旧的完整文档"或"新的完整文档"，不会有半截文档。

读盘失败按性质分三类，不再一锅端：

* 文件不存在 → 返回 ``None``，调用方用默认值即可；
* JSON 解析失败（半截/损坏）→ 原件**原样隔离**到 ``.quarantine/`` 等人工处理，
  抛 :class:`CorruptStorageError` 并附上归档路径，服务可以抓住它继续跑；
* 普通 IO 失败 → 抛 :class:`StorageError`。

默认策略是隔离，绝不自动重建。只有调用方拿着自己校验过的"最后一次有效内容"
显式调用 :meth:`recover_document` 时才允许重建，且只新建、不覆盖，隔离区里
的坏原件始终保留可追溯。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from typing import Any

from ..errors import CorruptStorageError, StorageError
from . import durable


class JsonStore:
    """目录形式的 JSON 文档存储。"""

    def __init__(self, root: str) -> None:
        self._root = os.path.abspath(root)
        self._lock = threading.RLock()
        os.makedirs(self._root, exist_ok=True)
        self._reap_stale_tempfiles()

    @property
    def root(self) -> str:
        return self._root

    def path_of(self, doc_id: str) -> str:
        """返回某文档对应的文件路径。"""

        return os.path.join(self._root, f"{doc_id}.json")

    def exists(self, doc_id: str) -> bool:
        """文档文件是否存在。"""

        return os.path.isfile(self.path_of(doc_id))

    def write(self, doc_id: str, payload: Any) -> str:
        """原子写入一个文档，返回最终文件路径。

        成功返回后内容与目录项都已落盘；失败时目标文件保持写入前的样子。
        """

        target = self.path_of(doc_id)
        with self._lock:
            temp_path: str | None = None
            try:
                handle = tempfile.NamedTemporaryFile(
                    "w",
                    encoding="utf-8",
                    dir=self._root,
                    prefix=f".{doc_id}.",
                    suffix=".tmp",
                    delete=False,
                )
                temp_path = handle.name
                with handle:
                    json.dump(payload, handle, ensure_ascii=False, indent=2)
                    handle.flush()
                    os.fsync(handle.fileno())
                durable.atomic_install(temp_path, target)
            except (OSError, TypeError, ValueError) as error:
                if temp_path and os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except OSError:
                        pass
                raise StorageError(f"写入文档 {doc_id} 失败: {error}", component="store") from error
        return target

    def read(self, doc_id: str) -> Any | None:
        """读取一个文档。

        不存在返回 ``None``；内容损坏时先隔离原件再抛 :class:`CorruptStorageError`，
        重复读取同一个坏文档不会重复隔离。
        """

        target = self.path_of(doc_id)
        with self._lock:
            if not os.path.isfile(target):
                return None
            try:
                with open(target, "r", encoding="utf-8") as handle:
                    return json.load(handle)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                archived = self._quarantine(doc_id, str(error))
                raise CorruptStorageError(
                    f"文档 {doc_id} 已损坏，原件已隔离至 {archived}，需要人工确认后显式重建",
                    quarantine_path=archived,
                ) from error

    def inspect(self, doc_id: str) -> dict:
        """给出一个文档的明确结论：ok / missing / quarantined，不抛异常。"""

        with self._lock:
            target = self.path_of(doc_id)
            evidence = [
                entry
                for entry in durable.read_manifest(self._root)
                if os.path.basename(entry.get("source", "")) == f"{doc_id}.json"
            ]
            if os.path.isfile(target):
                state = "ok"
                size: int | None = os.stat(target).st_size
            else:
                size = None
                state = "quarantined" if evidence else "missing"
            return {
                "doc_id": doc_id,
                "state": state,
                "path": target if state == "ok" else None,
                "bytes": size,
                "evidence": evidence,
            }

    def quarantine_report(self) -> list[dict]:
        """返回隔离区全部证据记录（含文档与流水段，按时间升序）。"""

        with self._lock:
            return durable.read_manifest(self._root)

    def recover_document(self, doc_id: str, payload: Any) -> str:
        """显式重建：用调用方校验过的有效内容恢复一个损坏/缺失的文档。

        允许重建的唯一入口，且只在以下条件放行：

        1. 当前活文件不存在（已经被读操作隔离，或从未存在）；如果活文件仍然
           有效，拒绝重建以免覆盖好数据；
        2. ``payload`` 由调用方负责校验（"最后一次有效内容"的判定权在业务侧）；
        3. 隔离区里的坏原件原样保留，只额外追加一条 ``rebuilt`` 证据。
        """

        target = self.path_of(doc_id)
        with self._lock:
            if os.path.isfile(target):
                raise StorageError(
                    f"拒绝重建文档 {doc_id}：活文件仍然有效，重建只允许用于已隔离的损坏文档",
                    component="store",
                )
            path = self.write(doc_id, payload)
            durable.append_manifest(
                self._root,
                {
                    "ts": round(time.time(), 3),
                    "source": target,
                    "archived": None,
                    "reason": "rebuilt",
                    "doc_id": doc_id,
                },
            )
            return path

    def remove(self, doc_id: str) -> bool:
        """删除一个文档，返回是否真的删掉了文件。"""

        target = self.path_of(doc_id)
        with self._lock:
            if not os.path.isfile(target):
                return False
            os.remove(target)
            durable.fsync_dir(self._root)
            return True

    def list_ids(self) -> list[str]:
        """列出全部文档标识（按文件名排序）。"""

        with self._lock:
            names = [name for name in os.listdir(self._root) if name.endswith(".json")]
        return sorted(name[: -len(".json")] for name in names)

    def _quarantine(self, doc_id: str, reason: str) -> str:
        target = self.path_of(doc_id)
        archived = durable.quarantine(self._root, target, f"文档损坏: {reason}", doc_id=doc_id)
        return archived

    def _reap_stale_tempfiles(self) -> None:
        """把上次掉电残留的半截临时文件隔离留证（它们从未 replace 成活文件）。"""

        for name in sorted(os.listdir(self._root)):
            if name.startswith(".") and name.endswith(".tmp"):
                stale = os.path.join(self._root, name)
                if os.path.isfile(stale):
                    durable.quarantine(self._root, stale, "掉电残留的未完成写入临时文件")
