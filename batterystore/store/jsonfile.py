"""原子 JSON 文档存储。

每个文档一个 ``<doc_id>.json`` 文件。写入遵循"要么成、要么不成"的崩溃原子
协议，任何一步掉电都不会把半截内容留在正式文件名下：

1. 同目录临时文件写入完整内容（自带 CRC32 校验）；
2. ``flush + fsync`` 把数据刷盘；
3. ``os.replace`` 同盘原子改名覆盖正式文件（rename 原子，读到的永远是上一份
   完整文档或新一份完整文档，不会读到中间态）；
4. fsync 目录，保证 rename 本身也落盘。

读取先校验再解析：半截、JSON 非法、校验不符都在进入业务解析之前被拦下，转成
:class:`~batterystore.errors.DocumentCorruptError`，并把坏文件**原样隔离**到
``.quarantine``，服务得到的是一个确定结论而不是栈崩。

坏文件默认只隔离、不重建（可追溯）。重建只有在同时满足以下前提时才允许，且
必须由调用方显式调用 :meth:`rebuild`：

* 已经有一份可信的、可验证的内容来源（如从流水/副本重放得出），不是猜测；
* 重建来源经调用方按业务规则确认有效；
* 坏文件已经隔离留底（重建绝不原地覆盖证据）；
* 文档是**派生状态**而非唯一事实来源——唯一事实来源（流水原件）永不重建。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from typing import Any

from ..errors import DocumentCorruptError, StorageError
from .framing import MAGIC, checksum, is_intact
from .quarantine import list_quarantined, quarantine_file
from .syncio import fsync_dir

TMP_PREFIX = ".doctmp."
TMP_SUFFIX = ".tmp"


class JsonStore:
    """目录形式的 JSON 文档存储。"""

    def __init__(self, root: str) -> None:
        self._root = os.path.abspath(root)
        self._lock = threading.RLock()
        os.makedirs(self._root, exist_ok=True)
        self._sweep_tmpfiles()

    @property
    def root(self) -> str:
        return self._root

    def path_of(self, doc_id: str) -> str:
        """返回某文档对应的文件路径。"""

        return os.path.join(self._root, f"{doc_id}.json")

    def exists(self, doc_id: str) -> bool:
        """文档文件是否存在（隔离区里的坏文件不算存在）。"""

        return os.path.isfile(self.path_of(doc_id))

    def write(self, doc_id: str, payload: Any) -> str:
        """原子写入一个文档，返回最终文件路径。

        成功返回后内容必然完整可见；任何中途失败正式文件都保持上一份不变，
        临时残片在下次打开存储时由 :meth:`_sweep_tmpfiles` 清走。
        """

        target = self.path_of(doc_id)
        with self._lock:
            tmp_name = ""
            try:
                handle = tempfile.NamedTemporaryFile(
                    "w",
                    encoding="utf-8",
                    dir=self._root,
                    prefix=TMP_PREFIX,
                    suffix=TMP_SUFFIX,
                    delete=False,
                )
                tmp_name = handle.name
                envelope = self._envelope(payload)
                with handle:
                    json.dump(envelope, handle, ensure_ascii=False, indent=2)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp_name, target)
                fsync_dir(self._root)
            except OSError as error:
                if tmp_name:
                    self._discard(tmp_name)
                raise StorageError(f"写入文档 {doc_id} 失败: {error}", component="store") from error
        return target

    def read(self, doc_id: str) -> Any | None:
        """读取一个文档。

        不存在返回 ``None``；发现坏文件时把它原样隔离并抛出
        :class:`DocumentCorruptError`（含隔离路径），绝不把解析异常泄给服务。
        """

        target = self.path_of(doc_id)
        with self._lock:
            if not os.path.isfile(target):
                return None
            payload, _envelope = self._load_verified(target, doc_id)
            return payload

    def read_or_default(self, doc_id: str, default: Any) -> Any:
        """读取文档；损坏时隔离坏文件并返回调用方给的默认值。

        给"读不到就按初始状态起步"的恢复路径使用。隔离动作照常发生，可通过
        :meth:`quarantined` 巡检。
        """

        with self._lock:
            try:
                return self.read(doc_id)
            except DocumentCorruptError:
                return default

    def rebuild(self, doc_id: str, source: Any) -> str:
        """按可信来源重建文档，返回新文件路径。

        前提：坏文件已被隔离（由先前的读取触发），这里只原子写入新文件，绝不
        原地覆盖证据。若正式文件仍在（即并未发生损坏），拒绝重建以免误覆盖。
        """

        target = self.path_of(doc_id)
        with self._lock:
            if os.path.isfile(target):
                raise StorageError(
                    f"文档 {doc_id} 仍有正式文件，拒绝原地重建；应先读取确认并隔离",
                    component="store",
                )
            path = self.write(doc_id, source)
        return path

    def remove(self, doc_id: str) -> bool:
        """删除一个文档，返回是否真的删掉了文件。"""

        target = self.path_of(doc_id)
        with self._lock:
            if not os.path.isfile(target):
                return False
            os.remove(target)
            fsync_dir(self._root)
            return True

    def list_ids(self) -> list[str]:
        """列出全部文档标识（按文件名排序）。"""

        with self._lock:
            names = [name for name in os.listdir(self._root) if name.endswith(".json")]
        return sorted(name[: -len(".json")] for name in names)

    def quarantined(self) -> list[dict]:
        """列出隔离区中的坏文件（原始路径、原因、隔离时间）。"""

        with self._lock:
            return list_quarantined(self._root)

    def _load_verified(self, target: str, doc_id: str) -> tuple[Any, dict]:
        """读取并校验一个文档；任何对"这是一份完整有效内容"的违背都隔离并报错。"""

        try:
            with open(target, "r", encoding="utf-8") as handle:
                raw = handle.read()
            envelope = json.loads(raw)
        except OSError as error:
            raise StorageError(f"读取文档 {doc_id} 失败: {error}", component="store") from error
        except json.JSONDecodeError as error:
            raise self._corrupt_error(
                target, doc_id, "json-decode-error", f"JSON 解析失败（疑似半截）: {error}"
            ) from error

        # 升级前写的旧式裸 JSON 文档没有校验封装：无法背书其完整性，按原样放行，
        # 下一次 write 会自动转成带校验的新格式。
        if not isinstance(envelope, dict):
            return envelope, {"legacy": True}
        if "magic" not in envelope and "crc32" not in envelope and "payload" not in envelope:
            return envelope, {"legacy": True}

        if envelope.get("magic") != MAGIC or "crc32" not in envelope or "payload" not in envelope:
            raise self._corrupt_error(
                target, doc_id, "bad-envelope", f"封装字段缺失或魔数不符: {envelope.get('magic')!r}"
            )
        payload = envelope["payload"]
        if not is_intact(payload, str(envelope["crc32"])):
            raise self._corrupt_error(
                target, doc_id, "checksum-mismatch", "CRC32 校验不符，内容不是完整有效的一份"
            )
        return payload, envelope

    def _corrupt_error(self, target: str, doc_id: str, reason: str, detail: str) -> DocumentCorruptError:
        """把坏文件原样隔离，并构造对应的确定结论异常（由调用方抛出）。"""

        qpath, _meta = quarantine_file(
            self._root, target, reason=reason, detail=detail, extra={"doc_id": doc_id}
        )
        return DocumentCorruptError(
            f"文档 {doc_id} 已损坏（{reason}），坏文件已隔离: {qpath or '(已处理)'}",
            path=target,
            quarantine_path=qpath,
            reason=reason,
        )

    def _envelope(self, payload: Any) -> dict:
        return {"magic": MAGIC, "crc32": checksum(payload), "payload": payload}

    def _sweep_tmpfiles(self) -> None:
        """清理上一次掉电前没来得及改名的临时残片（它们从未对读者可见）。"""

        for name in os.listdir(self._root):
            if name.startswith(TMP_PREFIX) and name.endswith(TMP_SUFFIX):
                self._discard(os.path.join(self._root, name))

    @staticmethod
    def _discard(path: str) -> None:
        try:
            os.remove(path)
        except OSError:
            pass
