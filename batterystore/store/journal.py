"""带帧校验与封口的分段 JSONL 流水。

每条记录编码成一帧（一行）::

    {"v": <业务记录>, "seq": 全局序号, "crc": "<crc32 十六进制>"}

CRC 覆盖帧体（``v`` 与 ``seq``）的规范化 JSON 字节，掉电产生的半截行一验即知。

**滚段封口**：一段写满 ``segment_limit`` 条后，先向段尾追加一帧封口记录
（``{"kind": "seal", ...}``）并 fsync，之后才开启下一段。因此重启时有三种结论：

* 段尾有合法封口 → 完整段；
* 最高位段没有封口（含记录全部有效但没来得及封口、以及封口行被截断）→
  **残缺段**：原件原样搬到 ``.partial/`` 留证，另写元数据，下一段从新段号
  接着写，绝不把它悄悄当成完整的一段；
* 段中部出现坏帧、封口之后还有数据、序号不连续 → 损坏段，按 ``on_damage``
  策略处理。

``on_damage`` 两种策略：

* ``"quarantine"``（默认）：坏段原样移到 ``.quarantine/`` 隔离，等人工处理；
* ``"rebuild"``：只允许"截到最后一个校验通过的帧"这一种重建——把有效前缀
  重新封口写回，坏原件仍先隔离留证。只许截断，不许编造任何内容。

并发：所有读写共用同一把锁；读到的要么是写者 fsync 之前的整段旧内容，要么是
fsync 之后的完整新帧，不会读到半行。
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
import zlib
from typing import Any

from ..errors import CorruptStorageError, StorageError
from . import durable

MAX_SEGMENTS = 4096
PARTIAL_DIR = ".partial"
QUARANTINE_POLICY = "quarantine"
REBUILD_POLICY = "rebuild"
_POLICIES = (QUARANTINE_POLICY, REBUILD_POLICY)


def _crc(body: dict) -> str:
    canonical = json.dumps(body, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return f"{zlib.crc32(canonical.encode('utf-8')) & 0xFFFFFFFF:08x}"


def _record_body(seq: int, payload: Any) -> dict:
    return {"v": payload, "seq": seq}


def _seal_body(seq: int) -> dict:
    return {"kind": "seal", "v": None, "seq": seq}


def _encode(body: dict) -> bytes:
    frame = dict(body)
    frame["crc"] = _crc(body)
    return (json.dumps(frame, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def _decode_frame(line: bytes, *, lenient_tail: bool = False) -> tuple[dict, bool] | None:
    """校验并解析一帧；任何不符返回 ``None``（不抛异常）。

    返回 ``(frame, crc_ok)``：``crc_ok`` 为真表示 CRC 严格通过；为假仅出现在
    ``lenient_tail`` 放宽捞回"最高位段最后一行"时——行的帧结构还在但 CRC/收尾
    已被掉电截断。放宽只作用于启动恢复的那一行，不影响日常读取的严格校验。
    """

    crc_ok = True
    try:
        frame = json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        if not lenient_tail:
            return None
        crc_ok = False
        text = line.decode("utf-8", errors="ignore")
        # 半截行的 JSON 外层结构被截断，但业务内容（嵌套在 "v" 中）通常还在。
        # 截到 "crc" 字段之前（该字段是帧的最后一个 key），把行补成合法 JSON。
        cut = max(text.rfind(', "crc"'), text.rfind(',"crc"'))
        if cut <= 0:
            return None
        try:
            frame = json.loads(text[:cut] + "}")
        except json.JSONDecodeError:
            return None
    if not isinstance(frame, dict) or "v" not in frame or not isinstance(frame.get("seq"), int):
        return None
    body = {key: value for key, value in frame.items() if key != "crc"}
    if crc_ok:
        if not isinstance(frame.get("crc"), str) or _crc(body) != frame["crc"]:
            if not lenient_tail:
                return None
            crc_ok = False
    if body["seq"] < 0:
        return None
    if body.get("kind") == "seal":
        if set(body) != {"kind", "v", "seq"} or body["v"] is not None:
            return None
        return body, crc_ok
    if "kind" in body or set(body) != {"v", "seq"}:
        return None
    return body, crc_ok


class JsonlJournal:
    """按段落写入、带校验与封口的 JSONL 流水。"""

    def __init__(self, root: str, topic: str, segment_limit: int = 512, on_damage: str = QUARANTINE_POLICY) -> None:
        if on_damage not in _POLICIES:
            raise StorageError(
                f"流水损坏策略必须是 {_POLICIES} 之一，收到 {on_damage!r}", component="store"
            )
        self._root = os.path.abspath(root)
        self._topic = topic
        self._segment_limit = max(1, segment_limit)
        self._on_damage = on_damage
        self._lock = threading.RLock()
        self._count = 0
        self._segment = 0
        self._active_count = 0
        self._highest_seq = 0
        self._partial: list[dict] = []
        self._quarantined: list[dict] = []
        os.makedirs(self._root, exist_ok=True)
        self._recover()

    @property
    def topic(self) -> str:
        return self._topic

    def _segment_path(self, index: int) -> str:
        return os.path.join(self._root, f"{self._topic}-{index:05d}.jsonl")

    @property
    def _partial_dir(self) -> str:
        return os.path.join(self._root, PARTIAL_DIR)

    # ---------------------------------------------------------------- 写入

    def append(self, record: Any) -> str:
        """原子追加一条记录并封口滚动，返回所在段路径。

        帧字节与封口在一次 append/fsync 内完成；返回后该帧必然完整落盘。
        """

        with self._lock:
            try:
                if self._segment >= MAX_SEGMENTS:
                    raise StorageError(
                        f"流水段号超过 {MAX_SEGMENTS} 上限，拒绝继续追加（需人工归档旧段）",
                        component="store",
                    )
                if self._active_count >= self._segment_limit:
                    self._roll()
                path = self._segment_path(self._segment)
                seq = self._highest_seq + 1
                with open(path, "ab") as handle:
                    handle.write(_encode(_record_body(seq, record)))
                    handle.flush()
                    os.fsync(handle.fileno())
            except (OSError, TypeError, ValueError) as error:
                raise StorageError(f"追加流水失败: {error}", component="store") from error
            self._count += 1
            self._highest_seq = seq
            self._active_count += 1
            return path

    def _roll(self) -> None:
        """把当前段写满封口，再把活动段号推进到下一段。

        先封口 fsync，再推进段号；掉电在封口完成前 → 旧段被重启认成残缺段；
        掉电在封口完成后 → 旧段已是完整段。两种情况都不会张冠李戴。
        """

        sealed = self._segment_path(self._segment)
        with open(sealed, "ab") as handle:
            handle.write(_encode(_seal_body(self._highest_seq)))
            handle.flush()
            os.fsync(handle.fileno())
        self._segment += 1
        self._active_count = 0

    # ---------------------------------------------------------------- 读取

    def count(self) -> int:
        """累计确认过的记录条数（活段 + 残缺段有效前缀；隔离段不计）。

        滚段后调用方能看到单调不减的计数；残缺段的记录算"已落盘但带标记"，
        通过 :meth:`read_partial` / :meth:`replay` 取。
        """

        with self._lock:
            return self._count

    def active_segment(self) -> int:
        """返回当前正在写入的段号。"""

        with self._lock:
            return self._segment

    def segments(self) -> list[str]:
        """返回活段文件路径（按段号升序；被隔离的段会形成空档）。"""

        with self._lock:
            return [item["path"] for item in self._live_segments()]

    def read_segment(self, index: int) -> list[dict]:
        """读取一个活段的记录。

        段号超出范围返回 ``[]``；该段已被隔离损坏时抛
        :class:`CorruptStorageError` 给出明确结论；残缺段不走这里，用
        :meth:`read_partial` 显式读取。
        """

        with self._lock:
            live = {item["index"]: item for item in self._live_segments()}
            if index not in live:
                if any(item["index"] == index for item in self._quarantined):
                    evidence = next(item for item in self._quarantined if item["index"] == index)
                    raise CorruptStorageError(
                        f"流水段 {index} 已损坏并隔离，证据位于 {evidence['archived']}",
                        quarantine_path=evidence["archived"],
                    )
                return []
            frames = self._read_live_frames(live[index]["path"], active=index == self._segment)
            return [frame["v"] for frame in frames if frame.get("kind") != "seal"]

    def read_all(self) -> list[dict]:
        """按段号顺序读取全部活段记录。

        存在损坏空档时不静默跳过，抛 :class:`CorruptStorageError`，避免拿一份
        缺段的数据冒充完整流水；启动回放请用 :meth:`replay`。
        """

        with self._lock:
            self._assert_no_corruption()
            records: list[dict] = []
            for item in self._live_segments():
                frames = self._read_live_frames(item["path"], active=item["index"] == self._segment)
                records.extend(frame["v"] for frame in frames if frame.get("kind") != "seal")
            return records

    def replay(self) -> dict:
        """跨重启回放：返回记录与本次恢复的完整健康结论。

        记录包含完整活段（含按策略重建的段）与残缺段的有效前缀，按段号顺序；
        被隔离且未重建的损坏段**不进入记录**，原件在隔离区等人工处理。调用方
        必须依据 ``health`` 里的 ``corrupt`` 决定能否信任这份回放。
        """

        with self._lock:
            live_indices = {item["index"] for item in self._live_segments()}
            records: list[tuple[int, list]] = []
            for item in self._live_segments():
                frames = self._read_live_frames(item["path"], active=item["index"] == self._segment)
                records.append((item["index"], [f["v"] for f in frames if f.get("kind") != "seal"]))
            for item in self._partial:
                records.append((item["index"], list(item["records"])))
            ordered: list = []
            for _index, rows in sorted(records):
                ordered.extend(rows)
            health = self.health()
            health["replayed_segment_indices"] = sorted(live_indices | {p["index"] for p in self._partial})
            return {"records": ordered, "health": health}

    def read_partial(self, index: int) -> list[dict]:
        """显式读取某个残缺段的有效前缀。"""

        with self._lock:
            for item in self._partial:
                if item["index"] == index:
                    return list(item["records"])
            return []

    def health(self) -> dict:
        """返回流水的明确健康结论，供控制台/巡检直接展示。"""

        with self._lock:
            live_indices = {item["index"] for item in self._live_segments()}
            open_damage = [dict(item) for item in self._quarantined if item["index"] not in live_indices]
            return {
                "topic": self._topic,
                "active_segment": self._segment,
                "count": self._count,
                "on_damage": self._on_damage,
                "segments": [
                    {"index": item["index"], "path": item["path"], "sealed": item["sealed"]}
                    for item in self._live_segments()
                ],
                "partial_segments": [dict(item, records=len(item["records"])) for item in self._partial],
                "quarantined_segments": [dict(item) for item in self._quarantined],
                "corrupt": bool(open_damage),
            }

    # -------------------------------------------------------- 启动恢复

    def _recover(self) -> None:
        live = self._discover()
        # 最高的"有数据"段才是真正的末段；其后的空文件是掉电时创建出来却没写
        # 成的活动段残壳。没有任何有数据段时（全空），把第一个空文件当末段。
        last_data = -1
        for pos, item in enumerate(live):
            if os.path.getsize(item["path"]) > 0:
                last_data = pos
        if last_data < 0 and live:
            last_data = 0
        empty_artifacts = {
            pos
            for pos, item in enumerate(live)
            if os.path.getsize(item["path"]) == 0 and pos != last_data
        }
        scans = [
            self._scan_file(item["path"], salvage_tail=pos == last_data and pos not in empty_artifacts)
            for pos, item in enumerate(live)
        ]
        recognized = 0
        highest_seq = 0
        for position, (item, scan) in enumerate(zip(live, scans)):
            is_last = position == last_data
            valid = self._validated(scan["frames"], highest_seq)
            if valid:
                highest_seq = valid[-1]["seq"]
            if position in empty_artifacts:
                self._archive_partial(item, [], reason_override="活动段打开后尚未写入即掉电（空段残壳）")
                continue
            recognized += len(valid)
            if scan["sealed"]:
                continue
            if scan["corrupt"] == "空段文件":
                # 此时必为 last_data（没有任何有数据段）：整个流水唯一的文件是空壳。
                self._archive_partial(item, [], reason_override="活动段打开后尚未写入即掉电（空段）")
                recognized = 0
                continue
            if scan["corrupt"] is not None:
                rebuilt = self._handle_damage(item, scan, valid, scan["corrupt"])
                if not rebuilt:
                    recognized -= len(valid)  # 隔离段的有效前缀不作数，证据里查
                continue
            if scan["tail_bad"]:
                boundary = not is_last
                self._archive_partial(item, valid, boundary_seal=boundary)
            elif not is_last:
                rebuilt = self._handle_damage(item, scan, valid, "非末段缺少封口")
                if not rebuilt:
                    recognized -= len(valid)
            # else: 完整的最高位活动段（干净停机），留在活段里继续写。
        # 段号永不复用。活动段的选择：
        #   * 最高位活段未封口（干净停机后的完整活动段）→ 继续写它本身；
        #   * 最高位活段已封口，或末段被归档（残缺/隔离）→ 活动段推进到
        #     扫描最大段号之后的全新段，残缺段/隔离段留下的空档保持可见。
        surviving = self._discover()
        self._segment = live[-1]["index"] + 1 if live else 0
        self._active_count = 0
        if surviving:
            last = surviving[-1]
            scan = self._scan_file(last["path"])
            if not scan["sealed"] and scan["corrupt"] is None:
                self._segment = last["index"]
                self._active_count = sum(1 for f in scan["frames"] if f.get("kind") != "seal")
        # count：活段 + 残缺段有效前缀；highest_seq：含残缺段，保证后续序号不复用。
        self._count = recognized
        self._highest_seq = highest_seq

    def _discover(self) -> list[dict]:
        pattern = re.compile(rf"^{re.escape(self._topic)}-(\d{{5}})\.jsonl$")
        found: list[dict] = []
        for name in sorted(os.listdir(self._root)):
            match = pattern.match(name)
            if not match:
                continue
            found.append({"index": int(match.group(1)), "path": os.path.join(self._root, name)})
        found.sort(key=lambda item: item["index"])
        return found

    def _live_segments(self) -> list[dict]:
        items = self._discover()
        active = self._segment
        for item in items:
            # 活动段允许末帧处于"写了一半"的并发状态，扫描时放宽最后一行；
            # 历史段严格校验，封口后任何坏帧都算损坏。
            scan = self._scan_file(item["path"], salvage_tail=item["index"] == active)
            item["sealed"] = scan["sealed"] and scan["corrupt"] is None
        return items

    def _validated(self, frames: list[dict], previous_seq: int) -> list[dict]:
        """取本段中序号严格大于前段最高序号的合法业务帧。

        扫描器已按 CRC 过滤坏帧，这里只做跨段衔接：掉电损坏导致前段丢帧时，
        后段记录的序号会大于"被截断前缀"的预期值，但它们本身完整可信，必须
        保留下来；重建时前段截到自己的有效前缀即可，不影响后段。只在出现
        倒序/重复时截断。
        """

        valid: list[dict] = []
        for frame in frames:
            if frame.get("kind") == "seal":
                break
            if frame["seq"] <= previous_seq:
                break
            valid.append(frame)
            previous_seq = frame["seq"]
        return valid

    def _handle_damage(self, item: dict, scan: dict, valid: list[dict], reason: str) -> bool:
        """处理损坏段，返回该段是否按 rebuild 策略重建为活段。"""

        archived = durable.quarantine(
            self._root, item["path"], f"流水段损坏: {reason}", topic=self._topic, segment=item["index"]
        )
        info = {"index": item["index"], "archived": archived, "reason": reason, "records_valid": len(valid)}
        rebuilt = False
        if self._on_damage == REBUILD_POLICY:
            rebuilt_path = self._install_rebuilt(item["index"], valid)
            info["rebuilt_path"] = rebuilt_path
            rebuilt = True
        self._quarantined.append(info)
        return rebuilt

    def _install_rebuilt(self, index: int, frames: list[dict]) -> str:
        """用有效前缀重建段并封口（临时文件原子替换，证据原件不受影响）。"""

        target = self._segment_path(index)
        handle = tempfile.NamedTemporaryFile(
            "wb", dir=self._root, prefix=f".{self._topic}-{index:05d}.", suffix=".rebuild", delete=False
        )
        temp_path = handle.name
        try:
            with handle:
                for frame in frames:
                    body = _record_body(frame["seq"], frame["v"])
                    handle.write(_encode(body))
                # 空前缀也写封口：封口 seq 取该段最后一个有效序号（0 表示该段
                # 一条有效记录都没有），后续段靠全局序号自证，不会借它凭空造记录。
                handle.write(_encode(_seal_body(frames[-1]["seq"] if frames else 0)))
                handle.flush()
                os.fsync(handle.fileno())
            durable.atomic_install(temp_path, target)
        except OSError as error:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise StorageError(f"重建流水段 {index} 失败: {error}", component="store") from error
        return target

    def _archive_partial(
        self,
        item: dict,
        valid: list[dict],
        *,
        boundary_seal: bool = False,
        reason_override: str | None = None,
    ) -> None:
        """把残缺段原件搬到 .partial/ 并写元数据，活段号留给下一段。"""

        os.makedirs(self._partial_dir, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        raw_name = f"{stamp}-{self._topic}-{item['index']:05d}.jsonl"
        archived = os.path.join(self._partial_dir, raw_name)
        counter = 1
        while os.path.exists(archived):
            archived = os.path.join(self._partial_dir, f"{stamp}.{counter}-{self._topic}-{item['index']:05d}.jsonl")
            counter += 1
        os.rename(item["path"], archived)
        durable.fsync_dir(self._root)
        durable.fsync_dir(self._partial_dir)
        reason = reason_override or (
            "滚段边界掉电，封口帧残缺"
            if boundary_seal
            else "末段缺少封口（掉电时正在写）"
        )
        records = [frame["v"] for frame in valid]
        meta = {
            "topic": self._topic,
            "index": item["index"],
            "reason": reason,
            "archived_raw": archived,
            "records_valid": len(records),
            "records": records,
            "ts": round(time.time(), 3),
        }
        self._write_partial_meta(item["index"], meta)
        self._partial.append(
            {
                "index": item["index"],
                "archived": archived,
                "reason": reason,
                "records": records,
            }
        )
        durable.append_manifest(
            self._root,
            {
                "ts": meta["ts"],
                "source": item["path"],
                "archived": archived,
                "reason": f"残缺段已单独归档: {reason}",
                "topic": self._topic,
                "segment": item["index"],
            },
        )

    def _write_partial_meta(self, index: int, meta: dict) -> None:
        target = os.path.join(self._partial_dir, f"{self._topic}-{index:05d}.partial.json")
        handle = tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self._partial_dir,
            prefix=f".{self._topic}-{index:05d}.",
            suffix=".tmp",
            delete=False,
        )
        temp_path = handle.name
        try:
            with handle:
                json.dump(meta, handle, ensure_ascii=False, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            durable.atomic_install(temp_path, target)
        except OSError as error:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise StorageError(f"记录残缺段 {index} 元数据失败: {error}", component="store") from error

    def _assert_no_corruption(self) -> None:
        live_indices = {item["index"] for item in self._live_segments()}
        # 隔离但已按策略重建成功的段重新算活段；只有"坏了且没重建"的才挡读。
        open_damage = [item for item in self._quarantined if item["index"] not in live_indices]
        if open_damage:
            first = open_damage[0]
            raise CorruptStorageError(
                f"流水存在 {len(open_damage)} 个损坏隔离段，"
                f"首个为段 {first['index']}（证据 {first['archived']}）；"
                "完整流水不可用，请查 health() 或走显式回放",
                quarantine_path=first["archived"],
            )
        indices = set(live_indices) | {item["index"] for item in self._partial}
        if live_indices:
            lo, hi = min(indices), max(live_indices)
            missing = sorted(index for index in range(lo, hi + 1) if index not in indices)
            if missing:
                raise CorruptStorageError(
                    f"流水段号不连续，缺失段 {missing}（既非活段也非残缺段，疑似损坏被移走），"
                    "已不适合当作完整流水读取",
                )

    def _read_live_frames(self, path: str, *, active: bool) -> list[dict]:
        """读一个活段。

        正常运行时读者可能与写者并发：只有**当前活动段**的最后一帧可能因尚未
        写完而截断，此时返回已完整落盘的前缀；历史段必须封口完好，任何坏帧都
        直接给出损坏结论。
        """

        with open(path, "rb") as handle:
            data = handle.read()
        scan = self._scan_file(path, data=data)
        if scan["corrupt"] is not None:
            raise CorruptStorageError(f"流水段 {path} 校验失败: {scan['corrupt']}")
        if scan["tail_bad"] and not active:
            raise CorruptStorageError(f"流水段 {path} 已封口却存在无法校验的尾帧，疑似掉电损坏")
        return scan["frames"]

    def _scan_file(self, path: str, data: bytes | None = None, *, salvage_tail: bool = False) -> dict:
        """扫描段字节，返回帧序列、是否封口、末帧是否截断、损坏原因。"""

        if data is None:
            with open(path, "rb") as handle:
                data = handle.read()
        if not data:
            return {"frames": [], "sealed": False, "tail_bad": False, "corrupt": "空段文件"}
        raw_lines = data.split(b"\n")
        # 末尾的 \n 会产生一个空尾串，属正常；其它空行不是我们写出的格式。
        if raw_lines and raw_lines[-1] == b"":
            raw_lines.pop()
        frames: list[dict] = []
        first_bad: int | None = None
        last_good = -1
        last_index = len(raw_lines) - 1
        middle_corrupt = False
        tail_salvaged = False
        for position, raw in enumerate(raw_lines):
            if not raw.strip():
                if first_bad is None:
                    first_bad = position
                continue
            decoded = _decode_frame(raw, lenient_tail=salvage_tail and position == last_index)
            if decoded is None:
                if first_bad is None:
                    first_bad = position
                continue
            frame, crc_ok = decoded
            if not crc_ok:
                # 仅放宽读取的最后一行可能走到这里：帧结构在、CRC 没过，是
                # 掉电截断的确凿证据，仍计入有效前缀，但必须留下残缺标记。
                tail_salvaged = True
            if first_bad is not None and not (salvage_tail and position == last_index):
                # 坏帧之后又出现合法帧：坏帧确实在段中部。这些帧不再算有效
                # （含其后的封口帧——损坏段不能靠一个还在的封口冒充完整段），
                # 只标记损坏，整段交给隔离/重建策略处理。
                middle_corrupt = True
                continue
            frames.append(frame)
            last_good = position
        tail_bad = tail_salvaged or (first_bad is not None and first_bad > last_good)
        corrupt: str | None = None
        if middle_corrupt:
            corrupt = "坏帧位于段中部，其后帧与封口一律不计"
        elif first_bad is not None and not tail_bad:
            corrupt = "段中部存在无法校验的帧"
        seal_index = next((i for i, f in enumerate(frames) if f.get("kind") == "seal"), None)
        sealed = seal_index is not None
        if seal_index is not None and seal_index != len(frames) - 1:
            corrupt = "封口帧之后仍有数据"
        return {"frames": frames, "sealed": sealed, "tail_bad": tail_bad, "corrupt": corrupt}
