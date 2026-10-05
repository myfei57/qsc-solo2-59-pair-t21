"""落盘内容的校验和封装。

掉电留下的"半截"靠两层识别：原子文档靠临时文件 + 同盘 rename（坏的根本不会
以正式名出现），流水帧和文档体则统一带 CRC32 校验——长度或校验有一处对不上，
就能在解析之前给出"这是坏文件"的确定结论，而不是让 json 解析栈炸穿服务。

校验对**规范化 JSON**计算：文档落盘可以 pretty-print 方便巡检，但校验只认内容
口径，不受缩进差异影响。
"""

from __future__ import annotations

import json
import zlib
from typing import Any

MAGIC = "BSV1"


def canonical_payload(payload: Any) -> bytes:
    """统一字节口径：紧凑分隔、键排序，保证校验稳定。"""

    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return text.encode("utf-8")


def checksum(payload: Any) -> str:
    """计算规范化内容的 CRC32（8 位十六进制）。"""

    return f"{zlib.crc32(canonical_payload(payload)) & 0xFFFFFFFF:08x}"


def is_intact(payload: Any, expected: str) -> bool:
    """内容与记录的校验是否一致。"""

    try:
        return bool(expected) and checksum(payload) == expected.lower()
    except (TypeError, ValueError):
        return False
