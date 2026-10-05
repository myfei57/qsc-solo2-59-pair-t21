"""文件型持久化：原子 JSON 文档与分段 JSONL 流水。

- :class:`JsonStore`：带 CRC 校验的原子文档，读坏文件给出确定结论并隔离。
- :class:`JsonlJournal`：带校验帧与封端帧的分段流水，能识别滚段边界上掉电
  留下的残缺段并单独登记。
"""

from __future__ import annotations

from .index import summarize_documents
from .journal import JsonlJournal
from .jsonfile import JsonStore

__all__ = ["JsonStore", "JsonlJournal", "summarize_documents"]
