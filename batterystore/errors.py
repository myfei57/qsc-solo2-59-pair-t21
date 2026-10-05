"""平台统一异常类型。

储能电站的失败必须按性质分开：顺序或联锁条件不满足属于可恢复的运行期拒绝；
消防相关异常属于安全侧，必须保持状态直到人工复位；配额不足属于记账侧拒绝；
配置与持久化失败则属于装配期就要拦下的接线问题。审计按异常类型分别归类，
巡检时才能一眼分清责任面。
"""

from __future__ import annotations


class StoreError(Exception):
    """所有平台异常的基类，携带出错的组件名。"""

    def __init__(self, message: str, *, component: str = "batterystore") -> None:
        super().__init__(message)
        self.message = message
        self.component = component

    def as_record(self) -> dict:
        """转成审计可以直接落盘的一条记录。"""

        return {
            "type": type(self).__name__,
            "component": self.component,
            "message": self.message,
        }


class InterlockError(StoreError):
    """顺序或联锁条件不满足，动作被拒绝。"""


class FireError(StoreError):
    """消防联动顺序失败或灭火介质未到位。"""


class QuotaError(StoreError):
    """充放电配额不足或跨周期记账失败。"""


class StorageError(StoreError):
    """持久化读写失败。"""


class CorruptStorageError(StorageError):
    """盘上内容校验不通过。

    与普通 IO 失败分开：这是"盘上已经有坏数据"的确定结论，调用方应当走隔离／
    重建分支，而不是当作一次可以重试的瞬态失败。坏文件位置与隔离去向随异常带出，
    读取入口永远给调用方一个明确说法，而不是让解析栈把服务带崩。
    """

    def __init__(
        self,
        message: str,
        *,
        component: str = "store",
        path: str = "",
        quarantine_path: str = "",
        reason: str = "corrupt",
    ) -> None:
        super().__init__(message, component=component)
        self.path = path
        self.quarantine_path = quarantine_path
        self.reason = reason

    def as_record(self) -> dict:
        record = super().as_record()
        record["path"] = self.path
        record["quarantine_path"] = self.quarantine_path
        record["reason"] = self.reason
        return record


class DocumentCorruptError(CorruptStorageError):
    """原子文档读取时发现内容损坏（半截、JSON 非法或校验不符）。"""


class SegmentCorruptError(CorruptStorageError):
    """流水段不是一份完整有效内容（残缺尾或校验不符）。"""


class ConfigError(StoreError):
    """配置缺失或取值非法。"""


class NamespaceError(StoreError):
    """命名空间重复注册或解析失败。"""
