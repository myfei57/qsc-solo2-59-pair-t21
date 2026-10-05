"""同步刷盘原语。

数据 fsync 与目录 fsync 的持久性语义不同：rename 在多数文件系统上要等目录项
落盘才算数，否则掉电后新名字可能根本不存在。这里把"刷目录"封成一个可在不
支持该操作的文件系统上安全降级的小工具。
"""

from __future__ import annotations

import os


def fsync_dir(path: str) -> None:
    """fsync 一个目录，确保其中的 rename／create／unlink 落盘。"""

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        # 部分文件系统（某些网络盘／tmpfs 配置）不支持 fsync 目录，尽力即可，
        # 不能因此阻断正常写入。
        pass
    finally:
        os.close(fd)
