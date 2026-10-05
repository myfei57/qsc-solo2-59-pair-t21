# 掉电安全的持久化：原子文档 + 校验封口流水

## 要解决的现场问题

1. 掉电后出现半截文档，读取阶段解析炸掉，整机被迫重启；
2. 流水滚段在边界上掉电，重启后分不清最后一段是完整还是残缺，容易把残缺段
   当完整段接着用；
3. 读写并发时，读者可能读到写了一半的记录。

## 总体策略

### 1. 写入：要么成，要么不成（没有"写了一半的活文件"）

* **文档**（`store/jsonfile.py`）：`临时文件 fsync → os.replace → 目录 fsync`。
  掉电落在任何一步，盘上都只会是旧的完整文件或新的完整文件。
  * 上次掉电残留的 `.tmp` 在下次启动时被移进隔离区，不就地删除。
* **流水帧**（`store/journal.py`）：`append + fsync` 保证"返回即完整落盘"；
  每条记录带 CRC32 与全局单调序号，半截帧一验即知。
* **滚段封口**：一段写满后先向段尾追加封口帧（`kind=seal`，同样带 CRC）并
  fsync，再推进段号。封口帧是"这段完整"的唯一凭证。

### 2. 坏文件处理：默认隔离，重建窄条件放行

**默认策略是隔离保留原样**（`.quarantine/` + `manifest.jsonl` 证据清单），
坏原件永不就地覆盖，可追溯。

允许重建的唯二情形：

* **文档**：不做任何自动重建。只有调用方拿着**自己校验过的"最后一次有效
  内容"**显式调用 `JsonStore.recover_document` / 平台 `POST /api/recover-state`
  才重建；活文件仍然健康时直接拒绝，防止覆盖好数据。
* **流水段**：由配置 `station.journal_on_damage` 决定（默认 `quarantine`）。
  只有显式配成 `rebuild` 时才重建，且重建只做一件事——**截到该段最后一个
  CRC 校验通过的帧并重新封口**。只许少、不许编造；坏原件先隔离留证，后段
  完整记录不受影响。

### 3. 残缺段 ≠ 损坏段（滚段边界掉电的专门结论）

启动恢复时，对最高位段放宽"最后一行"的 CRC 校验以捞回有效前缀，并据此
分类：

| 盘上现象 | 结论 | 处理 |
| --- | --- | --- |
| 每帧 CRC 合法、封口齐全 | 完整段 | 正常读 |
| 封口帧只写了一半（段已写满，下一段已存在） | **残缺段：滚段边界掉电** | 原件移 `.partial/`，写元数据，下一段继续 |
| 最高位段最后一条记录帧截断 | **残缺段：记录写入中掉电** | 有效前缀入 `.partial/`，新段从新段号写 |
| 段中部出现坏帧 / 封口后还有数据 / 非末段无封口 | **损坏段** | 按 `on_damage` 隔离或重建 |
| 空段文件（创建后一帧没写） | **残缺段：空壳** | 入 `.partial/` |

残缺段**单独标记、单独目录、单独段号**：

* 原件原样移到 `<root>/.partial/<时间戳>-<topic>-NNNNN.jsonl`；
* 同目录写一份 `<topic>-NNNNN.partial.json` 元数据（原因、有效前缀条数、
  有效记录内容、归档路径）；
* 段号**永不复用**，活动段推进到新段号接着写，段号空档保持可见，残缺段
  不会被悄悄当成完整一段；
* 隔离区清单同步记一条证据。

### 4. 并发：读者永远看到完整的一份

所有读写共用同一把可重入锁；读到的要么是 fsync 前的旧内容，要么是 fsync 后
的完整新帧。读者只在**当前活动段**容忍"最后一帧正在写"（返回已落盘前缀），
历史段任何坏帧都直接给损坏结论。

### 5. 读：给明确结论，不把服务带崩

* 新增异常 `CorruptStorageError(StorageError)`，携带隔离证据路径；
* 文档损坏：隔离 + 抛明确异常，平台 `_restore` 抓住它以默认值启动并发告警
  事件（`state_document_quarantined`），不再整机重启；
* 流水读取分三档：
  * `read_all()` / `GET /api/journal?strict=1`：要完整流水，缺段直接报
    `CorruptStorageError`（HTTP 409，错误类型明确），不静默给缺段数据；
  * `replay()` / 审计回放：启动恢复用，包含完整活段与残缺段有效前缀，跳过
    隔离段，同时返回 `health` 让调用方知道这份回放是否完整；
  * `health()` / `GET /api/storage-health`：完整段、残缺段、隔离段、策略、
    证据路径一目了然。
* 证据清单本身也是追加写，读取时容忍它自己的最后一行被截断。

## 目录布局

```
<storage_root>/
  <doc>.json                    # 原子文档活文件
  <topic>-NNNNN.jsonl           # 流水活段（封口齐全或当前活动段）
  .quarantine/
    manifest.jsonl              # 全部隔离/重建证据（追加写）
    <时间戳>-<原名>             # 坏文档 / 坏段原件
  .partial/
    <时间戳>-<topic>-NNNNN.jsonl  # 残缺段原件
    <topic>-NNNNN.partial.json    # 残缺段元数据与有效前缀
```

## 关键 API

* `JsonStore.write/read/inspect/quarantine_report/recover_document`
* `JsonlJournal.append/read_all/read_segment/read_partial/replay/health`
* 配置 `station.journal_on_damage`：`quarantine`（默认）/ `rebuild`
* 控制台：`GET /api/journal[?strict=1&segment=N&partial=N]`、
  `GET /api/storage-health`、`POST /api/recover-state`

## 何时允许重建（运维口径）

1. 文档：只在业务侧已确认"最后一次有效内容"（例如来自上级系统、对端副本或
   人工核对）后，走显式重建接口；
2. 流水：现场能接受"丢掉坏帧之后的尾段"、且希望服务免人工继续时，配置
   `journal_on_damage=rebuild`；需要完整审计可追溯时保持默认 `quarantine`，
   坏段留盘等人工处理。
