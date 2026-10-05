# pairwise-mvcc

一个纯内存的 MVCC 事务引擎示例。只用 Python 标准库实现，不连接任何真实数据库，
所有状态都保存在进程内存里，用于观察快照读、读己之写、写写冲突、事务中止与版本
垃圾回收（gc）的行为。

## 目录结构

- `mvcc/core.py` — MVCC 事务内核：版本链、快照读、写写冲突、提交 / 中止、GC
- `tests/test_core.py` — 9 个 unittest 用例

## 公共接口

`MVCCStore.begin(read_only=False)` 开启事务；事务对象提供 `get` / `put` / `delete` /
`scan` / `commit` / `abort`。`MVCCStore.gc()` 回收版本，`MVCCStore.versions_of(key)`
与 `MVCCStore.active_txn_ids()` 用于观察引擎内部状态。

## 运行测试

在项目根目录（本文件所在目录）执行：

    python3 -m unittest discover -s tests -v

全部用例通过时进程退出码为 0。如果系统里 `python3` 不可用，把命令中的 `python3`
换成 `python` 即可（Windows 上常见）。
