# AGENTS.md — tests/unit/

## 适用范围
- 本文件规范 `tests/unit/` 目录的 Tcl 测试约定。
- 这些测试由根目录 `runtest` 暂存到 Valkey Tcl harness 后拉起 Rust 二进制验证。

## 职责
- 直接拉 `versal-server` 二进制做 Tcl 断言，验证命令行为与 RESP 协议兼容性。
- 与子模块中的 Valkey 主线 Tcl 用例不同，本目录用于 Rust 特有或先行验证场景。

## 文件列表
| 文件 | 覆盖域 | 说明 |
|------|--------|------|
| `aof-basic.tcl` | AOF 基础 | 验证 AOF 追加写与启动回放 |
| `dump-string.tcl` | DUMP/RESTORE | 字符串类型的序列化往返 |
| `monitor.tcl` | MONITOR | 对齐 C 版过滤、顺序、脱敏与连接生命周期语义 |
| `pubsub-basic.tcl` | 发布订阅 | 频道级 PUBLISH/SUBSCRIBE 基础行为 |
| `rdb-save.tcl` | RDB 持久化 | SAVE 命令落盘与启动恢复 |
| `rust-functions.tcl` | Function | Rust 侧 function 验证 |

## 运行方式
在仓库根目录执行：
```bash
./runtest
```

## 约定
- 每个 `.tcl` 文件应可独立运行，不依赖外部状态。
- `runtest` 只提供无参数全量回归；定向排查时直接使用上游 Tcl harness。
- 若补齐了被跳过的能力，从根目录 `runtest` 中删除对应 `--skipunit` 参数。
- `runtest` 默认向 harness 传入 `--dump-logs --dont-clean`，失败日志会回显且 `thirdparty/valkey/tests/tmp/` 现场会保留。
- 新增 Tcl 测试文件后，需在根目录 `runtest` 中确认其能被正确发现。
