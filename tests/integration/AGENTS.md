# AGENTS.md — tests/integration/

## 适用范围
- 本文件规范 `tests/integration/` 目录的 cargo 集成测试约定。
- 这些测试通过 `tests/integration.rs` 聚合入口挂载。

## 职责
- 黑盒集成测试：通过 `common/mod.rs` 提供的 `TestServer` 和 `Client` 辅助工具，拉起编译好的 `versal-server` 二进制，用极简 RESP 客户端做线格式断言。
- 覆盖 RESP 协议、命令行为、持久化、复制等端到端场景。

## 关键文件
- `common/mod.rs` — 共享 harness：
  - `TestServer`：启动被测二进制、分配空闲端口与独立工作目录，Drop 时自动回收。
  - `Client`：极简 RESP 客户端，发送请求并读取原始字节回复。
  - 断言基于 RESP 线格式，不依赖服务端实现细节。

## 文件列表
| 文件 | 覆盖域 | 对应源码 |
|------|--------|---------|
| `protocol.rs` | 协议层 | `foundation/io/protocol/`、`engine/` |
| `connection.rs` | 连接/鉴权 | `framework/commands/connection/` |
| `keyspace.rs` | 数据层 | `framework/commands/string/`、`engine/state/db/` |
| `expire.rs` | TTL/过期 | `framework/commands/generic/`、`string/set.rs` |
| `admin.rs` | 管理/自省 | `framework/commands/server/` |
| `datastructures.rs` | 容器类型 | Hash/Set/ZSet 命令 |
| `pubsub.rs` | 发布订阅 | 频道级 SUBSCRIBE/PUBLISH |
| `dump.rs` | 序列化往返 | DUMP/RESTORE |
| `migrate.rs` | 迁移 | MIGRATE 命令 |
| `persistence.rs` | RDB 持久化 | SAVE 落盘 → 重启 → 恢复 |
| `startup_loading.rs` | 启动加载生命周期 | RDB/AOF loading 期间的信号与干净退出 |
| `replication.rs` | 复制 | REPLICAOF 等 |
| `scripting.rs` | 脚本 | EVAL/EVALSHA |
| `harness.rs` | 并行进程隔离 | 测试框架自身验证 |

## 约定
- 各用例独占端口（`free_port()`）、独立进程、独立临时工作目录（`make_test_dir()`）。
- 回收时优先 `SHUTDOWN NOSAVE`，1s 超时后 `kill()`，确保 coverage profile 写入。
- 运行方式：`make integration`，按域过滤可用 `cargo test --test integration protocol::`。
- 新增域：在 `integration/` 下建文件，并在 `integration.rs` 加一行 `#[path = "..."] mod`。
