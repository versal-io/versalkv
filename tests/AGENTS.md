# AGENTS.md — tests/

## 适用范围
- 本文件规范 `tests/` 目录的测试约定。
- 覆盖 cargo 集成测试、属性测试、Tcl 单元测试，以及库 API 测试和 Miri 确定性测试。

## 目录结构
```
tests/
├── AGENTS.md             本文件
├── README.md             测试布局说明
├── integration.rs        集成测试聚合入口（挂载 integration/ 下各子模块）
├── integration/          按域拆分的黑盒集成测试
│   ├── common/mod.rs     共享 TestServer + 极简 RESP 客户端
│   ├── admin.rs          管理/自省
│   ├── connection.rs     连接/鉴权
│   ├── datastructures.rs 容器类型
│   ├── dump.rs           DUMP/RESTORE 往返
│   ├── expire.rs         TTL/过期
│   ├── harness.rs        并行进程隔离
│   ├── keyspace.rs       数据层
│   ├── migrate.rs        迁移
│   ├── persistence.rs    RDB 持久化
│   ├── startup_loading.rs 启动 RDB/AOF 加载期间的信号与退出语义
│   ├── protocol.rs       协议层
│   ├── pubsub.rs         发布订阅
│   ├── replication.rs    复制
│   └── scripting.rs      脚本
├── property.rs           属性测试入口
├── property/             proptest 驱动的随机测试
│   ├── dict.rs           字典属性
│   └── protocol.rs       RESP 协议属性
├── unit/                 本地 Tcl 用例(runtest 暂存后运行)
│   ├── aof-basic.tcl
│   ├── dump-string.tcl
│   ├── pubsub-basic.tcl
│   ├── rdb-save.tcl
│   └── rust-functions.tcl
├── library_api.rs        库导出接口验证
└── miri_dict.rs          字典 Miri 确定性测试
```

## 测试分类与运行

| 类别 | 运行命令 | 说明 |
|------|---------|------|
| 集成测试 | `make integration` | 黑盒，拉起二进制做 RESP 断言 |
| 属性测试 | `make property` | proptest 随机测试 |
| Tcl 主线回归 | `./runtest` | 默认 debug，运行库单测及普通 Tcl 全量回归 |
| 库 API 测试 | `cargo test --test library_api` | 验证 lib.rs 导出可用 |
| Miri 测试 | `make miri` | 字典生命周期确定性验证 |

## 通用约定
- **隔离性**：各测试独占端口、进程和临时工作目录，RDB/AOF 文件互不污染。
- **回收**：`TestServer` 在 Drop 时优先发送 `SHUTDOWN NOSAVE`，超时后强杀。
- **并行**：cargo 测试默认并行执行，需保证测试间无共享状态。
- **新增测试**：在对应子目录建文件，更新入口文件（`integration.rs` / `property.rs`）添加 `mod` 声明。
