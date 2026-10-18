# 测试布局

本目录存放 crate `versal` 的黑盒测试与本地 Tcl 用例。cargo、普通 Tcl
与 Sentinel Tcl 三条测试线互不干扰、各司其职。

## 目录结构

```
tests/
├── README.md            本文件
├── checker_cli.rs       check-aof/check-rdb 真实进程、文件与退出状态
├── integration.rs       cargo 集成测试聚合入口(挂载 integration/ 下各子模块)
├── integration/         按域拆分的 cargo 集成测试(黑盒,拉起真实二进制)
│   ├── common/
│   │   └── mod.rs        共享 harness:TestServer + 极简 RESP 客户端
│   ├── protocol.rs       协议层:RESP 解析/序列化、半包重组
│   ├── connection.rs     连接层:HELLO/SELECT/RESET/QUIT、AUTH
│   ├── keyspace.rs       数据层:SET/GET/DEL/INCR、切库隔离
│   ├── expire.rs         TTL/过期
│   ├── admin.rs          管理/自省:INFO/COMMAND/CONFIG
│   ├── cluster.rs        集群
│   ├── startup_loading.rs 启动 RDB/AOF 加载期间的信号与退出语义
│   ├── replication/      复制
│   ├── scripting.rs      Lua/Function
│   └── ...               其余按域文件
├── property.rs          属性测试聚合入口
├── property/             proptest 驱动的随机测试
└── unit/                 本地 Tcl 用例(供 runtest 暂存后运行)
    ├── aof-basic.tcl
    ├── dump-string.tcl
    ├── monitor.tcl
    ├── pubsub-basic.tcl
    ├── rdb-save.tcl
    └── rust-functions.tcl
```

## 一、cargo 集成测试(`integration/`)

黑盒集成测试:经 `common/mod.rs` 拉起编译好的 `versal-server` 二进制,用极简 RESP
客户端做线格式断言。每个用例独占空闲端口、独立进程与临时工作目录,RDB/AOF
持久化文件不会互相污染,天然隔离、可并行。回收时 harness 优先发送
`SHUTDOWN NOSAVE`,超时后才强杀。

关于目录组织:cargo 只把 `tests/` 下的**顶层 `.rs`** 视为集成测试 target,子目录
中的文件不会被单独编译。因此这里用单一入口 `integration.rs` 通过 `#[path = "..."]`
挂载 `integration/` 下的各子模块;各子模块经 `crate::common` 引用共享 harness。

运行:

```bash
cargo test                       # 全部集成测试 + 内联单元测试
cargo test --test integration    # 仅这套集成测试
cargo test --test checker_cli    # 仅持久化 checker 二进制进程测试
```

按域过滤(利用模块名前缀):

```bash
cargo test --test integration protocol::     # 仅协议层
cargo test --test integration keyspace::     # 仅数据层
```

新增用例:在对应域的子模块文件里加 `#[test]` 函数即可;若需要新增一个域,
在 `integration/` 下建文件,并在 `integration.rs` 里加一行 `#[path = ...] mod`。

## 二、属性测试(`property/`)

`make property` 运行 proptest 驱动的属性测试,覆盖 dict、intset、protocol、
RDB 流与 zset;可用 `PROPTEST_CASES=<n>` 调整用例数量。

## 三、本地 Tcl 用例(`unit/`)

仓库根的 `runtest` 会把本目录的 `*.tcl` 暂存(复制)进
`thirdparty/valkey/tests/unit/`,再由 Valkey 的 Tcl harness 拉起 Rust 二进制运行,
同时把 `target/debug/versal-*` 以 `valkey-*` 兼容名复制到
`thirdparty/valkey/src/`。runner 不设置 `VALKEY_BIN_DIR`,也不恢复这些兼容副本或删除
复制后的本地 Tcl 文件;后续运行会直接覆盖它们。harness 默认回显失败服务端日志并
保留 `thirdparty/valkey/tests/tmp/` 现场，并为完整保留原生 slow cluster 工作量固定使用
3600 秒单元超时。这些本地用例用于 Rust 特有或先行验证场景。

运行（在仓库根目录，随普通 Tcl 全量回归执行）：

```bash
./runtest
```

## 四、Sentinel Tcl 回归

仓库根的 `runtest-sentinel` 使用 Rust `versal-server` 和 `versal-sentinel`，在真实
`thirdparty/valkey` 树中运行未经修改的全部原生 Sentinel 用例。runner 直接覆盖并保留
测试兼容二进制，不备份或修改上游 harness：

```bash
./runtest-sentinel
```

实例日志保留在 `thirdparty/valkey/tests/sentinel/tmp/`。不要在同一个本地工作区并发
运行 `runtest`、`runtest-sentinel` 或 `runtest-moduleapi`；它们会覆盖同一组
`thirdparty/valkey/src/valkey-*` 兼容入口，CI 中三者位于隔离 job。

## 三条线的分工

- cargo 线(集成测试 / 属性测试 / 内联单元测试):保证"自己写的逻辑对",秒级反馈。
- `runtest` + 普通 Tcl:保证常规命令和运行时兼容度只增不减。
- `runtest-sentinel` + Sentinel Tcl:保证 Sentinel 多进程语义和故障转移兼容度只增不减。
