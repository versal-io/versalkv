# AGENTS.md — tests/property/

## 适用范围
- 本文件规范 `tests/property/` 目录的属性测试约定。
- 这些测试通过 `tests/property.rs` 入口挂载。

## 职责
- 使用 `proptest` 框架驱动的随机测试，对核心数据结构和协议进行属性验证。
- 覆盖边界条件、随机输入和不变式断言，补充常规单元测试无法覆盖的路径。

## 文件列表
| 文件 | 覆盖域 | 说明 |
|------|--------|------|
| `dict.rs` | 字典数据结构 | 随机插入/删除/查找/清空/drain 序列 + rehash 期间混合操作 + rehash 中 scan 覆盖 + 扩容收缩抖动 |
| `zset.rs` | ZSet 跳表有序集 | 乱序 insert/delete 后 rank/count/range/pop/remove_range 四项查询互一致，HashMap 对照 + clone 一致性 |
| `protocol.rs` | RESP 协议 | 随机生成 RESP 输入，验证解析器正确性、往返序列化一致性 |
| `intset.rs` | IntSet 整型紧凑集 | 升级、排序、去重、contains 与按位 get 匹配随机用例 |

## 运行方式
```bash
# 默认运行（1000 个用例）
make property

# 调整随机测试用例数
PROPTEST_CASES=1000 make property

# 按文件过滤
cargo test --test property dict::
```

## 约定
- 测试应保持确定性：给定种子应产生相同结果。
- 失败时 `proptest` 会输出最小化复现输入，用于构造回归用例。
- 新增属性测试文件：在 `property/` 下建文件，在 `property.rs` 加 `#[path = "..."] mod` 声明。
