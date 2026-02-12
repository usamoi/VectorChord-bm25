# 非确定性来源快速参考 (Quick Reference)

## 关键发现总结

### ✅ 不存在的非确定性来源（生产代码）
- HashMap/HashSet 的迭代顺序问题
- 随机数生成
- 时间依赖操作 (SystemTime, Instant)
- 线程调度和竞态条件
- UUID 生成

### ⚠️ 存在的非确定性来源（生产代码）

#### 1. 浮点运算 (Floating-Point Arithmetic)
**影响：** 跨平台/编译器可能产生微小差异

| 文件 | 行号 | 描述 |
|------|------|------|
| `src/weight.rs` | 19-50 | BM25 权重计算，f32 乘法和除法 |
| `src/weight.rs` | 48-50 | IDF 计算，除法和对数运算 |
| `src/weight.rs` | 52-86 | 批量 BM25 评分，浮点累加 |
| `src/segment/posting/reader.rs` | - | block_max_score() 返回 f32 |
| `src/algorithm/block_wand.rs` | 34+ | f32 评分操作 |
| `src/segment/meta.rs` | - | avgdl() 使用 f32 除法 |

**严重程度：** 低到中等（同平台确定，跨平台可能有差异）

#### 2. 不稳定选择算法 (Unstable Selection)
**影响：** 相同分数的文档相对位置可能变化

| 文件 | 行号 | 描述 |
|------|------|------|
| `src/utils/topk_computer.rs` | 62-64 | select_nth_unstable_by() 可能导致相等元素顺序不同 |

**严重程度：** 低（不影响 top-k 集合本身，只影响相同分数文档的顺序）

---

## 代码位置索引

### 主要文件列表

**包含非确定性的生产代码：**
1. `src/weight.rs` - BM25 浮点运算
2. `src/utils/topk_computer.rs` - 不稳定选择
3. `src/segment/posting/reader.rs` - 浮点运算
4. `src/algorithm/block_wand.rs` - 浮点运算
5. `src/segment/meta.rs` - 浮点运算

**仅测试代码包含非确定性：**
- `src/algorithm/block_encode/delta_bitpack.rs`
- `src/utils/vint.rs`
- `src/utils/loser_tree.rs`
- `src/utils/topk_computer.rs` (测试部分)

---

## 如需完全确定性的建议

### 选项 1: 固定点运算
替换浮点运算为固定点算术（性能影响：中等）

### 选项 2: 稳定排序
替换 `select_nth_unstable_by` 为稳定排序（性能影响：较小）

### 选项 3: 次要排序键
为相同分数添加文档 ID 作为次要排序键（性能影响：最小）

---

**完整分析报告：**
- 中文版：`NON_DETERMINISM_ANALYSIS.md`
- English: `NON_DETERMINISM_ANALYSIS_EN.md`
