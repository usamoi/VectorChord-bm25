# 非确定性来源快速参考 (Quick Reference)

## 🎯 核心结论

**代码库是完全确定性的 - 没有发现真正的非确定性来源。**

---

## ✅ 确认为确定性的操作

### 1. 浮点运算 (IEEE 754 Standard)
- **位置：** `src/weight.rs`, `src/segment/posting/reader.rs`, `src/algorithm/block_wand.rs`, `src/segment/meta.rs`
- **状态：** ✅ 确定性
- **原因：** 遵守 IEEE 754/2008 标准，相同输入产生相同输出
- **使用场景：** BM25 评分计算

### 2. 不稳定排序 (Unstable Sort/Selection)
- **位置：** `src/utils/topk_computer.rs` (lines 57, 62-64)
- **状态：** ✅ 确定性
- **原因：** `sort_unstable_by` 和 `select_nth_unstable_by` 对相同输入产生相同输出
- **说明：** "unstable"指不保证相等元素的原始顺序，但不是非确定性

### 3. 指针比较 (Pointer Comparison)
- **位置：** `src/datatype/memory_bm25vector.rs` (toast检测)
- **状态：** ✅ 确定性
- **原因：** 检查指针恒等性（`p == q`），不依赖具体地址值
- **使用场景：** PostgreSQL toast 数据检测

### 4. 函数指针验证
- **位置：** `src/index/hook.rs` (lines 32-35)
- **状态：** ✅ 确定性
- **原因：** 单次运行中函数地址固定
- **使用场景：** 验证索引访问方法

---

## ✅ 确认不存在的非确定性来源

| 类别 | 状态 | 说明 |
|------|------|------|
| HashMap/HashSet 迭代 | ✅ 不存在 | 仅使用 BTreeMap（有序） |
| 随机数生成 | ✅ 仅测试 | 生产代码无随机数 |
| 时间依赖操作 | ✅ 不存在 | 无 SystemTime/Instant |
| 线程竞态条件 | ✅ 不存在 | 主要单线程代码 |
| 未初始化内存 | ✅ 不存在 | 安全的 unsafe 使用 |

---

## 📊 文件分类

### 生产代码文件（全部确定性）

**浮点运算：**
1. `src/weight.rs` - BM25 权重和评分
2. `src/segment/posting/reader.rs` - 块最大分数
3. `src/algorithm/block_wand.rs` - WAND 算法
4. `src/segment/meta.rs` - 平均文档长度

**不稳定排序：**
- `src/utils/topk_computer.rs` - Top-K 选择

**指针操作：**
- `src/datatype/memory_bm25vector.rs` - Toast 检测
- `src/index/hook.rs` - 函数指针验证

### 测试代码文件（包含随机数）
- `src/algorithm/block_encode/delta_bitpack.rs`
- `src/utils/vint.rs`
- `src/utils/loser_tree.rs`
- `src/utils/topk_computer.rs` (测试部分)

---

## 💡 关键技术点

### IEEE 754 确定性保证
- 相同的输入值
- 相同的运算顺序
- 相同的舍入模式
→ 产生完全相同的输出

### 不稳定算法的确定性
```rust
// "unstable" ≠ "non-deterministic"
sort_unstable_by(|a, b| a.total_cmp(&b))
// 对相同输入：结果相同 ✅
// 只是不保证：相等元素的原始顺序
```

### 指针比较的语义
```rust
if p != q {  // 检查：是否是不同的指针（恒等性）
    // 不是：比较地址值的大小
}
```

---

## 🎖️ 代码质量评估

| 方面 | 评级 | 说明 |
|------|------|------|
| 集合选择 | ⭐⭐⭐⭐⭐ | 避免 HashMap/HashSet |
| 算法选择 | ⭐⭐⭐⭐⭐ | 使用 total_cmp() |
| 内存安全 | ⭐⭐⭐⭐⭐ | 谨慎的 unsafe 使用 |
| 确定性 | ⭐⭐⭐⭐⭐ | 完全确定性 |

---

## 📚 完整文档

- **中文详细报告：** `NON_DETERMINISM_ANALYSIS.md`
- **English Report:** `NON_DETERMINISM_ANALYSIS_EN.md`

---

**分析版本：** v2.0  
**更新日期：** 2026-02-12  
**核心结论：** 代码库完全确定性，无需改进 ✅
