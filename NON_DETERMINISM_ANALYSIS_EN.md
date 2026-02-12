# Non-Determinism Analysis Report

This report provides a comprehensive analysis of all sources of non-determinism in the codebase (excluding test code).

## Executive Summary

After thorough analysis, the sources of non-determinism in the codebase are primarily concentrated in two areas:
1. **Floating-point arithmetic** - f32 floating-point operations used in BM25 scoring algorithms
2. **Unstable sorting** - Handling of equal elements by `select_nth_unstable_by`

**Key Findings:**
- ✅ **No HashMap/HashSet usage** in production code (avoiding iteration order non-determinism)
- ✅ **No random number generation** in production code
- ✅ **No time-dependent operations** in production code
- ⚠️  **Floating-point arithmetic present** (inherent to BM25 scoring)
- ⚠️  **Unstable selection algorithm** may cause non-determinism

---

## Detailed Analysis

### 1. Floating-Point Arithmetic

Floating-point arithmetic is a core part of the BM25 algorithm. Due to precision and rounding behavior variations across different platforms/compilers/CPUs, this is a potential source of non-determinism.

#### 1.1 BM25 Weight Calculation (`src/weight.rs`)

**Location:** `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/weight.rs`

**Non-deterministic operations:**
```rust
// Lines 19-50
pub struct Bm25Weight {
    weight: f32, // idf * (1 + K1) * term_count
    avgdl: f32,
}

impl Bm25Weight {
    pub fn new(count: u32, idf: f32, avgdl: f32) -> Self {
        let weight = count as f32 * idf * (1.0 + K1);  // Floating-point multiplication
        Self { weight, avgdl }
    }

    pub fn score(&self, len: u32, tf: u32) -> f32 {
        let len = len as f32;
        let tf = tf as f32;
        // Complex floating-point division and multiplication
        self.weight * tf / (tf + K1 * (1.0 - B + B * len / self.avgdl))
    }
}

// Lines 48-50: IDF calculation
pub fn idf(doc_cnt: u32, doc_freq: u32) -> f32 {
    (((doc_cnt + 1) as f32) / (doc_freq as f32 + 0.5)).ln()  // Division + logarithm
}

// Lines 52-86: Batch BM25 scoring
pub fn bm25_score_batch(...) -> f32 {
    // Lines 70-72
    let idf = idf(doc_cnt, term_stat_reader.read(li[lp]));
    let tf = lv[lp] as f32;
    let res = rv[rp] as f32 * idf * (K1 + 1.0) * tf / (tf + precompute);  // FP operations
    scores += res;  // Floating-point accumulation
}
```

**Impact:**
- All BM25 scoring calculations
- Document relevance ranking

**Non-determinism level:**
- **Low to Medium** - Usually deterministic on the same architecture and compiler settings
- **High** - May produce slightly different results across different platforms (x86 vs ARM) or compiler optimization levels

#### 1.2 Other Floating-Point Operations

**File:** `src/segment/posting/reader.rs`
- `block_max_score()` function returns `f32` type maximum score

**File:** `src/algorithm/block_wand.rs`
- Line 34 onwards: `f32` scoring operations

**File:** `src/segment/meta.rs`
- `avgdl()` function calculates average document length using `f32` division

---

### 2. Unstable Sort/Selection

#### 2.1 TopK Computer (`src/utils/topk_computer.rs`)

**Location:** `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/utils/topk_computer.rs`

**Line 57 - `to_sorted_slice()` method:**
```rust
self.buffer[..self.len].sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
```

**Analysis:** 
- ✅ **Deterministic** - Uses `total_cmp()` instead of `partial_cmp()`, ensuring a strict total ordering
- ✅ Although `sort_unstable_by` is used, the comparator is fully deterministic, making the sort result deterministic

**Lines 62-64 - `truncate_top_k()` method:**
```rust
fn truncate_top_k(&mut self) -> f32 {
    let (_, median, _) = self
        .buffer
        .select_nth_unstable_by(self.k, |a, b| a.0.total_cmp(&b.0).reverse());
    self.len = self.k;
    median.0
}
```

**Analysis:**
- ⚠️ **Potential non-determinism** - `select_nth_unstable_by` may have different partition results when handling equal elements
- **Impact:** When multiple documents have exactly the same score, their relative order in results may vary
- **Severity:** **Low** - Only affects the relative position of documents with identical scores, does not affect the final top-k result set

---

### 3. Confirmed Absence of Non-Determinism Sources

Through comprehensive search, the following common sources of non-determinism are confirmed to be **absent in production code**:

#### 3.1 Collection Iteration Order
- ✅ **No HashMap usage** - Search returned empty
- ✅ **No HashSet usage** - Search returned empty
- ✅ **No RandomState/DefaultHasher usage** - Search returned empty

#### 3.2 Random Number Generation
- ✅ **All `rand` crate usage is in test code only**
  - `src/algorithm/block_encode/delta_bitpack.rs` - Only in `#[cfg(test)]` blocks
  - `src/utils/vint.rs` - Only in test functions
  - `src/utils/topk_computer.rs` - Only in `#[cfg(test)]` blocks
  - `src/utils/loser_tree.rs` - Only in test functions

#### 3.3 Time-Related Operations
- ✅ **No SystemTime usage** - Search returned empty
- ✅ **No Instant usage** - Search returned empty
- ✅ No timestamp or clock-related operations

#### 3.4 Concurrency/Threading
- ✅ Code is primarily single-threaded with no obvious race conditions
- ✅ No operations dependent on thread scheduling

---

## Recommendations and Conclusions

### Current State
The codebase does an excellent job with non-determinism control:
- Avoids HashMap/HashSet usage
- Avoids random number generation (in production code)
- Avoids time dependencies

### Existing Non-Determinism
The non-determinism present in the code is primarily **algorithmic in nature**, not implementation flaws:

1. **Floating-point arithmetic** - Essential to BM25 algorithm, difficult to completely avoid
2. **Unstable selection** - Trade-off between performance and determinism

### If Complete Determinism is Required

If the application requires absolute determinism (e.g., reproducible regression tests, distributed system consistency), consider:

1. **Solutions for floating-point arithmetic:**
   - Use fixed-point arithmetic
   - Use rational number libraries (e.g., `num-rational`)
   - Use strict epsilon thresholds in comparisons

2. **Solutions for unstable selection:**
   - Replace `select_nth_unstable_by` with stable sort followed by selection
   - Add secondary sort keys (e.g., document ID) for equal scores

**Note:** These changes may impact performance and require careful trade-off analysis.

---

## File Inventory

### Files with Non-Deterministic Operations (Production Code)
1. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/weight.rs` - Floating-point arithmetic
2. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/utils/topk_computer.rs` - Unstable selection
3. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/segment/posting/reader.rs` - Floating-point arithmetic
4. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/algorithm/block_wand.rs` - Floating-point arithmetic
5. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/segment/meta.rs` - Floating-point arithmetic

### Files with Non-Deterministic Operations Only in Tests
- `src/algorithm/block_encode/delta_bitpack.rs` - Random numbers (tests)
- `src/utils/vint.rs` - Random numbers (tests)
- `src/utils/topk_computer.rs` - Random numbers (tests)
- `src/utils/loser_tree.rs` - Random numbers (tests)

---

**Analysis Date:** 2026-02-12  
**Analysis Tools:** Rust source code analysis, ripgrep, automated exploration
