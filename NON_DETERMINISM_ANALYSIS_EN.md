# Non-Determinism Analysis Report

This report provides a comprehensive analysis of all sources of non-determinism in the codebase (excluding test code).

## Executive Summary

**Conclusion: After thorough analysis, no true sources of non-determinism were found in production code.**

### Important Clarifications

1. **Floating-Point Arithmetic (IEEE 754/2008)** - Is deterministic
   - BM25 scoring uses f32 operations that comply with IEEE 754 standard
   - For the same inputs, produces identical outputs

2. **Unstable Sort** - Is deterministic
   - `sort_unstable_by` and `select_nth_unstable_by` produce identical outputs for identical inputs
   - "unstable" means it doesn't preserve the original order of equal elements, but it's not non-deterministic

3. **Pointer Comparison** - Is deterministic in this codebase's context
   - `p != q` comparison checks pointer identity (toast detection)
   - Does not depend on specific address values, only checks pointer identity

**Key Findings:**
- ✅ **No HashMap/HashSet usage** in production code (avoiding iteration order non-determinism)
- ✅ **No random number generation** in production code
- ✅ **No time-dependent operations** in production code
- ✅ Floating-point arithmetic follows IEEE 754 standard (deterministic)
- ✅ Unstable sorting produces identical output for identical input (deterministic)
- ✅ Pointer comparisons check identity, not address values (deterministic)

---

## Detailed Analysis

### Confirmation: No True Non-Determinism Sources

After in-depth analysis, here is detailed verification of potential non-determinism sources:

### 1. Floating-Point Arithmetic - ✅ Deterministic

**Conclusion: IEEE 754 standard guarantees determinism in floating-point arithmetic.**

Floating-point arithmetic is core to the BM25 algorithm. While floating-point has limited precision, the IEEE 754/2008 standard specifies exact operational rules, including rounding modes and special value handling. For identical inputs, implementations adhering to the standard produce identical outputs.

#### 1.1 BM25 Weight Calculation (`src/weight.rs`)

**Location:** `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/weight.rs`

**Floating-point operation examples:**
```rust
// Lines 19-50
pub struct Bm25Weight {
    weight: f32, // idf * (1 + K1) * term_count
    avgdl: f32,
}

impl Bm25Weight {
    pub fn new(count: u32, idf: f32, avgdl: f32) -> Self {
        let weight = count as f32 * idf * (1.0 + K1);  // FP multiplication
        Self { weight, avgdl }
    }

    pub fn score(&self, len: u32, tf: u32) -> f32 {
        let len = len as f32;
        let tf = tf as f32;
        // Complex FP division and multiplication
        self.weight * tf / (tf + K1 * (1.0 - B + B * len / self.avgdl))
    }
}

// Lines 48-50: IDF calculation
pub fn idf(doc_cnt: u32, doc_freq: u32) -> f32 {
    (((doc_cnt + 1) as f32) / (doc_freq as f32 + 0.5)).ln()  // Division + logarithm
}
```

**Determinism analysis:**
- ✅ Uses `total_cmp()` for comparisons, correctly handling NaN and special values
- ✅ IEEE 754 guarantees identical outputs for identical inputs
- ✅ Rust's f32 operations follow IEEE 754 standard

**Important notes:**
While IEEE 754 is deterministic, the following situations might cause cross-platform differences (these are NOT issues in the current code):
- Aggressive compiler optimizations changing operation order (can be disabled with `-ffast-math`)
- Different rounding mode settings (default is "round to nearest even")
- Use of extended precision registers (x87 vs SSE)

In the current code, these are not problems because:
1. Uses standard Rust floating-point operations
2. Does not use `-ffast-math` or other non-standard optimizations
3. Modern architectures (x86-64) default to SSE, which follows IEEE 754

#### 1.2 Other Floating-Point Operation Locations

**File:** `src/segment/posting/reader.rs`
- `block_max_score()` function returns `f32` type maximum score

**File:** `src/algorithm/block_wand.rs`
- Line 34 onwards: `f32` scoring operations

**File:** `src/segment/meta.rs`
- `avgdl()` function calculates average document length using `f32` division

**All of these are deterministic**, following IEEE 754 standard.

---

### 2. Unstable Sort/Selection - ✅ Deterministic

**Conclusion: Although named "unstable", it produces identical output for identical input, thus is deterministic.**

#### 2.1 TopK Computer (`src/utils/topk_computer.rs`)

**Location:** `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/utils/topk_computer.rs`

**Line 57 - `to_sorted_slice()` method:**
```rust
self.buffer[..self.len].sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
```

**Analysis:** 
- ✅ **Fully deterministic** - Uses `total_cmp()` instead of `partial_cmp()`, ensuring strict total ordering
- ✅ For the same input array, `sort_unstable_by` always produces the same sorted result
- ✅ "unstable" only means it doesn't preserve the original relative order of equal elements, but the output is deterministic

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
- ✅ **Deterministic** - `select_nth_unstable_by` produces identical output for identical input
- ✅ While the partition algorithm may vary, the kth element and partition results are deterministic for identical input
- ✅ Uses `total_cmp()` to ensure fully ordered comparison

**Key point:** "unstable" refers to the algorithm not guaranteeing stability (original order of equal elements), but it is **not** non-deterministic. For the same input slice and comparison function, the output is always identical.

---

### 3. Pointer Comparison - ✅ Deterministic (in this context)

#### 3.1 Toast Detection (`src/datatype/memory_bm25vector.rs`)

**Location:** Multiple uses of `p != q` pointer comparison

**Code example (lines 77-86):**
```rust
unsafe fn new(p: NonNull<Bm25VectorHeader>) -> Self {
    let q = unsafe {
        NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
    };
    if p != q {  // Check pointer identity
        Bm25VectorInput::Owned(Bm25VectorOutput(q))
    } else {
        unsafe { Bm25VectorInput::Borrowed(p.as_ref()) }
    }
}
```

**Analysis:**
- ✅ **Deterministic** - This comparison checks pointer **identity** (identity), not address values
- ✅ `pg_detoast_datum()` is a PostgreSQL function:
  - If data is toasted (compressed/external storage), returns new decompressed pointer
  - If data is not toasted, returns original pointer
- ✅ For the same data state (toasted or not toasted), the result of `p == q` is deterministic
- ✅ This doesn't depend on random address values, but checks if PostgreSQL allocated new memory

**Similar usage:**
- Lines 188-198: Same toast detection pattern
- Lines 209-219: Same toast detection pattern

#### 3.2 Function Pointer Comparison (`src/index/hook.rs`)

**Location:** Lines 32-35

```rust
Some(core::ptr::fn_addr_eq::<FnPtr, FnPtr>(
    *ambeginscan,
    crate::index::scan::ambeginscan,
))
```

**Analysis:**
- ✅ **Deterministic** - Within a single program run, function addresses are fixed
- ✅ This checks if PostgreSQL's index access method is our implementation
- ✅ Function pointers are determined at compile and load time, invariant during runtime
- ✅ While addresses may differ across different program runs, they are deterministic within a single run, which is the expected behavior

---

### 4. Other Verified Deterministic Operations

#### 4.1 Alignment Calculation (`src/datatype/memory_bm25vector.rs`)

**Lines 54-57:**
```rust
let ptr = self.phantom.as_ptr().cast::<u32>().add(len);
let offset = ptr.align_offset(8);  // Alignment calculation
let ptr = ptr.add(offset);
```

**Analysis:**
- ✅ **Deterministic** - `align_offset()` calculates alignment offset for a given pointer value
- ✅ While the pointer address itself may differ across runs, for the same data layout, the offset calculation is deterministic
- ✅ This is memory layout calculation, doesn't affect semantic behavior

---

### 5. Confirmed Absence of Non-Determinism Sources

Through comprehensive search, the following common sources of non-determinism are confirmed to be **absent in production code**:

#### 5.1 Collection Iteration Order
- ✅ **No HashMap usage** - Search returned empty
- ✅ **No HashSet usage** - Search returned empty
- ✅ **No RandomState/DefaultHasher usage** - Search returned empty
- ✅ Only uses BTreeMap (ordered map)

#### 5.2 Random Number Generation
- ✅ **All `rand` crate usage is in test code only**
  - `src/algorithm/block_encode/delta_bitpack.rs` - Only in `#[cfg(test)]` blocks
  - `src/utils/vint.rs` - Only in test functions
  - `src/utils/topk_computer.rs` - Only in `#[cfg(test)]` blocks
  - `src/utils/loser_tree.rs` - Only in test functions
- ✅ No random number generation in production code

#### 5.3 Time-Related Operations
- ✅ **No SystemTime usage** - Search returned empty
- ✅ **No Instant usage** - Search returned empty
- ✅ No timestamp or clock-related operations

#### 5.4 Concurrency/Threading
- ✅ Code is primarily single-threaded with no obvious race conditions
- ✅ No operations dependent on thread scheduling
- ✅ Only `std::thread::panicking()` call is for error handling, doesn't affect logic

#### 5.5 Uninitialized Memory
- ✅ All unsafe code carefully reviewed
- ✅ Uses safe abstractions like `std::slice::from_raw_parts`
- ✅ No reading of uninitialized memory

---

## Conclusions and Recommendations

### Final Conclusion

**The codebase is fully deterministic.**

After in-depth analysis, the codebase excels in non-determinism control:
- ✅ Avoids all common sources of non-determinism
- ✅ "Potentially non-deterministic" operations used (floating-point arithmetic, unstable sort) are actually deterministic
- ✅ Pointer comparisons are used for semantic checks (toast detection, function identification), not dependent on random address values

### Code Quality Assessment

1. **Collection Choices** - Excellent
   - Avoids HashMap/HashSet
   - Uses BTreeMap when ordering is needed

2. **Algorithm Choices** - Excellent
   - Uses `total_cmp()` for floating-point comparisons (correctly handles NaN)
   - Uses efficient unstable sort (while maintaining determinism)

3. **Memory Safety** - Excellent
   - Careful use of unsafe
   - Correct pointer operations
   - No uninitialized memory reads

### No Improvements Needed

The codebase has already achieved full determinism, no improvements needed. All operations are deterministic and produce identical outputs for identical inputs.

### References

1. **IEEE 754-2008 Standard** - Floating-point arithmetic specification
2. **Rust `total_cmp()` Documentation** - Total ordering floating-point comparison
3. **PostgreSQL Toast Mechanism** - Understanding `pg_detoast_datum` behavior

---

## File Inventory

### Files with Floating-Point Arithmetic (Deterministic)
1. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/weight.rs` - BM25 weights and scoring
2. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/segment/posting/reader.rs` - Block maximum score
3. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/algorithm/block_wand.rs` - WAND algorithm scoring
4. `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/segment/meta.rs` - Average document length calculation

### Files with Unstable Sort (Deterministic)
- `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/utils/topk_computer.rs` - Top-K selection algorithm

### Files with Pointer Operations (Deterministic)
- `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/datatype/memory_bm25vector.rs` - Toast detection and memory management
- `/home/runner/work/VectorChord-bm25/VectorChord-bm25/src/index/hook.rs` - Function pointer verification

### Files with Random Number Generation in Tests Only
- `src/algorithm/block_encode/delta_bitpack.rs`
- `src/utils/vint.rs`
- `src/utils/loser_tree.rs`
- `src/utils/topk_computer.rs` (test portion)

---

**Analysis Date:** 2026-02-12  
**Analysis Tools:** Rust source code analysis, ripgrep, automated exploration  
**Analysis Version:** v2.0 (Updated based on IEEE 754 and algorithmic determinism)
