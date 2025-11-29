# Interval-Based Query Caching

## Table of Contents

1. [Overview](#overview)
2. [Key Concepts](#key-concepts)
3. [How It Works](#how-it-works)
4. [Supported Query Patterns](#supported-query-patterns)
5. [Cache Management](#cache-management)
6. [Implementation Architecture](#implementation-architecture)
7. [Performance Benefits](#performance-benefits)
8. [Current Status](#current-status)
9. [Future Enhancements](#future-enhancements)

---

## Overview

Interval-based query caching extends the DataFusion Query Cache to support caching aggregates over **time intervals** rather than just single timestamps. This enables efficient reuse of cached results across **overlapping** time ranges through **partial interval reuse**, providing significant performance improvements for time-series analytical queries.

### The Core Problem Solved

Traditional result caching stores final query results, but for time-series data, queries often span different time ranges:

```sql
-- Query 1: Last hour
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T00:00:00Z'

-- Query 2: Last 2 hours
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T00:00:00Z'

-- Query 3: Different range
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T01:00:00Z'
```

With interval caching, Query 2 can reuse cached data from Query 1 and only compute the additional hour. Query 3 can reuse the overlapping portion from Query 1.

### Key Benefits

- **Automatic chunking**: Queries are automatically split into cached and uncached intervals
- **Optimal reuse**: Greedy algorithm selects largest non-overlapping cached intervals
- **Correctness**: Results identical to non-cached execution
- **Transparent**: Works with existing SQL queries without changes

---

## Key Concepts

### Time Intervals

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeInterval {
    pub start_ns: i64,  // nanoseconds since epoch
    pub end_ns: i64,    // nanoseconds since epoch
}
```

Intervals are **half-open**: `[start_ns, end_ns)` - containing start but excluding end.

### Interval Overlap

A cached interval can be reused if it **overlaps** with the requested interval:

```rust
impl TimeInterval {
    pub fn overlaps(&self, other: &TimeInterval) -> bool {
        self.start_ns < other.end_ns && other.start_ns < self.end_ns
    }
}
```

*Note: Earlier versions used `is_contained_in()` for complete containment. The current implementation uses `overlaps()` for partial interval reuse, allowing wider queries to reuse cached fragments from narrower queries.*

### Maximal Interval Selection

When multiple cached intervals overlap, a greedy algorithm selects the largest non-overlapping intervals:

1. Sort cached intervals by length (descending)
2. Greedily select intervals that don't overlap with already selected ones
3. Compute gaps between selected intervals and requested range

---

## How It Works

### 1. Query Analysis

The optimizer detects static time intervals in WHERE clauses:

```sql
-- Supported patterns:
WHERE timestamp >= '2024-01-01T00:00:00Z'
WHERE timestamp BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-01T23:59:59Z'
WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'
```

### 2. Cache Lookup

For a query requesting interval `[A, B)`, find all cached intervals that overlap with `[A, B)`:

```rust
let cached_intervals = cache.lookup(fingerprint, &TimeInterval::new(A, B)).await?;
```

### 3. Interval Selection

Select maximal non-overlapping cached intervals using greedy algorithm:

```rust
let selected_cached = select_maximal_cached_intervals(&cached_intervals, &requested_interval);
```

### 4. Gap Computation

Compute uncovered gaps between requested interval and selected cached intervals:

```rust
let gaps = compute_gaps(&requested_interval, &selected_cached);
```

### 5. Query Execution

Execute three types of sub-queries and union results:

- **Cached reads**: `CachedAggregateExec` for each selected interval
- **Gap computations**: `CacheUpdateAggregateExec` for each gap (also caches results)
- **Final aggregation**: `AggregateExec` combines all partial results

---

## Supported Query Patterns

### Time Range Filters

```sql
-- Single bound (inclusive/exclusive)
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T00:00:00Z'
SELECT count(*) FROM events WHERE timestamp > '2024-01-01T00:00:00Z'

-- Double bounds (any combination of >, >=, <, <=)
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'
SELECT count(*) FROM events WHERE timestamp > '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'

-- BETWEEN (converted to half-open)
SELECT count(*) FROM events WHERE timestamp BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-01T23:59:59Z'
```

### Aggregation Functions

```sql
SELECT count(*), sum(value), avg(value), min(value), max(value)
FROM events
WHERE timestamp >= '2024-01-01T00:00:00Z'
```

### Complex Queries

```sql
SELECT
    date_trunc('hour', timestamp) as hour,
    service,
    count(*) as requests,
    avg(response_time) as avg_response_time
FROM events
WHERE timestamp >= '2024-01-01T00:00:00Z'
GROUP BY hour, service
ORDER BY hour DESC, service
```

### Unsupported Patterns

- Dynamic lower bounds: `WHERE timestamp >= now() - interval '1 hour'`
- Non-temporal filters mixed with temporal bounds
- Non-aggregate queries
- Subqueries with time filters

---

## Cache Management

### Cache API

```rust
#[async_trait]
pub trait QueryCache: Send + Sync + fmt::Debug {
    async fn lookup(&self, query_fingerprint: &str, req: &TimeInterval)
        -> DataFusionResult<Vec<Arc<dyn OccupiedIntervalCacheEntry>>>;

    async fn put(&self, query_fingerprint: &str, interval: TimeInterval, record_batch: &[RecordBatch])
        -> DataFusionResult<()>;
}
```

### Memory Implementation

```rust
pub struct MemoryQueryCache {
    cache: Arc<Mutex<HashMap<String, Vec<(TimeInterval, Arc<Vec<RecordBatch>>)>>>>,
}
```

### Cache Inspection

```rust
// Print all cached intervals for a query
pub async fn print_cache_state<C: QueryCache>(cache: &C, fingerprint: &str) -> DataFusionResult<()>>
```

---

## Implementation Architecture

### Logical Plan Transformation

1. **QCAggregateOptimizerRule**: Detects aggregates with temporal filters
2. **StaticInterval::find()**: Extracts time intervals from WHERE clauses
3. **QCAggregatePlanNode**: Logical plan node containing interval metadata

### Physical Plan Construction

1. **QCAggregateExecPlanner**: Converts logical nodes to physical plans
2. **select_maximal_cached_intervals()**: Greedy interval selection algorithm
3. **compute_gaps()**: Identifies uncached time ranges
4. **with_interval_bounds()**: Creates filtered aggregate plans for gaps

### Execution Components

- **CachedAggregateExec**: Reads pre-computed results from cache
- **CacheUpdateAggregateExec**: Computes new results and stores in cache
- **UnionExec + CoalescePartitionsExec**: Combines cached and new results
- **Final AggregateExec**: Produces final aggregated output

---

## Performance Benefits

### Example Scenario

Consider a dashboard with queries over different time ranges:

```
Time: 10:00 AM
Query A: [00:00, 10:00) → Cache miss, compute and store

Time: 10:05 AM
Query B: [00:00, 10:05) → Reuse [00:00, 10:00), compute only [10:00, 10:05)

Time: 10:10 AM
Query C: [09:00, 10:10) → Reuse [09:00, 10:00) from cache, compute only [10:00, 10:10)

Time: 10:15 AM
Query D: [08:00, 10:15) → Reuse [09:00, 10:00) from cache, compute [08:00, 09:00) and [10:00, 10:15)
```

### Benefits

- **Reduced computation**: Only process new data, not entire time range
- **Memory efficiency**: Reuse cached partial aggregations
- **Scalability**: Performance improves as cache hit rate increases
- **Predictability**: Query time proportional to size of new data

---

## Current Status

### ✅ **Implemented Features**

- [x] Interval-based cache storage and retrieval
- [x] **Partial interval reuse** - wider queries can reuse cached fragments from narrower queries
- [x] Static time interval detection from SQL WHERE clauses
- [x] Maximal non-overlapping interval selection algorithm
- [x] Automatic query chunking (cached + gap computation)
- [x] Memory-based cache implementation
- [x] Support for all standard aggregation functions
- [x] BETWEEN, >=, <=, and combined temporal filters
- [x] Canonical fingerprinting for consistent cache keys across equivalent queries
- [x] Comprehensive integration tests
- [x] Correctness verification (results match non-cached execution)

### ✅ **Test Coverage**

```bash
cargo test --test interval_cache
```

Tests verify:
- Full cache miss → hit scenarios
- Partial hits with gap computation
- Multiple overlapping cached intervals
- BETWEEN vs >=/<= equivalence
- Result correctness across all scenarios

### ⚠️ **Current Limitations**

- Only supports static time bounds (no dynamic expressions like `now()`)
- Requires temporal column in query projection
- Memory-only cache (no persistence)
- Single temporal column per query
- No support for complex temporal expressions

---

## Future Enhancements

### High Priority

1. **Dynamic Lower Bounds**
   - Support for queries like `WHERE timestamp >= now() - interval '1 hour'`
   - Runtime interval computation

2. **Persistent Cache**
   - Disk-based storage for cache persistence
   - Cache size management and eviction policies

3. **Advanced Interval Selection**
   - Weighted interval scheduling for optimal cache utilization
   - Cost-based interval selection considering I/O vs computation tradeoffs

### Medium Priority

4. **Multiple Temporal Columns**
   - Support for queries with multiple time dimensions
   - Composite interval keys

5. **Cache Warming**
   - Proactive caching of commonly queried intervals
   - Predictive caching based on query patterns

6. **Compression**
   - Result compression for memory efficiency
   - Dictionary encoding for categorical data

### Low Priority

7. **Distributed Cache**
   - Shared cache across multiple DataFusion instances
   - Cache consistency and coordination

8. **Query Optimization**
   - Cache-aware query planning
   - Automatic time range suggestions

---

## Usage Example

```rust
use datafusion_query_cache::{QueryCacheConfig, MemoryQueryCache, TimeInterval};

// Setup cache
let cache = Arc::new(MemoryQueryCache::default());
let config = QueryCacheConfig::new(
    Column::new(Some("events".to_string()), "timestamp".to_string()),
    cache
);

// Create session with caching enabled
let ctx = SessionContext::new_with_state(
    SessionStateBuilder::new()
        .with_default_features()
        .with_query_cache_log(config, LogStderrColors::default())
        .build()
);

// Register data
ctx.register_table("events", Arc::new(table))?;

// Queries automatically use interval caching
let result1 = ctx.sql("SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T00:00:00Z'").await?;
let result2 = ctx.sql("SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T00:00:00Z'").await?;
// result2 will reuse cached data from result1
```

---

## Debugging and Monitoring

### Cache Inspection

```rust
// Print cache contents for a specific query
print_cache_state(&cache, "SELECT count(*) FROM events WHERE timestamp >= ...").await?;
```

### Logging

The system provides detailed logging at multiple levels:
- Query fingerprint generation
- Interval detection and extraction
- Cache lookup results
- Interval selection decisions
- Gap computation
- Physical plan construction

Enable logging with:
```bash
RUST_LOG=datafusion_query_cache=debug cargo run
```

---

## Performance Characteristics

### Time Complexity

- **Cache lookup**: O(n) where n = cached intervals for query
- **Interval selection**: O(n log n) due to sorting
- **Gap computation**: O(n) for selected intervals
- **Query execution**: Proportional to size of gaps, not full time range

### Space Complexity

- **Cache storage**: O(sum of result sizes across all cached intervals)
- **Memory overhead**: Minimal - only stores aggregated results, not raw data

### Cache Hit Scenarios

| Scenario | Cache Hit | Computation Saved |
|----------|-----------|-------------------|
| Identical query | 100% | Complete |
| Overlapping cached intervals | Partial | Based on maximal selection |
| Superset with cached fragments | Partial | Proportional to cached coverage |
| No overlap | 0% | None |

---

## Troubleshooting

### Common Issues

1. **"input not a table scan" error**
   - Ensure temporal column is accessible in the query plan
   - Check that query includes supported aggregation patterns

2. **Cache misses on identical queries**
   - Verify query fingerprint consistency
   - Check for non-deterministic elements in query

3. **Incorrect results**
   - Ensure time intervals are properly detected
   - Verify BETWEEN vs >=/<= semantics match expectations

### Debugging Steps

1. Enable debug logging to see interval detection
2. Use `print_cache_state()` to inspect cache contents
3. Compare physical plans between cached and non-cached execution
4. Verify time interval boundaries in test data

---

## Changelog

### v0.1.0 - Partial Interval Reuse (November 2025)

**Major Enhancement**: Implemented partial interval reuse, allowing wider queries to reuse cached fragments from narrower queries.

#### Key Changes:
- **Overlap-based caching**: Changed from `is_contained_in()` to `overlaps()` for cache lookups
- **Canonical fingerprinting**: Added regex-based normalization to ensure consistent cache keys across queries with different literal values
- **Enhanced test coverage**: Added comprehensive tests for overlapping interval scenarios

#### Technical Details:
- Cache lookup now finds intervals that overlap with the requested range instead of requiring complete containment
- Fingerprint normalization replaces timestamp literals and schema information with canonical placeholders
- Maintains correctness while significantly improving cache hit rates for time-series workloads

#### Example:
```sql
-- Cache narrow interval
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T02:00:00Z' AND timestamp < '2024-01-01T03:00:00Z'

-- Reuse cached fragment in wider query
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T01:00:00Z' AND timestamp < '2024-01-01T04:00:00Z'
-- Only computes [01:00, 02:00) and [03:00, 04:00), reuses cached [02:00, 03:00)
```

### v0.0.1 - Initial Interval Caching (October 2025)

**Initial Release**: Basic interval-based caching for time-series aggregates.

#### Features:
- Interval-based cache storage and retrieval
- Static time interval detection from SQL WHERE clauses
- Maximal non-overlapping interval selection algorithm
- Automatic query chunking (cached + gap computation)
- Memory-based cache implementation
- Support for standard aggregation functions

---

*This documentation reflects the current implementation as of November 2025. Features and capabilities may evolve with future development.*
