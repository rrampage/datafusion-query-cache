# Parameterized Interval Caching

## Overview

Parameterized interval caching extends the interval-based caching system to enable **cache reuse across queries with different time bounds but identical query structures**. This is achieved by normalizing temporal literal values in query fingerprints, allowing queries like `(t >= 3 AND t < 10)` to reuse cached results from `(t >= 4 AND t < 6)` and `(t >= 7 AND t < 9)`.

## Motivation

Traditional query caching stores results keyed by exact query fingerprints. For time-series queries, this means:
- `SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T04:00:00Z' AND timestamp < '2024-01-01T06:00:00Z'` 
- `SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T07:00:00Z' AND timestamp < '2024-01-01T09:00:00Z'`

These queries have different fingerprints and cannot share cached results, even though they have identical structure and could combine cached data.

## Solution: Parameterized Fingerprints

Parameterized interval caching normalizes temporal literals to placeholders (`?`) in query fingerprints:

1. **Query Structure Normalization**: Replace temporal literal values with `?` in logical plan expressions
2. **Shared Cache Keys**: Queries with identical structure now share the same cache key
3. **Interval-Level Storage**: Store results under parameterized keys, indexed by specific time intervals
4. **Intelligent Reuse**: Combine cached intervals from different queries to satisfy new query ranges

## Architecture

### Cache Structure

```rust
// Single-level map keyed by parameterized fingerprint
HashMap<String, Vec<(TimeInterval, RecordBatch)>>

// Example:
"SELECT count(*) FROM events WHERE timestamp >= ? AND timestamp < ?" => [
    ([4, 6), cached_results_4_6),
    ([7, 9), cached_results_7_9),
    ([10, 12), cached_results_10_12),
]
```

### Query Processing Flow

1. **Parse Query**: Extract logical plan with temporal bounds
2. **Normalize Fingerprint**: Replace literals with `?` to create parameterized key
3. **Extract Interval**: Parse specific time bounds from query
4. **Lookup Cache**: Find all cached intervals under parameterized key that intersect with query interval
5. **Compute Gaps**: Identify uncovered portions of query interval
6. **Execute Plan**: Union cached results + computed gaps

## Supported Query Patterns

### Temporal Operators
- `>=`, `>`, `<=`, `<` with temporal columns
- `BETWEEN` clauses
- Combined bounds with `AND`

### Examples
```sql
-- All these queries share the same parameterized fingerprint:
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T04:00:00Z' AND timestamp < '2024-01-01T06:00:00Z'
SELECT count(*) FROM events WHERE timestamp >= '2024-01-01T07:00:00Z' AND timestamp < '2024-01-01T09:00:00Z'
SELECT count(*) FROM events WHERE timestamp BETWEEN '2024-01-01T10:00:00Z' AND '2024-01-01T12:00:00Z'

-- Parameterized fingerprint: SELECT count(*) FROM events WHERE timestamp >= ? AND timestamp < ?
```

## Normalization Rules

### Expression Normalization
- `timestamp >= '2024-01-01T04:00:00Z'` → `timestamp >= ?`
- `timestamp BETWEEN '2024-01-01T04:00:00Z' AND '2024-01-01T06:00:00Z'` → `timestamp BETWEEN ? AND ?`
- Non-temporal expressions remain unchanged

### Interval Semantics
- `>=` and `<` create half-open intervals `[start, end)`
- `BETWEEN` converts to half-open equivalent
- Combined bounds merge using interval intersection

## Performance Benefits

### Cache Hit Rate Improvement
For a query requesting `[3, 10)` with cached intervals `[4, 6)` and `[7, 9)`:
- **Traditional caching**: 0% hit rate (different query fingerprints)
- **Parameterized caching**: ~60% hit rate (combines cached data, computes only `[3,4)` and `[9,10)`)

### Computational Savings
- Avoid re-computing overlapping intervals
- Reduce I/O for cached portions
- Maintain result accuracy through gap computation

## Implementation Details

### Normalization Algorithm
```rust
fn normalize_temporal_bounds_in_expr(expr: &Expr, temporal_columns: &HashSet<Column>) -> Expr {
    match expr {
        BinaryExpr { left, op, right } if is_temporal_comparison(left, op, temporal_columns) => {
            // Replace literal in right with ?
            BinaryExpr { left: left.clone(), op: *op, right: Box::new(Expr::Literal("?")) }
        }
        Between { expr, low, high } if is_temporal_column(expr, temporal_columns) => {
            // Replace literals with ?
            Between { expr: expr.clone(), low: Box::new(Expr::Literal("?")), high: Box::new(Expr::Literal("?")) }
        }
        // Recursively normalize other expressions
        _ => expr.clone() // with recursive normalization
    }
}
```

### Interval Selection
```rust
fn select_maximal_cached_intervals(cached: &[TimeInterval], requested: &TimeInterval) -> Vec<TimeInterval> {
    // Find all intervals contained within the requested range
    cached.iter()
        .filter(|interval| interval.is_contained_in(requested))
        .cloned()
        .collect()
}

fn compute_gaps(requested: &TimeInterval, selected: &[TimeInterval]) -> Vec<TimeInterval> {
    // Compute uncovered portions of requested interval
    // Implementation uses interval arithmetic to find gaps
}
```

## Testing Strategy

### Unit Tests
- **Fingerprint Normalization**: Verify identical parameterized fingerprints for different literal values
- **Expression Handling**: Test `>=`, `<=`, `>`, `<`, `BETWEEN` normalization
- **Complex Queries**: Ensure non-temporal expressions remain unchanged

### Integration Tests
- **Cache Reuse**: Verify queries with different bounds share cached results
- **Gap Computation**: Ensure correct identification of uncovered intervals
- **Result Accuracy**: Validate combined cached + computed results match full computation

### Example Test Case
```rust
// Cache [4,6) and [7,9)
execute_query("SELECT count(*) FROM t WHERE ts >= 4 AND ts < 6");
execute_query("SELECT count(*) FROM t WHERE ts >= 7 AND ts < 9");

// Query [3,10) should reuse both cached intervals
let plan = create_physical_plan("SELECT count(*) FROM t WHERE ts >= 3 AND ts < 10");
assert_eq!(cached_intervals(plan), [TimeInterval(4,6), TimeInterval(7,9)]);
assert_eq!(gap_intervals(plan), [TimeInterval(3,4), TimeInterval(9,10)]);
```

## Limitations

### Current Scope
- Single temporal column per query (default temporal column)
- Static temporal bounds (literals only, no dynamic expressions)
- Aggregate queries only

### Future Extensions
- Multiple temporal columns
- Dynamic temporal expressions
- Non-aggregate query caching

## Configuration

### QueryCacheConfig
```rust
let config = QueryCacheConfig::new(
    default_temporal_column: Column::new(Some("events"), "timestamp"),
    cache: Arc::new(MemoryQueryCache::default())
);
```

### Temporal Column Detection
- Configured `temporal_columns` set
- Default temporal column for simple cases
- Group-by column analysis for complex aggregates

## Monitoring and Debugging

### Cache Inspection
```rust
// View parameterized fingerprints and cached intervals
println!("{}", cache.display());
```

### Execution Plan Analysis
```rust
// Identify cached vs computed intervals in physical plan
let plan = ctx.create_physical_plan(logical_plan).await?;
let (cached, gaps) = collect_exec_intervals(&plan);
println!("Cache hits: {}, Gaps: {}", cached.len(), gaps.len());
```

## Migration and Compatibility

### Backward Compatibility
- Existing interval caching continues to work
- Parameterized caching is opt-in via configuration
- No breaking changes to cache API

### Performance Considerations
- Minimal overhead for fingerprint normalization
- Memory usage scales with cached intervals per parameterized key
- Consider interval tree optimization for high interval counts

## Conclusion

Parameterized interval caching transforms time-series query caching from exact-match to structure-aware reuse, providing significant performance improvements for analytical workloads with overlapping temporal queries. By normalizing temporal bounds in query fingerprints, the system enables intelligent combination of cached results across different but structurally similar queries.
