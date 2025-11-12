# DataFusion Query Cache: Deep Dive

## Table of Contents

1. [Introduction](#introduction)
2. [Key Concepts](#key-concepts)
3. [Last Run Time and Incremental Caching](#last-run-time-and-incremental-caching)
4. [Query Execution Pipeline](#query-execution-pipeline)
5. [DataFusion Building Blocks Integration](#datafusion-building-blocks-integration)
6. [Detailed Stage Explanations](#detailed-stage-explanations)
7. [Query Plan Comparison](#query-plan-comparison)
8. [Implementation Details](#implementation-details)
9. [Code Reference Guide](#code-reference-guide)

---

## Introduction

The DataFusion Query Cache is a sophisticated caching layer that intercepts query execution to cache and reuse intermediate aggregation results for time-series data. Unlike traditional result caching (which stores final query results), this system caches **partial aggregation states** and intelligently combines them with new data, dramatically reducing computation for repeated queries over growing datasets.

### The Core Insight

When querying time-series data, most historical data remains unchanged between queries. For example:

```sql
SELECT avg(price) FROM stock_prices WHERE timestamp > '2000-01-01'
```

If you run this query at 10:00 AM and again at 10:10 AM, only 10 minutes of new data has been added. The cache:
1. Stores the partial aggregation result from 10:00 AM
2. Computes a new partial aggregation for data after 10:00 AM
3. Combines both partial results to produce the final answer

This approach works because DataFusion's aggregation system already supports combining partial aggregation states—the cache simply leverages this existing machinery.

---

## Key Concepts

### 1. Query Fingerprint

A unique identifier for each query, generated from the **logical plan's display representation**:

```rust
let fingerprint = plan.display_indent_schema().to_string();
```

The fingerprint captures:
- The query structure (SELECT, WHERE, GROUP BY, aggregations)
- Column references
- Filter predicates
- Aggregation functions

**Important**: The fingerprint is based on the logical plan, so semantically identical queries will produce the same fingerprint even if written differently.

### 2. Temporal Column

A timestamp column that orders data chronologically. The cache assumes:
- New data has timestamps close to `now()`
- Cached data has timestamps older than the cache timestamp
- Data is append-only or updates don't affect cached time ranges

Configuration:
```rust
let temporal_col = Column::new(Some("records".to_string()), "timestamp".to_string());
let config = QueryCacheConfig::new(temporal_col, cache);
```

### 3. Partial Aggregations

DataFusion splits aggregations into two modes:

- **Partial**: Computes partial aggregation states (e.g., sum and count for `avg()`)
- **Final**: Combines partial states into final results

Example for `avg(price)`:

**Partial output**:
| avg(price)[sum] | avg(price)[count] |
|-----------------|-------------------|
| 12340.5         | 100               |
| 15620.8         | 125               |

**Final output** (after combining):
| avg(price) |
|------------|
| 112.17     |

The cache stores **partial aggregation states**, enabling combination with new partial results.

### 4. Cache Entry Lifecycle

```
┌─────────────┐
│   Vacant    │ ← No cached data exists
└──────┬──────┘
       │ put(timestamp, batches)
       ▼
┌─────────────┐
│  Occupied   │ ← Cached data available
└──────┬──────┘
       │ get() / put() (update)
       ▼
   (reused or updated)
```

### Last Run Time and Incremental Caching

The cache uses a **last run time** mechanism to enable incremental caching across multiple query sessions. This is crucial for production deployments where you want to maintain cache state between application restarts or scheduled runs.

#### The `override_now` Configuration Parameter

The `QueryCacheConfig` includes an `override_now` field that allows you to control the timestamp used for cache operations:

```rust
pub struct QueryCacheConfig {
    // ... other fields ...
    override_now: Option<i64>,  // Timestamp in nanoseconds
    // ... other fields ...
}

impl QueryCacheConfig {
    // ... other methods ...

    pub fn with_override_now(mut self, timestamp: Option<i64>) -> Self {
        self.override_now = timestamp;
        self
    }
}
```

#### How `override_now` Works

During query execution, the cache uses this timestamp to determine:

1. **Cache Storage**: When storing results, uses `override_now` (or current time if `None`) as the cache timestamp
2. **Cache Lookup**: On cache hits, uses the stored cache timestamp to filter new data

```rust
let now = self.config.override_now.unwrap_or_else(|| {
    session_state
        .execution_props()
        .query_execution_start_time
        .timestamp_nanos_opt()
        .unwrap()
});
```

#### What Happens When `override_now` is `None` (Default Behavior)

When you initialize `QueryCacheConfig` without setting `override_now`, or explicitly set it to `None`:

```rust
// These are equivalent - both use current query execution time
let config = QueryCacheConfig::new(temporal_column, cache);
// OR
let config = QueryCacheConfig::new(temporal_column, cache)
    .with_override_now(None);
```

**The cache uses the current query execution start time** from DataFusion's session state:

- **Source**: `session_state.execution_props().query_execution_start_time.timestamp_nanos_opt()`
- **When captured**: At the moment query execution begins
- **Scope**: Per query execution (not persistent across queries)

**Key Characteristics:**

1. **No Explicit Tracking**: The timestamp is **not tracked by the context** between queries. Each query captures its own execution start time.

2. **Dynamic Timestamps**: Every new query gets a fresh timestamp representing when that specific query started executing.

3. **Cache Updates**: The cache **will save the new epoch value** with each query execution, updating the stored timestamp to the current query's start time.

**Example Behavior:**

```rust
// First query at T₁ (e.g., 2024-01-01 10:00:00)
let results1 = ctx.sql("SELECT avg(value) FROM metrics").await?.collect().await?;
// Cache stores: timestamp = T₁, data = partial_aggregation_up_to_T₁

// Second query at T₂ (e.g., 2024-01-01 10:05:00) - 5 minutes later
let results2 = ctx.sql("SELECT avg(value) FROM metrics").await?.collect().await?;
// Cache HIT: Uses cached data (up to T₁) + processes new data (T₁ to T₂)
// Cache updates: timestamp = T₂, data = combined_partial_aggregation_up_to_T₂

// Third query at T₃ (e.g., 2024-01-01 10:10:00) - another 5 minutes later
let results3 = ctx.sql("SELECT avg(value) FROM metrics").await?.collect().await?;
// Cache HIT: Uses cached data (up to T₂) + processes new data (T₂ to T₃)
// Cache updates: timestamp = T₃, data = combined_partial_aggregation_up_to_T₃
```

**Important Implications:**

- **✅ Incremental Processing**: Each query only processes data newer than the previous query's execution time
- **✅ Automatic Updates**: Cache timestamps stay current with each query execution
- **❌ No Persistence**: If you restart your application, you lose the "last run time" - cache starts fresh
- **❌ Unpredictable Gaps**: If queries run at irregular intervals, you might have gaps in cached data

**When to Use `override_now = None`:**

- **Ad-hoc queries** in interactive sessions
- **Short-lived applications** where cache persistence isn't needed
- **Testing and development** where you want fresh cache behavior
- **When you don't need** incremental caching across application restarts

**When to Use `override_now = Some(timestamp)`:**

- **Production deployments** requiring persistent cache state
- **Scheduled jobs** that need to maintain incremental state
- **Long-running applications** where you want to preserve cache across restarts
- **When you need** precise control over what constitutes "new" data

#### Incremental Caching Workflow

Here's how to implement incremental caching using last run time:

##### Step 1: Initial Run - Establish Cache Baseline

```rust
use datafusion_query_cache::{QueryCacheConfig, MemoryQueryCache, with_query_cache};
use datafusion::prelude::*;
use std::sync::Arc;

// Load last run time from persistent storage (or use None for first run)
let last_run_time = load_last_run_time_from_storage().await; // Option<i64>

// Create context with override_now set to last run time
let cache = Arc::new(MemoryQueryCache::default());
let config = QueryCacheConfig::new(temporal_column, cache)
    .with_override_now(last_run_time); // Use last run time

let state_builder = SessionStateBuilder::new()
    .with_config(SessionConfig::new())
    .with_runtime_env(Arc::new(RuntimeEnv::default()))
    .with_default_features();
let state_builder = with_query_cache(state_builder, config);
let ctx = SessionContext::new_with_state(state_builder.build());

// Register tables and run queries
// This will:
// - Treat data as if query is running at last_run_time
// - Cache results with last_run_time as the timestamp
// - Process ALL data (since no previous cache exists)
let results = ctx.sql("SELECT avg(value) FROM metrics").await?.collect().await?;
```

##### Step 2: Subsequent Runs - Incremental Processing

```rust
// Load the previous cache and last run time
let cache = load_cache_from_storage().await;
let last_run_time = load_last_run_time_from_storage().await;

// Create context WITHOUT override_now (use current time)
let config = QueryCacheConfig::new(temporal_column, cache)
    .with_override_now(None); // Use actual current time

let state_builder = SessionStateBuilder::new()
    .with_config(SessionConfig::new())
    .with_runtime_env(Arc::new(RuntimeEnv::default()))
    .with_default_features();
let state_builder = with_query_cache(state_builder, config);
let ctx = SessionContext::new_with_state(state_builder.build());

// Run the same queries
let results = ctx.sql("SELECT avg(value) FROM metrics").await?.collect().await?;

// The cache will:
// - Find existing cache entry with timestamp = last_run_time
// - Only process data with timestamp >= last_run_time
// - Combine cached results + new results
// - Store updated cache with current timestamp
```

##### Step 3: Persist Updated State

```rust
// After successful query execution, save the cache and new last run time
let current_time = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)?
    .as_nanos() as i64;

save_cache_to_storage(&cache).await;
save_last_run_time_to_storage(current_time).await;
```

#### Production Implementation Example

```rust
use datafusion_query_cache::{QueryCacheConfig, MemoryQueryCache, with_query_cache};
use datafusion::prelude::*;
use std::sync::Arc;

struct CachedQueryEngine {
    cache: Arc<MemoryQueryCache>,
    temporal_column: Column,
}

impl CachedQueryEngine {
    async fn new() -> Self {
        let cache = load_cache_from_storage().await
            .unwrap_or_else(|| Arc::new(MemoryQueryCache::default()));

        let temporal_column = Column::new(Some("metrics".to_string()), "timestamp".to_string());

        Self { cache, temporal_column }
    }

    async fn execute_incremental_query(&self, sql: &str) -> DataFusionResult<Vec<RecordBatch>> {
        // Load last run time
        let last_run_time = load_last_run_time_from_storage().await;

        // Configure cache with last run time for incremental processing
        let config = QueryCacheConfig::new(self.temporal_column.clone(), self.cache.clone())
            .with_group_by_function("date_trunc")
            .with_override_now(last_run_time); // Key: use last run time

        // Create session with caching enabled
        let state_builder = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_runtime_env(Arc::new(RuntimeEnv::default()))
            .with_default_features();
        let state_builder = with_query_cache(state_builder, config);
        let ctx = SessionContext::new_with_state(state_builder.build());

        // Register tables (in real scenario, this would be your data source)
        // ctx.register_table("metrics", your_table_provider).await?;

        // Execute query - cache will handle incremental processing
        let results = ctx.sql(sql).await?.collect().await?;

        // Update last run time after successful execution
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos() as i64;

        save_last_run_time_to_storage(now).await;

        Ok(results)
    }
}

// Usage:
let engine = CachedQueryEngine::new().await;
let results = engine.execute_incremental_query(
    "SELECT date_trunc('hour', timestamp), avg(value) FROM metrics GROUP BY 1"
).await?;
```

#### Key Benefits of Incremental Caching

1. **Reduced Computation**: Only process data newer than the last run time
2. **Persistent Cache State**: Maintain cache across application restarts
3. **Time-Based Partitioning**: Natural alignment with time-series data patterns
4. **Automatic Cache Updates**: Cache automatically stays current with each run

#### Cache Storage Format

The cache stores data as: `HashMap<String, (i64, Arc<Vec<RecordBatch>>)>`

- **Key**: Query fingerprint (deterministic string representation of logical plan)
- **Value**: `(timestamp_nanos, cached_record_batches)`

Where `timestamp_nanos` represents the "last run time" when the cache entry was created.

#### Cache Hit Logic with Last Run Time

```rust
// Cache lookup finds entry with timestamp T_last_run
if let Some((T_last_run, cached_batches)) = cache.get(&fingerprint) {
    // Filter new data: timestamp >= T_last_run
    let new_data = filter_data_where_timestamp_gte(T_last_run);

    // Combine: cached_partial_agg + new_partial_agg
    let combined = union(cached_batches, new_data);

    // Final aggregation produces up-to-date result
    let result = aggregate_final(combined);

    // Update cache with current timestamp T_now
    cache.put(fingerprint, (T_now, combined_batches));
}
```

This mechanism ensures that:
- **First run** (with `override_now = None`): Processes all data, caches with current time
- **Subsequent runs** (with `override_now = None`): Process only new data, combine with cache
- **Restarted runs** (with `override_now = last_run_time`): Continue incremental processing from where you left off

---

## Query Execution Pipeline

DataFusion processes queries through multiple stages. The cache intercepts at specific points:

```
SQL Query
   │
   ▼
┌─────────────────────────────┐
│ 1. SQL Parsing              │ ← DataFusion handles
│    (SQL → AST)              │
└──────────┬──────────────────┘
           ▼
┌─────────────────────────────┐
│ 2. Logical Plan Creation    │ ← DataFusion handles
│    (AST → LogicalPlan)      │
└──────────┬──────────────────┘
           ▼
┌─────────────────────────────┐
│ 3. Logical Optimization     │ ◄─ CACHE INTERVENES HERE
│    (OptimizerRule)          │    (QCAggregateOptimizerRule)
│                             │
│  - Identifies cacheable     │
│    aggregation queries      │
│  - Validates temporal cols  │
│  - Wraps with Extension     │
│    node (QCAggregatePlanNode)
└──────────┬──────────────────┘
           ▼
┌─────────────────────────────┐
│ 4. Physical Plan Creation   │ ◄─ CACHE INTERVENES HERE
│    (ExtensionPlanner)       │    (QCAggregateExecPlanner)
│                             │
│  - Looks up cache entry     │
│  - Rewrites plan structure  │
│  - Adds UnionExec for       │
│    cached + new data        │
└──────────┬──────────────────┘
           ▼
┌─────────────────────────────┐
│ 5. Physical Execution       │ ◄─ CACHE ACTIVE HERE
│    (ExecutionPlan::execute) │
│                             │
│  - CachedAggregateExec      │
│    retrieves cached data    │
│  - CacheUpdateAggregateExec │
│    stores new results       │
└──────────┬──────────────────┘
           ▼
      Final Results
```

### Stage 1: SQL Parsing

**Handler**: DataFusion's SQL parser

Converts SQL text into an Abstract Syntax Tree (AST). The cache doesn't intervene here.

```rust
// User calls:
let df = ctx.sql("SELECT avg(price) FROM stocks WHERE timestamp > '2000-01-01'").await?;
```

### Stage 2: Logical Plan Creation

**Handler**: DataFusion's logical planner

Converts the AST into a `LogicalPlan` tree structure:

```
Aggregate[avg(price)]
  └── Filter[timestamp > '2000-01-01']
       └── TableScan[stocks]
```

The cache doesn't intervene here either—it operates on the logical plan after creation.

### Stage 3: Logical Plan Optimization

**Cache Component**: `QCAggregateOptimizerRule`

**File**: [`src/aggregate.rs`](../src/aggregate.rs) (lines 40-237)

This is where the cache **first intervenes**. The optimizer rule:

1. **Examines each node** in the logical plan (bottom-up traversal)
2. **Identifies aggregation queries** (`LogicalPlan::Aggregate`)
3. **Validates cacheability**:
   - Checks for temporal columns in GROUP BY expressions
   - Analyzes filter predicates for temporal bounds
   - Ensures the sort column is projected
4. **Wraps cacheable plans** in a `QCAggregatePlanNode` (extension node)

**Key Code**:
```rust
impl OptimizerRule for QCAggregateOptimizerRule {
    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) 
        -> DataFusionResult<Transformed<LogicalPlan>> {
        
        // Generate fingerprint for this plan
        let fingerprint = plan.display_indent_schema().to_string();
        
        // Only process Aggregate nodes
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        
        // Validate temporal columns and cacheability
        // ... (validation logic)
        
        // Wrap in extension node
        let transformed_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(QCAggregatePlanNode::new(
                plan,
                temporal_column,
                dynamic_lower_bound,
                Some(fingerprint),
            )?),
        });
        
        Ok(Transformed::yes(transformed_plan))
    }
}
```

**Output**: A logical plan with a `QueryCacheAggregate` extension node wrapping the original aggregation.

### Stage 4: Physical Plan Creation

**Cache Component**: `QCAggregateExecPlanner` (implements `ExtensionPlanner`)

**File**: [`src/aggregate.rs`](../src/aggregate.rs) (lines 343-475)

This is where the cache performs its **most complex work**:

1. **Detects extension nodes**: Looks for `QCAggregatePlanNode` in the physical planning phase
2. **Performs cache lookup**: Uses the query fingerprint to check for cached data
3. **Rewrites the physical plan** based on cache hit/miss:

**On Cache MISS**:
```
AggregateExec[Final]
  └── CacheUpdateAggregateExec (stores result)
       └── CoalescePartitionsExec
            └── AggregateExec[Partial]
                 └── Filter[timestamp > '2000-01-01']
                      └── TableScan[stocks]
```

**On Cache HIT** (with cached timestamp = T₀):
```
AggregateExec[Final]
  └── CacheUpdateAggregateExec (updates cache with combined result)
       └── CoalescePartitionsExec
            └── UnionExec
                 ├── CachedAggregateExec (reads from cache)
                 └── AggregateExec[Partial]
                      └── Filter[timestamp > '2000-01-01' AND timestamp >= T₀]
                           └── TableScan[stocks]
```

**Key Code**:
```rust
impl ExtensionPlanner for QCAggregateExecPlanner {
    async fn plan_extension(&self, ...) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        // Get cache entry
        let cache_entry = self.config.cache().entry(&agg_node.fingerprint).await?;
        
        let input_exec = match &cache_entry {
            CacheEntry::Occupied(entry) => {
                // Cache HIT: Union of cached data + new data with filter
                let cached_exec = CachedAggregateExec::new_exec_plan(...);
                let new_exec = with_lower_bound(&partial_agg_exec, &temporal_column, entry.timestamp())?;
                Arc::new(CoalescePartitionsExec::new(
                    Arc::new(UnionExec::new(vec![cached_exec, new_exec]))
                ))
            }
            CacheEntry::Vacant(_) => {
                // Cache MISS: compute everything
                partial_agg_exec
            }
        };
        
        // Wrap in cache update executor
        let input_exec = CacheUpdateAggregateExec::new_exec_plan(cache_entry, input_exec, now);
        
        // Create final aggregation
        Arc::new(AggregateExec::try_new(AggregateMode::Final, ...))
    }
}
```

### Stage 5: Physical Execution

**Cache Components**: `CachedAggregateExec`, `CacheUpdateAggregateExec`

**File**: [`src/aggregate.rs`](../src/aggregate.rs) (lines 562-752)

During execution, two custom execution plans handle caching:

#### `CachedAggregateExec` (Cache Read)

Retrieves partial aggregation results from the cache:

```rust
async fn execute_get(cache_entry: Arc<dyn OccupiedCacheEntry>) -> DataFusionResult<Vec<RecordBatch>> {
    let batches = cache_entry.get().await?;
    Ok(batches.to_vec())
}
```

- Has **no children** (leaf node in execution tree)
- Reads from cache storage
- Returns partial aggregation batches

#### `CacheUpdateAggregateExec` (Cache Write)

Wraps the input execution plan and stores results:

```rust
async fn execute_store(
    input: Arc<dyn ExecutionPlan>,
    cache_entry: CacheEntry,
    now: i64,
    context: Arc<TaskContext>,
) -> DataFusionResult<Vec<RecordBatch>> {
    // Execute input plan
    let batches = collect(input, context).await?;
    
    // Store results in cache
    cache_entry.put(now, &batches).await?;
    
    // Pass through the batches
    Ok(batches)
}
```

- Executes its input plan
- Stores the output in the cache (with current timestamp)
- Passes data through to the final aggregation

**Critical**: This executor stores the **combined partial aggregation** result (union of cached + new), so the cache always contains the full historical aggregation up to the query time.

---

## DataFusion Building Blocks Integration

The cache integrates deeply with DataFusion's extension points. Here's how each building block is used:

### 1. `OptimizerRule` Trait

**Implementation**: `QCAggregateOptimizerRule`

**Purpose**: Intercept logical plan optimization to mark cacheable queries.

**Key Methods**:
- `name()`: Returns `"query-cache-agg-group-by"`
- `apply_order()`: Returns `BottomUp` (process leaves first, then parents)
- `rewrite()`: Examines nodes and wraps aggregations in extension nodes

**Integration Point**:
```rust
// In src/lib.rs
let state_builder = builder
    .with_optimizer_rule(Arc::new(QCAggregateOptimizerRule::new(log, config)));
```

DataFusion calls this rule during logical optimization, allowing us to transform the plan tree.

### 2. `ExtensionPlanner` Trait

**Implementation**: `QCAggregateExecPlanner`

**Purpose**: Convert custom logical nodes (our extension) into physical execution plans.

**Key Methods**:
- `plan_extension()`: Converts `QCAggregatePlanNode` → physical plan with caching

**Integration Point**:
```rust
// In src/lib.rs
impl QueryPlanner for QueryCacheQueryPlanner {
    async fn create_physical_plan(...) -> Arc<dyn ExecutionPlan> {
        let planners = vec![Arc::new(QCAggregateExecPlanner::new(...))];
        DefaultPhysicalPlanner::with_extension_planners(planners)
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
```

DataFusion's `DefaultPhysicalPlanner` delegates to our extension planner when it encounters our custom logical node.

### 3. `UserDefinedLogicalNodeCore` Trait

**Implementation**: `QCAggregatePlanNode`

**Purpose**: Represent a cacheable aggregation in the logical plan.

**Key Data**:
- `input`: The original `Aggregate` logical plan
- `fingerprint`: Query identifier for cache lookup
- `temporal_column`: Column used for time-based filtering
- `dynamic_lower_bound`: Optional time-based filter (not fully implemented)

**Key Methods**:
- `name()`: Returns `"QueryCacheAggregate"`
- `schema()`: Delegates to input plan's schema
- `with_exprs_and_inputs()`: Recreates node with new inputs (for plan transformations)

This node acts as a **marker** in the logical plan, signaling to the physical planner that caching should be applied.

### 4. `ExecutionPlan` Trait

**Implementations**: `CachedAggregateExec`, `CacheUpdateAggregateExec`

**Purpose**: Custom physical executors that interact with the cache during execution.

**Key Methods**:
- `execute()`: Returns a stream of record batches
- `children()`: Returns child execution plans
- `properties()`: Describes output partitioning and ordering

#### `CachedAggregateExec`

- **Children**: None (leaf executor)
- **Behavior**: Reads partial aggregation batches from cache
- **Output**: RecordBatch stream with cached partial results

#### `CacheUpdateAggregateExec`

- **Children**: One (the input execution plan to cache)
- **Behavior**: Executes input, stores result, passes through
- **Output**: Same batches as input (pass-through)

### 5. `AggregateExec` Modes

DataFusion's `AggregateExec` has two modes that enable caching:

#### `AggregateMode::Partial`

Computes partial aggregation states:
- `count(*)` → local count
- `sum(x)` → local sum
- `avg(x)` → (local sum, local count)
- `max(x)` → local maximum

These partial states can be **combined** later.

#### `AggregateMode::Final`

Combines partial states into final results:
- `count(*)` → sum of local counts
- `sum(x)` → sum of local sums
- `avg(x)` → sum(local sums) / sum(local counts)
- `max(x)` → maximum of local maxima

**How Cache Uses This**:

1. **Without cache**: DataFusion naturally creates Partial → Final structure for parallel execution
2. **With cache**: We hijack the Partial executor's output and store it
3. **On next run**: We union cached partial results with new partial results
4. **Final aggregation**: Same Final executor combines everything—it doesn't know or care that some data came from cache

This is the **genius** of the design: we leverage DataFusion's existing aggregation combining logic instead of implementing our own.

### 6. `UnionExec`

**Purpose**: Combines multiple input streams into one.

**Cache Usage**: Union cached partial results with new partial results:

```rust
let combined = UnionExec::new(vec![
    cached_exec,  // Cached partial aggregations
    new_exec,     // New partial aggregations
]);
```

The `UnionExec` simply concatenates streams—the Final aggregation downstream will combine them properly.

### 7. `CoalescePartitionsExec`

**Purpose**: Reduces multiple partitions to a single partition.

**Cache Usage**:
- Wraps `UnionExec` to ensure single partition output
- Cache stores single-partition results (simpler to manage)

```rust
Arc::new(CoalescePartitionsExec::new(combined_input))
```

---

## Detailed Stage Explanations

### Optimizer Rule Stage: Pattern Matching and Validation

**File**: [`src/aggregate.rs`](../src/aggregate.rs), `impl OptimizerRule`

The optimizer rule performs several validation steps before marking a query as cacheable:

#### Step 1: Identify Aggregation Queries

```rust
let LogicalPlan::Aggregate(agg) = &plan else {
    return Ok(Transformed::no(plan)); // Not an aggregation
};
```

Only `Aggregate` nodes are candidates. Filters, scans, and other operations pass through unchanged.

#### Step 2: Find Temporal GROUP BY Columns

If the query has `GROUP BY`, check if any expression uses a temporal column:

```rust
fn find_temporal_group_by(&self, expr: &Expr) -> Option<Column> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return None;
    };
    
    // Check if function is allowed (e.g., "date_trunc")
    if !self.config.allow_group_by_function(func.name()) {
        return None;
    }
    
    // Check if second argument is a temporal column
    let second_arg = args.get(1)?;
    if let Expr::Column(column) = second_arg {
        if self.config.allow_temporal_column(column) {
            return Some(column.clone());
        }
    }
    None
}
```

Example: `date_trunc('hour', timestamp)` would match if:
- `date_trunc` is in `group_by_functions` config
- `timestamp` is in `temporal_columns` config

#### Step 3: Analyze Filter Predicates for Dynamic Lower Bounds

```rust
let (dynamic_lower_bound, input) = if let LogicalPlan::Filter(filter) = &agg_input {
    let dlb = DynamicLowerBound::find(&filter.predicate, &needle_columns);
    match dlb {
        DynamicLowerBound::Found(bin_expr) => Some(bin_expr),
        DynamicLowerBound::Stable => None,
        _ => return Ok(Transformed::no(plan)), // Unstable, can't cache
    }
} else {
    (None, agg_input)
};
```

**Dynamic Lower Bound**: A filter like `timestamp > now() - interval '1 day'` that changes over time.

**Current Status**: Detection implemented but rewriting not fully supported (returns error at line 206).

**Stable Filters**: Static bounds like `timestamp > '2000-01-01'` are fine—they don't change.

#### Step 4: Ensure Temporal Column is Projected

For non-GROUP BY aggregations, the temporal column must be in the projection to enable filtering:

```rust
if temporal_group_by.is_none() {
    let LogicalPlan::TableScan(scan) = input else {
        return Ok(Transformed::no(plan)); // Not a table scan
    };
    
    let field_name = self.config.default_temporal_column().name;
    if !scan.projected_schema.fields().iter().any(|f| f.name() == &field_name) {
        // Add the temporal column to projection
        let mut new_projection = scan.projection.unwrap();
        new_projection.push(new_col_id);
        // ... rebuild plan with new projection
    }
}
```

This ensures we can filter on the temporal column even if it's not in the SELECT clause.

#### Step 5: Create Extension Node

```rust
let transformed_plan = LogicalPlan::Extension(Extension {
    node: Arc::new(QCAggregatePlanNode::new(
        plan.clone(),                // Original aggregate plan
        temporal_column,             // Column to use for time-based filtering
        dynamic_lower_bound,         // Optional dynamic filter
        Some(fingerprint.clone()),   // Cache key
    )?),
});
```

This wraps the original plan in our custom extension node, which will be recognized during physical planning.

### Physical Planning Stage: Cache Lookup and Plan Rewriting

**File**: [`src/aggregate.rs`](../src/aggregate.rs), `impl ExtensionPlanner`

#### Step 1: Detect Extension Nodes

```rust
async fn plan_extension(&self, node: &dyn UserDefinedLogicalNode, ...) 
    -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
    
    let Some(agg_node) = node.as_any().downcast_ref::<QCAggregatePlanNode>() else {
        return Ok(None); // Not our node, skip
    };
    
    // ... process our extension node
}
```

DataFusion calls `plan_extension()` for every extension node. We check if it's ours via downcast.

#### Step 2: Extract Physical Input

```rust
let exec = physical_inputs[0].clone(); // Already converted to physical plan

let Some(agg_exec): Option<&AggregateExec> = exec.as_any().downcast_ref() else {
    log_warn!("Expected AggregateExec, found {}", exec.name());
    return Ok(Some(exec));
};
```

By this point, DataFusion has already converted the inner `Aggregate` logical plan to a physical `AggregateExec`. We extract it.

**Expected Structure**:
```
AggregateExec[Final]
  └── AggregateExec[Partial]
       └── ... (filters, scans)
```

We need to work with the **Partial** aggregation (accessed via `agg_exec.input()`).

#### Step 3: Perform Cache Lookup

```rust
let cache_entry = self.config.cache().entry(&agg_node.fingerprint).await?;

log_info!(
    self.log,
    &agg_node.fingerprint,
    "Cache lookup result: {}",
    if cache_entry.occupied() { "HIT" } else { "MISS" }
);
```

Queries the cache implementation (e.g., `MemoryQueryCache`) using the fingerprint.

Returns:
- `CacheEntry::Occupied(entry)` if cached data exists
- `CacheEntry::Vacant(entry)` if no cached data

#### Step 4: Get Current Time

```rust
let now = self.config.override_now.unwrap_or_else(|| {
    session_state
        .execution_props()
        .query_execution_start_time
        .timestamp_nanos_opt()
        .unwrap()
});
```

Used as the timestamp when storing new cache entries. Can be overridden for testing.

#### Step 5: Rewrite Physical Plan (Cache Hit)

```rust
let input_exec = match &cache_entry {
    CacheEntry::Occupied(entry) => {
        let cached_exec = CachedAggregateExec::new_exec_plan(entry.clone(), ...);
        let new_exec = with_lower_bound(
            &partial_agg_exec, 
            &temporal_column, 
            entry.timestamp()  // Filter: timestamp >= cached_timestamp
        )?;
        
        // Union cached + new data
        let combined = Arc::new(UnionExec::new(vec![cached_exec, new_exec]));
        
        // Coalesce to single partition
        Arc::new(CoalescePartitionsExec::new(combined))
    }
    CacheEntry::Vacant(_) => {
        partial_agg_exec // No cache, compute everything
    }
};
```

**`with_lower_bound()` function**: Injects a filter into the physical plan:

```rust
fn with_lower_bound(
    partial_agg_exec: &Arc<dyn ExecutionPlan>,
    bound_column: &Column,
    lower_bound_ns: i64,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // Find the timestamp column and convert bound to its time unit
    let lower_bound_scalar = ScalarValue::TimestampNanosecond(Some(lower_bound_ns), None);
    
    // Create predicate: column >= lower_bound
    let predicate = Arc::new(PhysicalBinaryExpr::new(
        Arc::new(PhysicalColumn::new(&bound_column.name, column_id)),
        Operator::GtEq,
        Arc::new(PhysicalLiteral::new(lower_bound_scalar)),
    ));
    
    // Wrap existing filter or create new one
    let filter_exec = FilterExec::try_new(predicate, agg_exec.input().clone())?;
    
    // Rebuild AggregateExec with new filter
    Arc::new(AggregateExec::try_new(
        *agg_exec.mode(),
        agg_exec.group_expr().clone(),
        agg_exec.aggr_expr().to_vec(),
        Arc::new(filter_exec),
        ...
    )?)
}
```

This ensures new data computation only processes records after the cache timestamp.

#### Step 6: Wrap with Cache Update Executor

```rust
let input_exec = CacheUpdateAggregateExec::new_exec_plan(cache_entry, input_exec, now);
```

Wraps the (possibly unioned) partial aggregation with an executor that will store results.

#### Step 7: Create Final Aggregation

```rust
let final_plan = Arc::new(AggregateExec::try_new(
    AggregateMode::Final,
    agg_exec.group_expr().clone(),
    agg_exec.aggr_expr().to_vec(),
    agg_exec.filter_expr().to_vec(),
    input_exec,  // Our cache-aware input
    input_schema,
)?);
```

The top-level Final aggregation remains unchanged—it simply combines partial results from our cache-aware input.

### Execution Stage: Cache Operations

#### Cache Read: `CachedAggregateExec::execute()`

```rust
fn execute(&self, partition: usize, _context: Arc<TaskContext>) 
    -> DataFusionResult<SendableRecordBatchStream> {
    
    assert_eq!(partition, 0); // Single partition
    
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        self.schema.clone(),
        execute_get(self.cache_entry.clone(), metrics)
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten_stream(),
    )))
}

async fn execute_get(cache_entry: Arc<dyn OccupiedCacheEntry>) 
    -> DataFusionResult<Vec<RecordBatch>> {
    let batches = cache_entry.get().await?;
    Ok(batches.to_vec())
}
```

Simple read from cache, converted to a stream.

#### Cache Write: `CacheUpdateAggregateExec::execute()`

```rust
fn execute(&self, partition: usize, context: Arc<TaskContext>) 
    -> DataFusionResult<SendableRecordBatchStream> {
    
    Ok(Box::pin(RecordBatchStreamAdapter::new(
        self.input.schema(),
        execute_store(self.input.clone(), self.cache_entry.clone(), self.now, context, metrics)
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten_stream(),
    )))
}

async fn execute_store(
    input: Arc<dyn ExecutionPlan>,
    cache_entry: CacheEntry,
    now: i64,
    context: Arc<TaskContext>,
) -> DataFusionResult<Vec<RecordBatch>> {
    // Execute input plan (may include cached data + new data)
    let batches = collect(input, context).await?;
    
    // Store combined result
    cache_entry.put(now, &batches).await?;
    
    // Pass through to final aggregation
    Ok(batches)
}
```

**Key Insight**: The cached data is the **combined** partial aggregation (cached + new), not just the new data. This means:
- First run: Cache stores partial aggregation of all data
- Second run: Cache stores partial aggregation of all data (old cached + new)
- Third run: Cache stores partial aggregation of all data (previous combined + newest)

The cache always contains the full historical partial aggregation up to the query time.

---

## Query Plan Comparison

Let's examine the same query in three scenarios:

**Query**:
```sql
SELECT round(avg(value), 2), count(*) 
FROM records 
WHERE value > 1
```

### Scenario 1: Without Caching (Standard DataFusion)

**Logical Plan**:
```
Aggregate: groupBy=[[]], aggr=[[ROUND(AVG(records.value), Int64(2)), COUNT(*)]]
  Filter: records.value > Int64(1)
    TableScan: records
```

**Physical Plan**:
```
AggregateExec: mode=Final, gby=[], aggr=[ROUND(AVG(records.value),Int64(2)), COUNT(*)]
  CoalescePartitionsExec
    AggregateExec: mode=Partial, gby=[], aggr=[AVG(records.value), COUNT(*)]
      FilterExec: value@2 > 1
        MemoryExec: partitions=1, partition_sizes=[1]
```

**Execution Flow**:
1. `MemoryExec` reads all records from table
2. `FilterExec` filters `value > 1`
3. `AggregateExec[Partial]` computes partial aggregations (sum, count)
4. `CoalescePartitionsExec` combines partitions
5. `AggregateExec[Final]` produces final result
6. Post-processing applies `ROUND()`

**Cost**: Reads and processes entire table every time.

### Scenario 2: With Caching (First Run - Cache Miss)

**Logical Plan** (after optimization):
```
Extension: QueryCacheAggregate
  Aggregate: groupBy=[[]], aggr=[[ROUND(AVG(records.value), Int64(2)), COUNT(*)]]
    Filter: records.value > Int64(1)
      TableScan: records projection=[timestamp, service, value]
                                     ^^^^^^^^^ added by optimizer
```

**Physical Plan**:
```
AggregateExec: mode=Final, gby=[], aggr=[ROUND(AVG(records.value),Int64(2)), COUNT(*)]
  CacheUpdateAggregateExec(CoalescePartitionsExec)  ← Stores result in cache
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[AVG(records.value), COUNT(*)]
        FilterExec: value@2 > 1
          MemoryExec: partitions=1, partition_sizes=[1]
```

**Execution Flow**:
1. `MemoryExec` reads all records
2. `FilterExec` filters `value > 1`
3. `AggregateExec[Partial]` computes partial aggregations
4. `CoalescePartitionsExec` combines partitions
5. **`CacheUpdateAggregateExec` stores partial result (with timestamp T₁)**
6. `AggregateExec[Final]` produces final result

**Cache State After**:
```
Cache[fingerprint] = {
    timestamp: T₁
    batches: [
        RecordBatch {
            schema: [AVG(value)[sum], AVG(value)[count], COUNT(*)[count]],
            rows: [(150.5, 100, 100)]
        }
    ]
}
```

**Cost**: Same as without caching (first run always computes everything).

### Scenario 3: With Caching (Second Run - Cache Hit)

Assume:
- First query ran at timestamp T₁
- Second query runs at timestamp T₂ (10 minutes later)
- New data has been added with timestamps > T₁

**Logical Plan**: Same as first run (optimizer doesn't know about cache state)

**Physical Plan**:
```
AggregateExec: mode=Final, gby=[], aggr=[ROUND(AVG(records.value),Int64(2)), COUNT(*)]
  CacheUpdateAggregateExec(CoalescePartitionsExec)  ← Updates cache with combined result
    CoalescePartitionsExec
      UnionExec  ← Combines cached + new data
        ├── CachedAggregateExec  ← Reads from cache (data up to T₁)
        └── AggregateExec: mode=Partial, gby=[], aggr=[AVG(records.value), COUNT(*)]
              FilterExec: value@2 > 1 AND timestamp@0 >= T₁  ← Added filter
                MemoryExec: partitions=1, partition_sizes=[2]
```

**Execution Flow**:
1. **Left branch (cached)**:
   - `CachedAggregateExec` reads partial aggregations from cache (instant)
   
2. **Right branch (new data)**:
   - `MemoryExec` reads all records (including old ones)
   - `FilterExec` filters `value > 1 AND timestamp >= T₁` (only new data passes)
   - `AggregateExec[Partial]` computes partial aggregations for new data only
   
3. **Union**:
   - `UnionExec` concatenates cached partial batches + new partial batches
   - `CoalescePartitionsExec` ensures single partition
   
4. **Cache Update**:
   - `CacheUpdateAggregateExec` receives combined partial batches
   - Stores them in cache with timestamp T₂
   
5. **Final Aggregation**:
   - `AggregateExec[Final]` combines all partial results into final answer

**Cache State After**:
```
Cache[fingerprint] = {
    timestamp: T₂  ← Updated
    batches: [
        RecordBatch {
            schema: [AVG(value)[sum], AVG(value)[count], COUNT(*)[count]],
            rows: [(200.5, 125, 125)]  ← Combined result (old + new)
        }
    ]
}
```

**Cost**: Only processes new data (10 minutes worth) instead of entire table. If table has months of data, this is a massive savings.

### Visual Comparison

```
┌────────────────────────────────────────────────────────────────────────────┐
│                         EXECUTION PLAN COMPARISON                          │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  NO CACHING           CACHE MISS              CACHE HIT                   │
│  ═══════════          ═══════════             ═════════                   │
│                                                                            │
│  AggExec[Final]       AggExec[Final]          AggExec[Final]             │
│       │                    │                        │                     │
│       │                    │                        │                     │
│  CoalesceParts        CacheUpdate             CacheUpdate                │
│       │                    │                        │                     │
│       │                    │                        │                     │
│  AggExec[Part.]       CoalesceParts           CoalesceParts              │
│       │                    │                        │                     │
│       │                    │                        │                     │
│  FilterExec           AggExec[Part.]          UnionExec                  │
│       │                    │                    ╱       ╲                 │
│       │                    │                   ╱         ╲                │
│  MemoryExec           FilterExec         CachedAgg   AggExec[Part.]     │
│                            │                (fast!)        │              │
│                            │                               │              │
│                       MemoryExec                      FilterExec         │
│                                                       (timestamp >= T₁)   │
│                                                            │              │
│                                                       MemoryExec          │
│                                                                            │
│  Reads: ALL           Reads: ALL               Reads: NEW ONLY           │
│  Cache: N/A           Cache: STORE             Cache: READ + UPDATE      │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

### Actual Query Plan Examples

The following examples show real execution plans generated by the DataFusion Query Cache for different types of queries. These were captured from the `demo.rs` example program.

#### Example 1: Simple Aggregation (No GROUP BY)

**Query**: `SELECT round(avg(value), 2), count(*) FROM records WHERE value > 1`

**With Caching (Cache Miss)**:
```
ProjectionExec: expr=[round(avg(records.value)@0, 2), count(*)@1]
  AggregateExec: mode=Final, gby=[], aggr=[avg(records.value), count(*)]
    CacheUpdateAggregateExec  ← Caches result after computation
      CoalescePartitionsExec
        AggregateExec: mode=Partial, gby=[], aggr=[avg(records.value), count(*)]
          CoalesceBatchesExec
            FilterExec: value@1 > 1
              RepartitionExec: partitioning=RoundRobinBatch(10)
                MemoryExec: partitions=1
```

**Without Caching**:
```
ProjectionExec: expr=[round(avg(records.value)@0, 2), count(*)@1]
  AggregateExec: mode=Final, gby=[], aggr=[avg(records.value), count(*)]
    CoalescePartitionsExec
      AggregateExec: mode=Partial, gby=[], aggr=[avg(records.value), count(*)]
        CoalesceBatchesExec
          FilterExec: value@0 > 1
            RepartitionExec: partitioning=RoundRobinBatch(8)
              MemoryExec: partitions=1
```

**Key Differences**:
- **With Caching**: Includes `CacheUpdateAggregateExec` that stores partial aggregation results
- **Without Caching**: Standard DataFusion execution plan
- **Partitioning**: Cache-aware plan uses 10 partitions vs 8 for non-cache plan

#### Example 2: GROUP BY with date_trunc (Cache Hit)

**Query**: `SELECT date_trunc('hour', timestamp), round(avg(value), 2), count(*) FROM records WHERE value > 1 GROUP BY 1 ORDER BY 1 DESC`

**With Caching (Cache Hit)**:
```
ProjectionExec: expr=[date_trunc(Utf8("hour"),timestamp@0), round(avg(records.value)@1, 2), count(*)@2]
  SortPreservingMergeExec: [date_trunc(Utf8("hour"),timestamp@0) DESC]
    SortExec: expr=[date_trunc(Utf8("hour"),timestamp@0) DESC]
      AggregateExec: mode=Final, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), count(*)]
        CacheUpdateAggregateExec  ← Updates cache with combined result
          CoalescePartitionsExec
            UnionExec  ← Combines cached + new data
              CachedAggregateExec  ← Reads cached partial aggregations
              AggregateExec: mode=Partial, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), count(*)]
                CoalesceBatchesExec
                  FilterExec: value@2 > 1 AND timestamp@0 >= T₁  ← Filters for new data only
                    RepartitionExec: partitioning=RoundRobinBatch(10)
                      MemoryExec: partitions=1
```

**Without Caching**:
```
ProjectionExec: expr=[date_trunc(Utf8("hour"),timestamp@0), round(avg(records.value)@1, 2), count(*)@2]
  SortPreservingMergeExec: expr=[date_trunc(Utf8("hour"),timestamp@0) DESC]
    AggregateExec: mode=FinalPartitioned, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), count(*)]
      CoalesceBatchesExec
        RepartitionExec: partitioning=Hash([date_trunc('hour',timestamp@0)], 8)
          AggregateExec: mode=Partial, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), count(*)]
            CoalesceBatchesExec
              FilterExec: value@1 > 1
                RepartitionExec: partitioning=RoundRobinBatch(8)
                  MemoryExec: partitions=1
```

**Key Differences**:
- **With Caching**: Uses `UnionExec` to combine `CachedAggregateExec` (instant read) + filtered new data computation
- **With Caching**: `FilterExec` includes `timestamp >= T₁` to process only data newer than cache
- **Without Caching**: Uses hash partitioning for distributed aggregation
- **Performance**: Cache hit processes only new data instead of entire dataset

#### Example 3: GROUP BY on Categorical Column

**Query**: `SELECT service, round(avg(value), 2), count(*) FROM records WHERE value > 1 GROUP BY service ORDER BY service`

**With Caching (Cache Miss)**:
```
SortPreservingMergeExec: [service@0 ASC NULLS LAST]
  SortExec: expr=[service@0 ASC NULLS LAST]
    ProjectionExec: expr=[service@0, round(avg(records.value)@1, 2), count(*)@2]
      RepartitionExec: partitioning=RoundRobinBatch(10)
        AggregateExec: mode=Final, gby=[service@0], aggr=[avg(records.value), count(*)]
          CacheUpdateAggregateExec  ← Caches partial aggregations by service
            CoalescePartitionsExec
              AggregateExec: mode=Partial, gby=[service@0], aggr=[avg(records.value), count(*)]
                CoalesceBatchesExec
                  FilterExec: value@2 > 1
                    RepartitionExec: partitioning=RoundRobinBatch(10)
                      MemoryExec: partitions=1
```

**Without Caching**:
```
SortPreservingMergeExec: [service@0 ASC NULLS LAST]
  SortExec: expr=[service@0 ASC NULLS LAST]
    ProjectionExec: expr=[service@0, round(avg(records.value)@1, 2), count(*)@2]
      AggregateExec: mode=FinalPartitioned, gby=[service@0], aggr=[avg(records.value), count(*)]
        CoalesceBatchesExec
          RepartitionExec: partitioning=Hash([service@0], 8)
            AggregateExec: mode=Partial, gby=[service@0], aggr=[avg(records.value), count(*)]
              CoalesceBatchesExec
                FilterExec: value@1 > 1
                  RepartitionExec: partitioning=RoundRobinBatch(8)
                    MemoryExec: partitions=1
```

**Key Differences**:
- **With Caching**: Uses `CacheUpdateAggregateExec` to store partial aggregations grouped by service
- **With Caching**: Single final aggregation (no partitioning needed due to cache)
- **Without Caching**: Uses `FinalPartitioned` with hash partitioning for distributed processing
- **Partitioning**: Cache-aware plan uses 10 partitions vs 8 for non-cache plan

#### Example 4: Time Series Buckets (Hourly intervals)

**Query**: `SELECT date_trunc('hour', timestamp) as bucket, count(*) as requests_per_hour FROM records GROUP BY bucket ORDER BY bucket DESC`

**With Caching (Cache Miss)**:
```
SortPreservingMergeExec: [date_trunc(Utf8("hour"),timestamp@0) DESC]
  SortExec: expr=[date_trunc(Utf8("hour"),timestamp@0) DESC]
    AggregateExec: mode=Final, gby=[date_trunc('hour',timestamp@0)], aggr=[count(*)]
      CacheUpdateAggregateExec  ← Caches partial aggregations by time bucket
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[date_trunc('hour',timestamp@0)], aggr=[count(*)]
            RepartitionExec: partitioning=RoundRobinBatch(10)
              MemoryExec: partitions=1
```

**Without Caching**:
```
SortPreservingMergeExec: [date_trunc(Utf8("hour"),timestamp@0) DESC]
  SortExec: expr=[date_trunc(Utf8("hour"),timestamp@0) DESC]
    AggregateExec: mode=FinalPartitioned, gby=[date_trunc('hour',timestamp@0)], aggr=[count(*)]
      CoalesceBatchesExec
        RepartitionExec: partitioning=Hash([date_trunc('hour',timestamp@0)], 8)
          AggregateExec: mode=Partial, gby=[date_trunc('hour',timestamp@0)], aggr=[count(*)]
            RepartitionExec: partitioning=RoundRobinBatch(8)
              MemoryExec: partitions=1
```

**Key Differences**:
- **With Caching**: Uses `CacheUpdateAggregateExec` to store partial aggregations grouped by hourly time buckets
- **With Caching**: Single final aggregation (no hash partitioning needed)
- **Without Caching**: Uses `FinalPartitioned` with hash partitioning for distributed processing by time bucket
- **Performance**: Cache hit would process only new time buckets instead of entire dataset

#### Example 5: Multi-dimensional Time Series (Service + Time Buckets)

**Query**: `SELECT date_trunc('minute', timestamp) as bucket, service, count(*) as requests FROM records GROUP BY bucket, service ORDER BY bucket DESC, service`

**With Caching (Cache Miss)**:
```
SortPreservingMergeExec: [date_trunc(Utf8("minute"),timestamp@0) DESC, service@1 ASC NULLS LAST]
  SortExec: expr=[date_trunc(Utf8("minute"),timestamp@0) DESC, service@1 ASC NULLS LAST]
    AggregateExec: mode=Final, gby=[date_trunc('minute',timestamp@0), service@1], aggr=[count(*)]
      CacheUpdateAggregateExec  ← Caches partial aggregations by time bucket AND service
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[date_trunc('minute',timestamp@0), service@1], aggr=[count(*)]
            RepartitionExec: partitioning=RoundRobinBatch(10)
              MemoryExec: partitions=1
```

**Without Caching**:
```
SortPreservingMergeExec: [date_trunc(Utf8("minute"),timestamp@0) DESC, service@1 ASC NULLS LAST]
  SortExec: expr=[date_trunc(Utf8("minute"),timestamp@0) DESC, service@1 ASC NULLS LAST]
    AggregateExec: mode=FinalPartitioned, gby=[date_trunc('minute',timestamp@0), service@1], aggr=[count(*)]
      CoalesceBatchesExec
        RepartitionExec: partitioning=Hash([date_trunc('minute',timestamp@0), service@1], 8)
          AggregateExec: mode=Partial, gby=[date_trunc('minute',timestamp@0), service@1], aggr=[count(*)]
            RepartitionExec: partitioning=RoundRobinBatch(8)
              MemoryExec: partitions=1
```

**Key Differences**:
- **With Caching**: Stores partial aggregations for each combination of time bucket and service
- **With Caching**: Multi-dimensional grouping handled by single final aggregation
- **Without Caching**: Uses hash partitioning across both time and service dimensions
- **Cache Efficiency**: On cache hit, only computes new time buckets for each service

#### Example 6: Time Series with Multiple Aggregates and Filtering

**Query**: `SELECT date_trunc('hour', timestamp) as bucket, round(avg(value), 2) as avg_value, min(value), max(value) FROM records GROUP BY bucket ORDER BY bucket DESC`

**With Caching (Cache Hit)**:
```
SortPreservingMergeExec: [date_trunc(Utf8("hour"),timestamp@0) DESC]
  SortExec: expr=[date_trunc(Utf8("hour"),timestamp@0) DESC]
    ProjectionExec: expr=[date_trunc(Utf8("hour"),timestamp@0), round(avg(records.value)@1, 2), min(records.value)@2, max(records.value)@3]
      AggregateExec: mode=Final, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), min(records.value), max(records.value)]
        CacheUpdateAggregateExec  ← Updates cache with combined result
          CoalescePartitionsExec
            UnionExec  ← Combines cached + new data
              CachedAggregateExec  ← Reads cached partial aggregations
              AggregateExec: mode=Partial, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), min(records.value), max(records.value)]
                FilterExec: timestamp@0 >= T₁  ← Filters for new data only
                  RepartitionExec: partitioning=RoundRobinBatch(10)
                    MemoryExec: partitions=1
```

**Without Caching**:
```
SortPreservingMergeExec: [date_trunc(Utf8("hour"),timestamp@0) DESC]
  SortExec: expr=[date_trunc(Utf8("hour"),timestamp@0) DESC]
    ProjectionExec: expr=[date_trunc(Utf8("hour"),timestamp@0), round(avg(records.value)@1, 2), min(records.value)@2, max(records.value)@3]
      AggregateExec: mode=FinalPartitioned, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), min(records.value), max(records.value)]
        CoalesceBatchesExec
          RepartitionExec: partitioning=Hash([date_trunc('hour',timestamp@0)], 8)
            AggregateExec: mode=Partial, gby=[date_trunc('hour',timestamp@0)], aggr=[avg(records.value), min(records.value), max(records.value)]
              RepartitionExec: partitioning=RoundRobinBatch(8)
                MemoryExec: partitions=1
```

**Key Differences**:
- **With Caching**: `UnionExec` combines cached partial results with new partial results for only recent hours
- **With Caching**: `CachedAggregateExec` provides instant access to historical hourly aggregations
- **Without Caching**: Processes entire dataset for all hourly aggregations
- **Multiple Aggregates**: Cache handles avg, min, max simultaneously with single cache entry

### Aggregate Queries vs Time Series Bucket Queries

The cache works fundamentally the same for both aggregate queries and time series bucket queries, but there are important differences in behavior and optimization opportunities:

#### Aggregate Queries (No GROUP BY)
```sql
SELECT avg(value), count(*) FROM records WHERE value > 1
```

**Characteristics**:
- **Cache Structure**: Single partial aggregation result per query fingerprint
- **Temporal Filtering**: Applied to entire dataset (all records after cache timestamp)
- **Cache Reuse**: Complete replacement - cache contains all historical data up to cache time
- **Performance**: Simple 2x speedup (process all vs process new only)

#### Time Series Bucket Queries (GROUP BY on temporal buckets)
```sql
SELECT date_trunc('hour', timestamp), count(*) FROM records GROUP BY 1
```

**Characteristics**:
- **Cache Structure**: Partial aggregations grouped by time bucket
- **Temporal Filtering**: Applied per bucket (only buckets after cache timestamp)
- **Cache Reuse**: Incremental - new buckets added to existing bucket groups
- **Performance**: Variable speedup depending on bucket size and data distribution

#### Key Differences in Execution

| Aspect | Aggregate Queries | Time Series Bucket Queries |
|--------|-------------------|----------------------------|
| **Cache Granularity** | Single result | Per time bucket |
| **Incremental Updates** | Replace entire result | Add new buckets |
| **Filter Efficiency** | Simple timestamp filter | Bucket-aware filtering |
| **Memory Usage** | Fixed size | Grows with time buckets |
| **Query Flexibility** | Fixed aggregation | Flexible bucket sizes |

#### Multi-dimensional Time Series (Buckets + Categories)
```sql
SELECT date_trunc('minute', timestamp), service, count(*) FROM records GROUP BY 1, 2
```

**Characteristics**:
- **Cache Structure**: Partial aggregations for each (time_bucket, category) combination
- **Temporal Filtering**: Applied per (bucket, category) pair
- **Cache Reuse**: Most complex - adds new combinations while preserving existing ones
- **Performance**: Highest potential speedup when categories are stable

#### Performance Implications

**Aggregate Queries**:
- **Best Case**: 2x speedup (process new data only vs all data)
- **Worst Case**: Same as no cache (first run)
- **Scaling**: Linear with new data volume

**Time Series Queries**:
- **Best Case**: 100x+ speedup (only process new time buckets)
- **Worst Case**: Same as no cache (if all buckets are new)
- **Scaling**: Sub-linear with new data volume (depends on bucket size)

**Example Scaling Comparison**:

For 1M historical records, 1K new records:

| Query Type | Without Cache | Cache Hit |
|------------|---------------|-----------|
| Aggregate | Process 1M records | Process 1K records (1000x speedup) |
| Hourly buckets (24 hours) | Process 1M records | Process ~42 records (24Kx speedup) |
| 10-min buckets (144 buckets) | Process 1M records | Process ~7 records (143Kx speedup) |

**Time series queries with small buckets provide the highest cache efficiency** because most new data falls into just a few recent buckets.

### Performance Comparison

The demo program shows the effectiveness of caching across different query types. Here's the measured performance impact:

#### Demo Results Summary

**Dataset**: ~62,000 records with timestamps spanning ~17 hours
**New Data Added**: ~31,000 additional records in second run

| Query Type | Cache Status | Records Processed | Key Operations |
|------------|-------------|-------------------|----------------|
| Simple Aggregate | Cache Miss | All ~62K records | Stores partial aggregations |
| Simple Aggregate | Cache Hit | Only new ~31K records | Combines cached + new data |
| GROUP BY Hour | Cache Miss | All ~62K records | Stores partial aggregations |
| GROUP BY Hour | Cache Hit | Only new ~31K records | UnionExec: cached + filtered new |
| Multiple Aggregates | Cache Miss | All ~62K records | Stores partial aggregations |
| GROUP BY Service | Cache Miss | All ~62K records | Stores partial aggregations |
| Time Series (10-min buckets) | Cache Miss | All ~62K records | Stores partial aggregations by bucket |
| Time Series (10-min buckets) | Cache Hit | Only new ~31K records | UnionExec: cached buckets + new buckets |
| Multi-dim Time Series (5-min + service) | Cache Miss | All ~62K records | Stores partial aggregations by (bucket,service) |
| Time Series with Filtering | Cache Miss | All ~62K records | Stores filtered partial aggregations |

#### Theoretical Performance Scaling

For a production scenario with 1,000,000 historical records and 1,000 new records:

##### Aggregate Queries (No GROUP BY)

| Scenario      | Records Read | Records Filtered | Records Aggregated | Cache Ops | Speedup |
|---------------|--------------|------------------|--------------------|-----------|---------|
| No Caching    | 1,001,000    | 1,001,000        | 500,500            | None      | 1x      |
| Cache Miss    | 1,001,000    | 1,001,000        | 500,500            | 1 write   | 1x      |
| Cache Hit     | 1,001,000    | 1,000            | 1,000              | 1 read + 1 write | ~500x   |

##### Time Series Queries (GROUP BY temporal buckets)

For time series queries, speedup depends on bucket size. Assuming data spans 30 days:

| Bucket Size | Total Buckets | New Buckets | Scenario | Records Processed | Speedup |
|-------------|---------------|-------------|----------|-------------------|---------|
| Hourly      | 720           | 1-2         | No Cache | 1,001,000         | 1x      |
| Hourly      | 720           | 1-2         | Cache Hit| ~1,390 (1,000 + ~390 cached buckets) | ~720x   |
| 10-minute   | 4,320         | 1-6         | No Cache | 1,001,000         | 1x      |
| 10-minute   | 4,320         | 1-6         | Cache Hit| ~232 (1,000 + ~6 new buckets) | ~4,320x |
| 1-minute    | 43,200        | 1-60        | No Cache | 1,001,000         | 1x      |
| 1-minute    | 43,200        | 1-60        | Cache Hit| ~23 (1,000 + ~60 new buckets) | ~43,000x|

**Key Insight**: Time series queries with smaller buckets provide exponentially higher cache efficiency because new data is concentrated in fewer recent buckets.

#### Real-World Factors Affecting Performance

- **Data Distribution**: Cache effectiveness depends on temporal data distribution
- **Query Frequency**: More frequent queries over the same time range = better cache utilization
- **Cache Hit Rate**: Determined by how much historical data is reused
- **Storage Performance**: In-memory cache provides instant access vs disk/network caches
- **Query Complexity**: Simple aggregations benefit more than complex expressions

---

## Implementation Details

### Query Fingerprinting

The fingerprint is generated from the **logical plan's string representation**:

```rust
let fingerprint = plan.display_indent_schema().to_string();
```

**Example output**:
```
Aggregate: groupBy=[[]], aggr=[[ROUND(AVG(records.value), Int64(2)), COUNT(*)]]
  Filter: records.value > Int64(1)
    TableScan: records projection=[timestamp, service, value]
```

**Properties**:
- **Deterministic**: Same query → same fingerprint
- **Schema-aware**: Includes data types
- **Semantic**: Captures query meaning, not syntax
- **Verbose**: Contains full plan tree

**Caveat**: Very similar queries produce different fingerprints (no cache sharing):
```sql
-- Different fingerprints:
SELECT avg(value) FROM records WHERE value > 1
SELECT avg(value) FROM records WHERE value > 2  -- Different filter constant
```

Future optimization: Parameterize constants in fingerprints to enable cache sharing.

### Temporal Column Detection

Configured during setup:

```rust
let config = QueryCacheConfig::new(default_temporal_column, cache)
    .with_temporal_column(additional_column)  // Can add multiple
    .with_group_by_function("date_trunc")     // Allow in GROUP BY
    .with_group_by_function("date_bin");      // Allow in GROUP BY
```

**Validation in Optimizer**:

1. **GROUP BY queries**: Must use an allowed function with a temporal column:
   ```sql
   SELECT date_trunc('hour', timestamp), avg(value)
   FROM records
   GROUP BY 1  -- ✓ Valid: date_trunc(timestamp) is recognized
   ```

2. **Non-GROUP BY queries**: Temporal column must be projected (auto-added if missing):
   ```sql
   SELECT avg(value) FROM records  -- ✓ Valid: timestamp added to projection
   ```

### Dynamic Lower Bound Analysis

**Goal**: Detect filters that change over time (e.g., `timestamp > now() - interval '1 day'`).

**Current Status**: Detection implemented, rewriting **not yet supported** (line 206 returns error).

**Detection Logic**:

```rust
enum DynamicLowerBound {
    Abandon,    // Unstable, can't cache
    Found(expr), // Found dynamic bound
    FoundNow,   // Found now() call
    Stable,     // Static, safe to cache
}

impl DynamicLowerBound {
    fn find(expr: &Expr, columns: &HashSet<Column>) -> Self {
        match expr {
            Expr::BinaryExpr(bin) => {
                // Check for: column >= (expression with now())
                if let (Expr::Column(col), Expr::ScalarFunction(func)) = (&bin.left, &bin.right) {
                    if matches!(func.name(), "now" | "current_timestamp") {
                        return Found(bin.clone());
                    }
                }
                // Recursively check both sides
                let left = Self::find(&bin.left, columns);
                let right = Self::find(&bin.right, columns);
                left.either(right)
            }
            Expr::ScalarFunction(func) if matches!(func.name(), "now" | "current_timestamp") => {
                FoundNow
            }
            Expr::Literal(_) | Expr::Column(_) => Stable,
            _ => Abandon,  // Complex expressions: unsafe
        }
    }
}
```

**Future Implementation**: When rewriting is supported, the cache would:
1. Store both the dynamic filter expression and cached data
2. On cache hit, recompute the dynamic bound (e.g., `now() - 1 day`)
3. If new bound is earlier than cache timestamp, discard cache (data too old)
4. Otherwise, apply filter to cached data to exclude outdated buckets

### Cache Entry Lifecycle

**Trait Definitions** ([`src/cache.rs`](../src/cache.rs)):

```rust
#[async_trait]
pub trait QueryCache: Send + Sync {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<CacheEntry>;
}

pub enum CacheEntry {
    Occupied(Arc<dyn OccupiedCacheEntry>),
    Vacant(Arc<dyn VacantCacheEntry>),
}

#[async_trait]
pub trait OccupiedCacheEntry: Send + Sync {
    fn timestamp(&self) -> i64;
    async fn get(&self) -> DataFusionResult<&[RecordBatch]>;
    async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()>;
}

#[async_trait]
pub trait VacantCacheEntry: Send + Sync {
    async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()>;
}
```

**MemoryQueryCache Implementation**:

```rust
pub struct MemoryQueryCache {
    cache: Arc<Mutex<HashMap<String, (i64, Arc<Vec<RecordBatch>>)>>>,
}

impl QueryCache for MemoryQueryCache {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<CacheEntry> {
        let cache = self.cache.lock().unwrap();
        if let Some((timestamp, record_batch)) = cache.get(query_fingerprint).cloned() {
            Ok(CacheEntry::Occupied(Arc::new(OccupiedMemoryCacheEntry {
                fingerprint: query_fingerprint.to_string(),
                timestamp,
                record_batch,
                cache: self.clone(),
            })))
        } else {
            Ok(CacheEntry::Vacant(Arc::new(VacantMemoryCacheEntry {
                fingerprint: query_fingerprint.to_string(),
                cache: self.clone(),
            })))
        }
    }
}
```

**Storage Format**: `HashMap<String, (i64, Arc<Vec<RecordBatch>>)>`
- Key: Query fingerprint
- Value: (timestamp, partial aggregation batches)

**Eviction**: No automatic eviction in `MemoryQueryCache`. For production use:
- Implement `QueryCache` with TTL-based eviction
- Use external cache (Redis, object store)
- Implement LRU/LFU policies

### Partial Aggregation Internals

DataFusion stores partial aggregation states in record batches with special column names:

**Example: `avg(value)`**

Logical aggregation: `avg(value)`

Physical partial columns:
- `avg(value)[sum]` (Float64): Sum of all values
- `avg(value)[count]` (UInt64): Count of values

Physical final computation:
```
avg(value) = sum(avg(value)[sum]) / sum(avg(value)[count])
```

**Example: Multiple aggregations**

```sql
SELECT count(*), sum(price), avg(price), max(price) FROM stocks
```

Partial batch schema:
```
count(*)[count]: UInt64
sum(price)[sum]: Float64
avg(price)[sum]: Float64
avg(price)[count]: UInt64
max(price)[max]: Float64
```

**Combining Partial Results**:

Cache batch:
```
count(*)[count] | sum(price)[sum] | avg(price)[sum] | avg(price)[count] | max(price)[max]
100             | 15000.0          | 15000.0         | 100               | 200.5
```

New batch:
```
count(*)[count] | sum(price)[sum] | avg(price)[sum] | avg(price)[count] | max(price)[max]
25              | 4000.0           | 4000.0          | 25                | 210.3
```

Combined (via `UnionExec`):
```
count(*)[count] | sum(price)[sum] | avg(price)[sum] | avg(price)[count] | max(price)[max]
100             | 15000.0          | 15000.0         | 100               | 200.5
25              | 4000.0           | 4000.0          | 25                | 210.3
```

Final (via `AggregateExec[Final]`):
```
count(*) | sum(price) | avg(price) | max(price)
125      | 19000.0    | 152.0      | 210.3
```

The Final aggregation:
- Sums the `count(*)[count]` values: 100 + 25 = 125
- Sums the `sum(price)[sum]` values: 15000 + 4000 = 19000
- Computes avg: sum(avg[sum]) / sum(avg[count]) = 19000 / 125 = 152.0
- Takes max of `max(price)[max]`: max(200.5, 210.3) = 210.3

**This is why the cache works**: DataFusion already knows how to combine partial states. The cache simply preserves these states between queries.

### Filter Injection Mechanism

When a cache hit occurs, we need to filter new data to only process records after the cached timestamp.

**Function**: `with_lower_bound()` ([`src/aggregate.rs`](../src/aggregate.rs), lines 477-540)

**Steps**:

1. **Find temporal column in schema**:
```rust
let find_column = agg_exec.input().schema().fields().iter().enumerate()
    .find_map(|(id, f)| {
        if f.name() == &bound_column.name {
            Some((id, f.data_type()))
        } else {
            None
        }
    });
```

2. **Convert timestamp to column's time unit**:
```rust
let lower_bound_scalar = match time_unit {
    TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(lower_bound_ns), None),
    TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(lower_bound_ns / 1000), None),
    TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(lower_bound_ns / 1_000_000), None),
    TimeUnit::Second => ScalarValue::TimestampSecond(Some(lower_bound_ns / 1_000_000_000), None),
};
```

3. **Create physical predicate**:
```rust
let lower_bound_predicate = Arc::new(PhysicalBinaryExpr::new(
    Arc::new(PhysicalColumn::new(&bound_column.name, column_id)),
    Operator::GtEq,  // >=
    Arc::new(PhysicalLiteral::new(lower_bound_scalar)),
));
```

4. **Combine with existing filter** (if present):
```rust
let filter_exec = if let Some(existing_filter) = agg_exec.input().as_any().downcast_ref::<FilterExec>() {
    // AND the predicates together
    let combined_predicate = PhysicalBinaryExpr::new(
        existing_filter.predicate().clone(),
        Operator::And,
        lower_bound_predicate,
    );
    FilterExec::try_new(Arc::new(combined_predicate), existing_filter.input().clone())?
} else {
    // No existing filter, create new one
    FilterExec::try_new(lower_bound_predicate, agg_exec.input().clone())?
};
```

5. **Rebuild AggregateExec with new filter**:
```rust
Arc::new(AggregateExec::try_new(
    *agg_exec.mode(),  // Preserve mode (Partial)
    agg_exec.group_expr().clone(),
    agg_exec.aggr_expr().to_vec(),
    agg_exec.filter_expr().to_vec(),
    Arc::new(filter_exec),  // New input with timestamp filter
    input_schema,
)?)
```

**Result**: The partial aggregation now only processes new data, significantly reducing work.

### Cache Update Mechanism

**Executor**: `CacheUpdateAggregateExec`

**Key Design Decision**: Store the **combined** result (cached + new), not just new data.

**Rationale**:
- Simpler: Cache always contains full historical aggregation
- More efficient: No need to union multiple cache entries
- Trade-off: Larger cache writes (full partial state vs. incremental)

**Alternative Design** (not implemented):
Store only incremental partial results and union multiple cache entries:

```
Cache entries:
- [2024-01-01 00:00] → partial_agg(data up to 2024-01-01 00:00)
- [2024-01-01 10:00] → partial_agg(data from 00:00 to 10:00)
- [2024-01-01 20:00] → partial_agg(data from 10:00 to 20:00)

On query: Union all three entries
```

**Problems with alternative**:
- More complex cache lookup (need to fetch multiple entries)
- More union branches (slower)
- Cache entry management (pruning old entries)
- Higher memory usage during execution

**Current design is simpler and faster for most use cases.**

---

## Code Reference Guide

### Key Files

1. **[`src/lib.rs`](../src/lib.rs)** (118 lines)
   - Public API and configuration
   - `QueryCacheConfig`: Configuration struct
   - `with_query_cache()`: Integration function
   - `QueryCacheQueryPlanner`: Custom query planner implementation

2. **[`src/aggregate.rs`](../src/aggregate.rs)** (874 lines)
   - Core caching logic
   - `QCAggregateOptimizerRule`: Logical plan optimizer
   - `QCAggregatePlanNode`: Extension logical node
   - `QCAggregateExecPlanner`: Physical planner
   - `CachedAggregateExec`: Cache read executor
   - `CacheUpdateAggregateExec`: Cache write executor
   - `DynamicLowerBound`: Time-based filter analysis

3. **[`src/cache.rs`](../src/cache.rs)** (194 lines)
   - Cache abstraction traits
   - `QueryCache`: Main cache interface
   - `CacheEntry`, `OccupiedCacheEntry`, `VacantCacheEntry`: Entry types
   - `MemoryQueryCache`: In-memory implementation

4. **[`src/log.rs`](../src/log.rs)** (114 lines)
   - Logging abstraction
   - `AbstractLog`: Logging trait
   - `LogStderrColors`: Colorized stderr logger
   - `LogNoOp`: No-op logger

5. **[`examples/demo.rs`](../examples/demo.rs)** (183 lines)
   - Working example of cache usage
   - Demonstrates cache hits/misses
   - Shows plan comparison

### Key Functions

#### Optimizer Rule: `QCAggregateOptimizerRule::rewrite()`

**Location**: [`src/aggregate.rs:79-236`](../src/aggregate.rs)

**Purpose**: Examine logical plans and wrap cacheable aggregations.

**Logic Flow**:
```
1. Check if node is Aggregate → if not, skip
2. Find temporal GROUP BY columns → if multiple, skip
3. Analyze filter predicates → if unstable, skip
4. Ensure temporal column is projected → add if missing
5. Create QCAggregatePlanNode extension → return wrapped plan
```

#### Physical Planner: `QCAggregateExecPlanner::plan_extension()`

**Location**: [`src/aggregate.rs:357-475`](../src/aggregate.rs)

**Purpose**: Convert extension nodes to cache-aware physical plans.

**Logic Flow**:
```
1. Detect QCAggregatePlanNode → if not ours, return None
2. Extract physical AggregateExec input
3. Perform cache lookup
4. If cache hit:
   a. Create CachedAggregateExec (read cached data)
   b. Add temporal filter to partial aggregation (new data only)
   c. Union cached + new via UnionExec
   d. Coalesce to single partition
5. If cache miss:
   a. Use original partial aggregation (all data)
6. Wrap in CacheUpdateAggregateExec (store result)
7. Create final AggregateExec on top
```

#### Filter Injection: `with_lower_bound()`

**Location**: [`src/aggregate.rs:477-540`](../src/aggregate.rs)

**Purpose**: Add timestamp >= cache_timestamp filter to physical plan.

**Logic Flow**:
```
1. Find temporal column in schema
2. Convert cache timestamp to column's time unit
3. Create physical predicate: column >= timestamp
4. If existing filter: AND with new predicate
5. Rebuild AggregateExec with new filter
```

#### Cache Read: `execute_get()`

**Location**: [`src/aggregate.rs:744-752`](../src/aggregate.rs)

**Purpose**: Retrieve cached batches.

**Signature**:
```rust
async fn execute_get(
    cache_entry: Arc<dyn OccupiedCacheEntry>,
    metrics: BaselineMetrics,
) -> DataFusionResult<Vec<RecordBatch>>
```

#### Cache Write: `execute_store()`

**Location**: [`src/aggregate.rs:652-665`](../src/aggregate.rs)

**Purpose**: Execute input plan and store result.

**Signature**:
```rust
async fn execute_store(
    input: Arc<dyn ExecutionPlan>,
    cache_entry: CacheEntry,
    now: i64,
    context: Arc<TaskContext>,
    metrics: BaselineMetrics,
) -> DataFusionResult<Vec<RecordBatch>>
```

### Key Structs

#### `QueryCacheConfig`

**Location**: [`src/lib.rs:22-72`](../src/lib.rs)

**Fields**:
```rust
pub struct QueryCacheConfig {
    default_temporal_column: Column,
    temporal_columns: HashSet<Column>,
    group_by_functions: HashSet<String>,
    override_now: Option<i64>,
    cache: Arc<dyn QueryCache>,
}
```

**Builder Methods**:
- `new(column, cache)`: Create config
- `with_temporal_column(col)`: Add temporal column
- `with_group_by_function(fn)`: Allow function in GROUP BY
- `with_override_now(ts)`: Override current time (for testing)

#### `QCAggregatePlanNode`

**Location**: [`src/aggregate.rs:239-339`](../src/aggregate.rs)

**Fields**:
```rust
struct QCAggregatePlanNode {
    input: LogicalPlan,           // Original aggregate plan
    fingerprint: String,          // Cache key
    temporal_column: Column,      // Time-based filtering column
    dynamic_lower_bound: Option<BinaryExpr>,  // Dynamic time filter (if any)
}
```

**Implements**: `UserDefinedLogicalNodeCore`

#### `CachedAggregateExec`

**Location**: [`src/aggregate.rs:668-752`](../src/aggregate.rs)

**Fields**:
```rust
struct CachedAggregateExec {
    cache_entry: Arc<dyn OccupiedCacheEntry>,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}
```

**Implements**: `ExecutionPlan`

**Children**: None (leaf executor)

#### `CacheUpdateAggregateExec`

**Location**: [`src/aggregate.rs:563-665`](../src/aggregate.rs)

**Fields**:
```rust
struct CacheUpdateAggregateExec {
    cache_entry: CacheEntry,
    input: Arc<dyn ExecutionPlan>,
    now: i64,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}
```

**Implements**: `ExecutionPlan`

**Children**: One (the input plan to execute and cache)

### Configuration Example

```rust
use datafusion::prelude::{SessionContext, SessionConfig};
use datafusion::execution::{SessionStateBuilder, RuntimeEnv};
use datafusion::common::Column;
use datafusion_query_cache::{
    with_query_cache_log, QueryCacheConfig, MemoryQueryCache, LogStderrColors
};
use std::sync::Arc;

async fn setup_cached_context() -> SessionContext {
    // Standard DataFusion setup
    let config = SessionConfig::new().with_target_partitions(4);
    let runtime = Arc::new(RuntimeEnv::default());
    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features();
    
    // Define temporal column
    let temporal_col = Column::new(
        Some("events".to_string()),  // Table name
        "timestamp".to_string()       // Column name
    );
    
    // Create cache
    let cache = Arc::new(MemoryQueryCache::default());
    
    // Configure query cache
    let query_cache_config = QueryCacheConfig::new(temporal_col, cache)
        .with_group_by_function("date_trunc")   // Allow in GROUP BY
        .with_group_by_function("date_bin");     // Allow in GROUP BY
    
    // Enable logging
    let log = LogStderrColors::default();
    
    // Register cache with session state
    let state_builder = with_query_cache_log(state_builder, query_cache_config, log);
    
    SessionContext::new_with_state(state_builder.build())
}
```

---

## Summary

The DataFusion Query Cache is a sophisticated system that:

1. **Leverages DataFusion's partial aggregation mechanism** to store and combine intermediate results
2. **Intercepts query execution at multiple stages** (optimization, physical planning, execution)
3. **Uses standard DataFusion extension points** (OptimizerRule, ExtensionPlanner, ExecutionPlan)
4. **Minimizes recomputation** by caching partial aggregations and filtering new data based on timestamps
5. **Requires minimal configuration** (just specify temporal columns and allowed GROUP BY functions)

The key insight is that **partial aggregation states are naturally combinable**—the cache exploits this property to dramatically reduce computation for repeated queries over time-series data.

For time-series workloads with frequent queries over growing datasets, this cache can provide orders of magnitude speedup by processing only new data on subsequent runs.

---

## Frequently Asked Questions

### Q1: How does the codebase handle queries with temporal functions like `now()`?

**Short Answer**: Detection is implemented but full support is **not yet available**. Queries with `now()` or time-relative filters will be rejected from caching.

**Detailed Explanation**:

The codebase has sophisticated logic to detect "dynamic lower bounds"—filters that change over time, such as:
```sql
SELECT avg(value) FROM records WHERE timestamp > now() - interval '1 day'
```

**Detection Logic** ([`src/aggregate.rs:754-873`](../src/aggregate.rs)):

```rust
enum DynamicLowerBound {
    Abandon,     // Unstable expression, cannot cache
    Found(expr), // Found a dynamic lower bound expression
    FoundNow,    // Found now() function call
    Stable,      // Static expression, safe to cache
}

impl DynamicLowerBound {
    fn find(expr: &Expr, columns: &HashSet<Column>) -> Self {
        match expr {
            Expr::ScalarFunction(scalar) => {
                // Detect now(), current_timestamp(), current_date()
                if matches!(scalar.name(), "now" | "current_timestamp" | "current_date") {
                    return Self::FoundNow;
                }
                Self::Abandon
            }
            Expr::BinaryExpr(bin_expr) => {
                // Check if: column >= (expression with now())
                let left = Self::find(&bin_expr.left, columns);
                let right = Self::find(&bin_expr.right, columns);
                
                // If we find: timestamp > now() - interval
                if matches!(left, Self::Column) && matches!(right, Self::FoundNow) {
                    return Self::Found(bin_expr.clone());
                }
                
                left.either(right)
            }
            _ => Self::Stable,
        }
    }
}
```

**Where Detection Happens** ([`src/aggregate.rs:120-140`](../src/aggregate.rs)):

```rust
// In QCAggregateOptimizerRule::rewrite()
let (dynamic_lower_bound, input) = if let LogicalPlan::Filter(filter) = &agg_input {
    let dlb = DynamicLowerBound::find(&filter.predicate, &needle_columns);
    match dlb {
        DynamicLowerBound::Found(bin_expr) => Some(bin_expr),
        DynamicLowerBound::Stable => None,
        _ => {
            // Found unstable expression - reject caching
            self.log.info(&fingerprint, "we found an unstable expression, caching not possible")?;
            return Ok(Transformed::no(plan));
        }
    }
} else {
    (None, agg_input)
};
```

**Current Limitation** ([`src/aggregate.rs:205-207`](../src/aggregate.rs)):

```rust
if dynamic_lower_bound.is_some() {
    return plan_err!("dynamic lower bound not yet supported");
}
```

**Why It's Hard**: When a query has `timestamp > now() - interval '1 day'`, the cache must:
1. Store the dynamic expression alongside cached data
2. On cache hit, **recompute** the dynamic bound (e.g., evaluate `now() - 1 day` at the new query time)
3. Determine if cached data is still valid:
   - If new bound is **earlier** than cache timestamp → cached data is too recent, can't use it
   - If new bound is **later** than cache timestamp → need to filter cached data to exclude old buckets
4. For GROUP BY queries, potentially discard entire time buckets from cache

**Example Problem**:
```sql
-- Query at 10:00 AM: timestamp > now() - interval '1 hour'
-- Caches data from 09:00-10:00

-- Query at 11:00 AM: timestamp > now() - interval '1 hour'  
-- Should only return 10:00-11:00, NOT 09:00-11:00
-- Need to filter/discard the cached 09:00-10:00 bucket
```

**Current Workaround**: Use static time bounds:
```sql
-- Instead of:
WHERE timestamp > now() - interval '1 day'

-- Use:
WHERE timestamp > '2024-01-01 00:00:00'  -- Static bound, cacheable
```

---

### Q2: What happens when the same query is executed after 1 hour? Will we get stale data from cache?

**Short Answer**: **No stale data**. The cache correctly combines old cached results with new data, giving you up-to-date results.

**How It Works**:

When you execute the same query multiple times, the cache uses the **query execution timestamp** to partition old vs. new data:

**First Execution (10:00 AM)**:
```sql
SELECT avg(price) FROM stocks WHERE symbol = 'AAPL'
```

**What happens** ([`src/aggregate.rs:410-425`](../src/aggregate.rs)):
```rust
// In QCAggregateExecPlanner::plan_extension()
let cache_entry = self.config.cache().entry(&fingerprint).await?;
// Returns: CacheEntry::Vacant (no cached data)

let now = session_state
    .execution_props()
    .query_execution_start_time
    .timestamp_nanos_opt()
    .unwrap();
// now = 1704880800000000000 (10:00 AM in nanoseconds)
```

Cache stores:
- Partial aggregation of all data up to 10:00 AM
- Timestamp: `1704880800000000000`

**Second Execution (11:00 AM - 1 hour later)**:

**Cache Lookup** ([`src/aggregate.rs:410-416`](../src/aggregate.rs)):
```rust
let cache_entry = self.config.cache().entry(&fingerprint).await?;
// Returns: CacheEntry::Occupied(entry)
// entry.timestamp() = 1704880800000000000 (10:00 AM)

log_info!("Cache HIT: Using cached data from timestamp {}", entry.timestamp());
```

**Plan Rewriting** ([`src/aggregate.rs:429-451`](../src/aggregate.rs)):
```rust
let input_exec = match &cache_entry {
    CacheEntry::Occupied(entry) => {
        // Read cached partial aggregation (data up to 10:00 AM)
        let cached_exec = CachedAggregateExec::new_exec_plan(entry.clone(), ...);
        
        // Compute new partial aggregation with timestamp filter
        let new_exec = with_lower_bound(
            &partial_agg_exec,
            &temporal_column,
            entry.timestamp()  // Filter: timestamp >= 10:00 AM
        )?;
        
        // Union: cached data (< 10:00 AM) + new data (>= 10:00 AM)
        Arc::new(UnionExec::new(vec![cached_exec, new_exec]))
    }
    // ...
};
```

**Filter Injection** ([`src/aggregate.rs:477-540`](../src/aggregate.rs)):
```rust
fn with_lower_bound(
    partial_agg_exec: &Arc<dyn ExecutionPlan>,
    bound_column: &Column,
    lower_bound_ns: i64,  // 1704880800000000000 (10:00 AM)
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // Creates predicate: timestamp >= 1704880800000000000
    let lower_bound_predicate = Arc::new(PhysicalBinaryExpr::new(
        Arc::new(PhysicalColumn::new(&bound_column.name, column_id)),
        Operator::GtEq,  // Greater than or equal
        Arc::new(PhysicalLiteral::new(lower_bound_scalar)),
    ));
    
    // Adds filter to the physical plan
    let filter_exec = FilterExec::try_new(lower_bound_predicate, input)?;
    // ...
}
```

**Result**:
- **Cached data**: Partial aggregations computed at 10:00 AM (all data before 10:00 AM)
- **New data**: Partial aggregations computed at 11:00 AM (only data from 10:00-11:00 AM)
- **Combined**: UnionExec merges both partial results
- **Final**: AggregateExec[Final] combines into up-to-date answer

**Key Point**: The cache stores the **timestamp when the query was executed**, and subsequent queries only process data after that timestamp. This ensures:
- ✅ **No stale data**: Results always include the latest data
- ✅ **No duplicate processing**: Old data is retrieved from cache
- ✅ **Correct aggregations**: Partial states combine properly

**Cache Update** ([`src/aggregate.rs:652-665`](../src/aggregate.rs)):
```rust
async fn execute_store(
    input: Arc<dyn ExecutionPlan>,
    cache_entry: CacheEntry,
    now: i64,  // 1704884400000000000 (11:00 AM)
    context: Arc<TaskContext>,
) -> DataFusionResult<Vec<RecordBatch>> {
    // Execute input (cached data + new data combined)
    let batches = collect(input, context).await?;
    
    // Update cache with combined result and new timestamp
    cache_entry.put(now, &batches).await?;
    
    Ok(batches)
}
```

After the 11:00 AM query:
- Cache now contains: Combined partial aggregation (all data up to 11:00 AM)
- New timestamp: `1704884400000000000` (11:00 AM)

**Third Execution (12:00 PM)** will use the 11:00 AM cache, process only 11:00-12:00 data, and so on.

---

### Q3: What happens when we change the filter constant (e.g., `time > '2025-01-01'` to `time > '2024-12-01'`)? Will we get a partial cache hit?

**Short Answer**: **No cache hit at all**. The queries have different fingerprints, so they are treated as completely separate queries with independent cache entries.

**Why No Cache Sharing**:

**Query Fingerprinting** ([`src/aggregate.rs:84`](../src/aggregate.rs)):
```rust
// In QCAggregateOptimizerRule::rewrite()
let mut fingerprint = plan.display_indent_schema().to_string();
```

The fingerprint is generated from the **entire logical plan**, including filter constants.

**Example**:

**Query 1**:
```sql
SELECT avg(price) FROM stocks WHERE timestamp > '2025-01-01'
```

**Fingerprint 1**:
```
Aggregate: groupBy=[[]], aggr=[[AVG(stocks.price)]]
  Filter: stocks.timestamp > Utf8("2025-01-01")
    TableScan: stocks projection=[timestamp, price]
```

**Query 2**:
```sql
SELECT avg(price) FROM stocks WHERE timestamp > '2024-12-01'
```

**Fingerprint 2**:
```
Aggregate: groupBy=[[]], aggr=[[AVG(stocks.price)]]
  Filter: stocks.timestamp > Utf8("2024-12-01")  ← Different constant
    TableScan: stocks projection=[timestamp, price]
```

**Cache Lookup** ([`src/aggregate.rs:410`](../src/aggregate.rs)):
```rust
let cache_entry = self.config.cache().entry(&agg_node.fingerprint).await?;
```

Since `fingerprint_1 != fingerprint_2`, they use different cache entries:

**MemoryQueryCache Implementation** ([`src/cache.rs:135-152`](../src/cache.rs)):
```rust
impl QueryCache for MemoryQueryCache {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<CacheEntry> {
        let cache = self.cache.lock().unwrap();
        
        // Lookup by exact fingerprint match
        if let Some((timestamp, record_batch)) = cache.get(query_fingerprint).cloned() {
            Ok(CacheEntry::Occupied(...))
        } else {
            Ok(CacheEntry::Vacant(...))  // No match = cache miss
        }
    }
}

// Internal storage:
pub struct MemoryQueryCache {
    cache: Arc<Mutex<HashMap<String, (i64, Arc<Vec<RecordBatch>>)>>>,
    //                      ^^^^^^ key is the fingerprint string
}
```

**What Actually Happens**:

1. **Query 1 execution** (`timestamp > '2025-01-01'`):
   - Fingerprint: `"...timestamp > Utf8(\"2025-01-01\")..."`
   - Cache miss
   - Computes and stores result under fingerprint_1

2. **Query 2 execution** (`timestamp > '2024-12-01'`):
   - Fingerprint: `"...timestamp > Utf8(\"2024-12-01\")..."`
   - Cache lookup with fingerprint_2
   - **Cache miss** (fingerprint_2 not in cache)
   - Computes from scratch and stores under fingerprint_2

**Result**: You now have **two separate cache entries**, even though Query 2 could have reused part of Query 1's cached data.

**Why This Matters**:

Changing the filter constant creates a cache miss even when data overlap exists:

```
Timeline: |-------- 2024 --------|-------- 2025 --------|

Query 1: timestamp > 2025-01-01
         Cached data: [2025-01-01 onwards]

Query 2: timestamp > 2024-12-01
         Cached data: [2024-12-01 onwards]
         
Could theoretically reuse Query 1's cache for [2025-01-01 onwards]
and only compute [2024-12-01 to 2025-01-01], but doesn't.
```

**Limitations and Design Trade-offs**:

This is a **known limitation** mentioned in the Future Enhancements:

> **Query Parameterization**: Share cache entries between similar queries with different constants

**Why Not Implemented**:

1. **Complexity**: Would require:
   - Parameterizing filter constants in fingerprints
   - Storing parameter values separately
   - Complex cache lookup logic to find compatible entries
   - Filtering cached data based on parameter differences

2. **Correctness concerns**: 
   - `time > '2025-01-01'` vs `time > '2024-12-01'` means Query 2 needs MORE data
   - Can't just reuse Query 1's cache (it doesn't have 2024 data)
   - Would need to detect "compatible" caches (where cached bound ≥ query bound)

3. **Current design philosophy**: 
   - Simple and correct > complex and optimized
   - Exact fingerprint matching is easier to reason about
   - No risk of accidentally using incompatible cached data

**Code Location for Decision** ([`src/cache.rs:136-137`](../src/cache.rs)):
```rust
// The decision happens here: exact string match
if let Some((timestamp, record_batch)) = cache.get(query_fingerprint).cloned() {
```

If `query_fingerprint` doesn't exactly match any key in the HashMap, it's a cache miss. There's no logic to find "similar" or "compatible" cache entries.

**Workaround**: 

If you need to query different time ranges frequently, consider:
1. **Use the broader range consistently**: Always query from the earliest date you need
2. **Post-filter in application**: Query wider range from cache, filter results in app code
3. **Implement custom cache**: Write a `QueryCache` implementation with parameterization logic

**Example**:
```rust
// Instead of varying the filter:
// ❌ SELECT avg(price) FROM stocks WHERE timestamp > $dynamic_date

// Use a fixed filter and post-process:
// ✅ SELECT timestamp, price FROM stocks WHERE timestamp > '2020-01-01'
// Then filter in application based on actual needs
```

---

## Future Enhancements

1. **Dynamic Lower Bound Support**: Full implementation of time-relative filters (`now() - interval '1 day'`)
2. **Cache Invalidation**: TTL-based or manual invalidation mechanisms
3. **Distributed Caching**: Object store backend for multi-node deployments
4. **Query Parameterization**: Share cache entries between similar queries with different constants (addresses Q3 above)
5. **Selective Caching**: Cost-based decision on whether to cache (small tables may not benefit)
6. **Cache Statistics**: Hit rate, storage size, eviction metrics
7. **Incremental Cache**: Store deltas instead of full combined results (trade-off analysis needed)
8. **Non-Aggregation Queries**: Support simple filter queries, projections
9. **Multi-Table Support**: Handle joins, subqueries, CTEs

---

**For questions or contributions, see the [main README](../README.md) and [examples/demo.rs](../examples/demo.rs).**

