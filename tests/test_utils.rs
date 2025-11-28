use datafusion::common::Column;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_query_cache::{MemoryQueryCache, QueryCacheConfig, TimeInterval};
use std::sync::Arc;

/// Information about cache intervals found in a physical plan
#[derive(Debug, Clone, PartialEq)]
pub struct CacheIntervalInfo {
    pub cached_intervals: Vec<TimeInterval>,
    pub gap_intervals: Vec<TimeInterval>,
}

/// Execution metrics extracted from a physical plan
#[derive(Debug, Clone, PartialEq)]
pub struct ExecutionMetrics {
    pub bytes_scanned: Option<usize>,
    pub rows_produced: Option<usize>,
    pub elapsed_compute_ns: Option<u64>,
}

/// Builder for creating test session contexts with configurable settings
#[derive(Debug)]
pub struct TestContextBuilder {
    cache: Option<Arc<MemoryQueryCache>>,
    temporal_column: Option<Column>,
    target_partitions: usize,
    enable_default_features: bool,
}

impl Default for TestContextBuilder {
    fn default() -> Self {
        Self {
            cache: None,
            temporal_column: None,
            target_partitions: 1,
            enable_default_features: true,
        }
    }
}

impl TestContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_cache(mut self, cache: Arc<MemoryQueryCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn with_temporal_column(mut self, table: &str, column: &str) -> Self {
        self.temporal_column = Some(Column::new(Some(table.to_string()), column.to_string()));
        self
    }

    pub fn with_target_partitions(mut self, partitions: usize) -> Self {
        self.target_partitions = partitions;
        self
    }

    pub fn build_vanilla(self) -> SessionContext {
        let config = SessionConfig::new().with_target_partitions(self.target_partitions);
        let runtime = Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default());
        let state_builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime);

        let state_builder = if self.enable_default_features {
            state_builder.with_default_features()
        } else {
            state_builder
        };

        SessionContext::new_with_state(state_builder.build())
    }

    pub fn build_cached(self) -> SessionContext {
        let cache = self.cache.expect("Cache must be set for cached context");
        let temporal_column = self.temporal_column.expect("Temporal column must be set for cached context");

        let config = SessionConfig::new().with_target_partitions(self.target_partitions);
        let runtime = Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default());
        let state_builder = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_default_features();

        let query_cache_config = QueryCacheConfig::new(temporal_column, cache);

        let state_builder = datafusion_query_cache::with_query_cache(
            state_builder,
            query_cache_config
        );

        SessionContext::new_with_state(state_builder.build())
    }
}

/// Format a logical plan for consistent display and comparison
pub fn format_logical_plan(plan: &LogicalPlan) -> String {
    format!("{}", plan.display_indent_schema())
}

/// Format a physical plan for consistent display and comparison
pub fn format_physical_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(true))
}

/// Normalize a plan string for comparison by removing volatile details
pub fn normalize_plan_for_comparison(plan_str: &str) -> String {
    let mut result = plan_str.to_string();

    // Remove memory addresses (pointers)
    let addr_re = regex::Regex::new(r"0x[0-9a-fA-F]+").unwrap();
    result = addr_re.replace_all(&result, "0x...").to_string();

    // Normalize timestamps (replace actual timestamp values with placeholders)
    let timestamp_re = regex::Regex::new(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?").unwrap();
    result = timestamp_re.replace_all(&result, "TIMESTAMP_PLACEHOLDER").to_string();

    // Normalize numeric literals in expressions
    let numeric_re = regex::Regex::new(r"(\w+):\s*(\w+)\s*\[([^\]]+)\]").unwrap();
    result = numeric_re.replace_all(&result, "$1: $2 [SCHEMA_NORMALIZED]").to_string();

    result
}

/// Extract cache interval information from a physical plan
pub fn collect_exec_intervals(plan: &Arc<dyn ExecutionPlan>) -> CacheIntervalInfo {
    let mut cached_intervals = Vec::new();
    let mut gap_intervals = Vec::new();

    // Walk the plan tree to find execution nodes
    fn walk_plan(plan: &Arc<dyn ExecutionPlan>, cached: &mut Vec<TimeInterval>, gaps: &mut Vec<TimeInterval>) {
        // Check if this is a CachedAggregateExec
        if plan.name() == "CachedAggregateExec" {
            // Try to downcast to our custom exec to get the interval
            if let Some(cached_exec) = plan.as_any().downcast_ref::<datafusion_query_cache::CachedAggregateExec>() {
                cached.push(cached_exec.interval());
            }
        }
        // Check if this is a CacheUpdateAggregateExec
        else if plan.name() == "CacheUpdateAggregateExec" {
            if let Some(update_exec) = plan.as_any().downcast_ref::<datafusion_query_cache::CacheUpdateAggregateExec>() {
                gaps.push(update_exec.interval());
            }
        }

        // Recursively walk children
        for child in plan.children() {
            walk_plan(child, cached, gaps);
        }
    }

    walk_plan(plan, &mut cached_intervals, &mut gap_intervals);
    CacheIntervalInfo {
        cached_intervals,
        gap_intervals,
    }
}

/// Check if a physical plan contains cache hits (uses cached data)
pub fn has_cache_hit(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let info = collect_exec_intervals(plan);
    !info.cached_intervals.is_empty()
}

/// Check if a physical plan contains cache misses (computes new data)
pub fn has_cache_miss(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let info = collect_exec_intervals(plan);
    !info.gap_intervals.is_empty()
}

/// Extract bytes scanned metric from a physical plan
pub fn get_bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    fn walk_plan(plan: &Arc<dyn ExecutionPlan>, total_bytes: &mut usize) {
        if let Some(metrics) = plan.metrics() {
            if let Some(bytes) = metrics
                .sum(|metric| metric.value().name() == "bytes_scanned")
                .map(|v| v.as_usize())
            {
                *total_bytes += bytes;
            }
        }

        // Recursively walk children
        for child in plan.children() {
            walk_plan(child, total_bytes);
        }
    }

    let mut total_bytes = 0;
    walk_plan(plan, &mut total_bytes);
    if total_bytes > 0 {
        Some(total_bytes)
    } else {
        None
    }
}

/// Extract rows produced metric from a physical plan
pub fn get_rows_produced(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    if let Some(metrics) = plan.metrics() {
        metrics
            .sum(|metric| metric.value().name() == "output_rows")
            .map(|v| v.as_usize())
    } else {
        None
    }
}

/// Extract comprehensive execution metrics from a physical plan
pub fn get_execution_metrics(plan: &Arc<dyn ExecutionPlan>) -> ExecutionMetrics {
    ExecutionMetrics {
        bytes_scanned: get_bytes_scanned(plan),
        rows_produced: get_rows_produced(plan),
        elapsed_compute_ns: None, // TODO: implement when metrics are available
    }
}

/// Execute a query and return both logical and physical plans
pub async fn get_plans_as_strings(
    ctx: &SessionContext,
    query: &str
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let df = ctx.sql(query).await?;
    let logical_plan = df.logical_plan();
    let logical_plan_str = format_logical_plan(logical_plan);

    let physical_plan = ctx.state().create_physical_plan(&logical_plan.clone()).await?;
    let physical_plan_str = format_physical_plan(&physical_plan);

    Ok((logical_plan_str, physical_plan_str))
}

/// Execute a query and return results as record batches
pub async fn execute_query(
    ctx: &SessionContext,
    query: &str
) -> Result<Vec<datafusion::arrow::array::RecordBatch>, Box<dyn std::error::Error>> {
    let df = ctx.sql(query).await?;
    let results = df.collect().await?;
    Ok(results)
}

/// Create a simple test table with timestamp and value columns
pub fn create_test_table(
    _schema_name: &str,
    _table_name: &str
) -> datafusion::datasource::MemTable {
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("service", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    // Create some test data
    let timestamps: Vec<i64> = (0..100).map(|i| 1_700_000_000_000_000 + i * 1_000_000).collect();
    let services: Vec<&str> = (0..100).map(|i| ["service_a", "service_b", "service_c"][i % 3]).collect();
    let values: Vec<i64> = (0..100).map(|i| i as i64 * 10).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(services)),
            Arc::new(Int64Array::from(values)),
        ],
    ).unwrap();

    datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap()
}
