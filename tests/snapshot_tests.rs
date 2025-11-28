use datafusion_query_cache::MemoryQueryCache;
use std::sync::Arc;

mod test_utils;

/// Snapshot test for simple aggregate query logical plan
#[tokio::test]
async fn test_simple_aggregate_logical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    let df = ctx.sql(query).await.unwrap();
    let logical_plan = df.logical_plan();
    let logical_plan_str = test_utils::format_logical_plan(logical_plan);

    // Snapshot the logical plan
    insta::assert_snapshot!("simple_aggregate_logical_plan", logical_plan_str);
}

/// Snapshot test for simple aggregate query physical plan
#[tokio::test]
async fn test_simple_aggregate_physical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let physical_plan_str = test_utils::format_physical_plan(&physical_plan);

    // Snapshot the physical plan
    insta::assert_snapshot!("simple_aggregate_physical_plan", physical_plan_str);
}

/// Snapshot test for aggregate with GROUP BY logical plan
#[tokio::test]
async fn test_group_by_aggregate_logical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT date_trunc('hour', timestamp), count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' GROUP BY 1";

    let df = ctx.sql(query).await.unwrap();
    let logical_plan = df.logical_plan();
    let logical_plan_str = test_utils::format_logical_plan(logical_plan);

    // Snapshot the logical plan
    insta::assert_snapshot!("group_by_aggregate_logical_plan", logical_plan_str);
}

/// Snapshot test for aggregate with GROUP BY physical plan
#[tokio::test]
async fn test_group_by_aggregate_physical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT date_trunc('hour', timestamp), count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' GROUP BY 1";

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let physical_plan_str = test_utils::format_physical_plan(&physical_plan);

    // Snapshot the physical plan
    insta::assert_snapshot!("group_by_aggregate_physical_plan", physical_plan_str);
}

/// Snapshot test for aliased table query logical plan
#[tokio::test]
async fn test_aliased_table_logical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("t", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-02T00:00:00Z'";

    let df = ctx.sql(query).await.unwrap();
    let logical_plan = df.logical_plan();
    let logical_plan_str = test_utils::format_logical_plan(logical_plan);

    // Snapshot the logical plan
    insta::assert_snapshot!("aliased_table_logical_plan", logical_plan_str);
}

/// Snapshot test for aliased table query physical plan
#[tokio::test]
async fn test_aliased_table_physical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("t", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-02T00:00:00Z'";

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let physical_plan_str = test_utils::format_physical_plan(&physical_plan);

    // Snapshot the physical plan
    insta::assert_snapshot!("aliased_table_physical_plan", physical_plan_str);
}

/// Snapshot test for subquery logical plan
#[tokio::test]
async fn test_subquery_logical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("inner_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT * FROM (SELECT count(*) as cnt FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z') AS inner_table WHERE inner_table.cnt > 0";

    let df = ctx.sql(query).await.unwrap();
    let logical_plan = df.logical_plan();
    let logical_plan_str = test_utils::format_logical_plan(logical_plan);

    // Snapshot the logical plan
    insta::assert_snapshot!("subquery_logical_plan", logical_plan_str);
}

/// Snapshot test for subquery physical plan
#[tokio::test]
async fn test_subquery_physical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("inner_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT * FROM (SELECT count(*) as cnt FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z') AS inner_table WHERE inner_table.cnt > 0";

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let physical_plan_str = test_utils::format_physical_plan(&physical_plan);

    // Snapshot the physical plan
    insta::assert_snapshot!("subquery_physical_plan", physical_plan_str);
}

/// Snapshot test for complex temporal filters logical plan
#[tokio::test]
async fn test_complex_temporal_filters_logical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' AND service = 'service_a'";

    let df = ctx.sql(query).await.unwrap();
    let logical_plan = df.logical_plan();
    let logical_plan_str = test_utils::format_logical_plan(logical_plan);

    // Snapshot the logical plan
    insta::assert_snapshot!("complex_temporal_filters_logical_plan", logical_plan_str);
}

/// Snapshot test for complex temporal filters physical plan
#[tokio::test]
async fn test_complex_temporal_filters_physical_plan_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' AND service = 'service_a'";

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let physical_plan_str = test_utils::format_physical_plan(&physical_plan);

    // Snapshot the physical plan
    insta::assert_snapshot!("complex_temporal_filters_physical_plan", physical_plan_str);
}

/// Snapshot test comparing vanilla vs cached context logical plans
#[tokio::test]
async fn test_vanilla_vs_cached_logical_plan_comparison() {
    // Register test table for both contexts
    let table = test_utils::create_test_table("test_schema", "test_table");

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    // Vanilla context
    let vanilla_ctx = test_utils::TestContextBuilder::new().build_vanilla();
    vanilla_ctx.register_table("test_table", Arc::new(test_utils::create_test_table("test_schema", "test_table"))).unwrap();

    let vanilla_df = vanilla_ctx.sql(query).await.unwrap();
    let vanilla_logical_plan = vanilla_df.logical_plan();
    let vanilla_logical_str = test_utils::format_logical_plan(vanilla_logical_plan);

    // Cached context
    let cache = Arc::new(MemoryQueryCache::default());
    let cached_ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();
    cached_ctx.register_table("test_table", Arc::new(table)).unwrap();

    let cached_df = cached_ctx.sql(query).await.unwrap();
    let cached_logical_plan = cached_df.logical_plan();
    let cached_logical_str = test_utils::format_logical_plan(cached_logical_plan);

    // Create combined snapshot showing both
    let comparison = format!(
        "VANILLA LOGICAL PLAN:\n{}\n\nCACHED LOGICAL PLAN:\n{}",
        vanilla_logical_str, cached_logical_str
    );

    insta::assert_snapshot!("vanilla_vs_cached_logical_plan_comparison", comparison);
}

/// Snapshot test comparing vanilla vs cached context physical plans
#[tokio::test]
async fn test_vanilla_vs_cached_physical_plan_comparison() {
    // Register test table for both contexts
    let table = test_utils::create_test_table("test_schema", "test_table");

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    // Vanilla context
    let vanilla_ctx = test_utils::TestContextBuilder::new().build_vanilla();
    vanilla_ctx.register_table("test_table", Arc::new(test_utils::create_test_table("test_schema", "test_table"))).unwrap();

    let vanilla_df = vanilla_ctx.sql(query).await.unwrap();
    let vanilla_physical_plan = vanilla_ctx.state().create_physical_plan(vanilla_df.logical_plan()).await.unwrap();
    let vanilla_physical_str = test_utils::format_physical_plan(&vanilla_physical_plan);

    // Cached context
    let cache = Arc::new(MemoryQueryCache::default());
    let cached_ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();
    cached_ctx.register_table("test_table", Arc::new(table)).unwrap();

    let cached_df = cached_ctx.sql(query).await.unwrap();
    let cached_physical_plan = cached_ctx.state().create_physical_plan(cached_df.logical_plan()).await.unwrap();
    let cached_physical_str = test_utils::format_physical_plan(&cached_physical_plan);

    // Create combined snapshot showing both
    let comparison = format!(
        "VANILLA PHYSICAL PLAN:\n{}\n\nCACHED PHYSICAL PLAN:\n{}",
        vanilla_physical_str, cached_physical_str
    );

    insta::assert_snapshot!("vanilla_vs_cached_physical_plan_comparison", comparison);
}

/// Snapshot test for cache interval information
#[tokio::test]
async fn test_cache_interval_info_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    // First execution - cache miss
    let df1 = ctx.sql(query).await.unwrap();
    let physical_plan1 = ctx.state().create_physical_plan(df1.logical_plan()).await.unwrap();
    let cache_info1 = test_utils::collect_exec_intervals(&physical_plan1);

    // Second execution - cache hit
    let df2 = ctx.sql(query).await.unwrap();
    let physical_plan2 = ctx.state().create_physical_plan(df2.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);

    let cache_info_snapshot = format!(
        "FIRST EXECUTION (CACHE MISS):\nCached intervals: {:?}\nGap intervals: {:?}\n\nSECOND EXECUTION (CACHE HIT):\nCached intervals: {:?}\nGap intervals: {:?}",
        cache_info1.cached_intervals, cache_info1.gap_intervals,
        cache_info2.cached_intervals, cache_info2.gap_intervals
    );

    insta::assert_snapshot!("cache_interval_info_snapshot", cache_info_snapshot);
}

/// Snapshot test for execution metrics
#[tokio::test]
async fn test_execution_metrics_snapshot() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    // First execution - cache miss
    let df1 = ctx.sql(query).await.unwrap();
    let physical_plan1 = ctx.state().create_physical_plan(df1.logical_plan()).await.unwrap();
    let metrics1 = test_utils::get_execution_metrics(&physical_plan1);
    let _results1 = df1.collect().await.unwrap();

    // Second execution - cache hit
    let df2 = ctx.sql(query).await.unwrap();
    let physical_plan2 = ctx.state().create_physical_plan(df2.logical_plan()).await.unwrap();
    let metrics2 = test_utils::get_execution_metrics(&physical_plan2);
    let _results2 = df2.collect().await.unwrap();

    let metrics_snapshot = format!(
        "FIRST EXECUTION (CACHE MISS):\nBytes scanned: {:?}\nRows produced: {:?}\nElapsed compute ns: {:?}\n\nSECOND EXECUTION (CACHE HIT):\nBytes scanned: {:?}\nRows produced: {:?}\nElapsed compute ns: {:?}",
        metrics1.bytes_scanned, metrics1.rows_produced, metrics1.elapsed_compute_ns,
        metrics2.bytes_scanned, metrics2.rows_produced, metrics2.elapsed_compute_ns
    );

    insta::assert_snapshot!("execution_metrics_snapshot", metrics_snapshot);
}
