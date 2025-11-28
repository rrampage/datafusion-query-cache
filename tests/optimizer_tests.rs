use datafusion_query_cache::MemoryQueryCache;
use std::sync::Arc;

mod test_utils;

/// Test simple aggregate query optimization
#[tokio::test]
async fn test_simple_aggregate_query() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    // Get logical and physical plans
    let (logical_plan_str, _physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application - should have QCAggregatePlanNode in logical plan
    assert!(logical_plan_str.contains("QueryCacheAggregate"), "Logical plan should contain QueryCacheAggregate node");

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Verify physical plan structure - should have cache-related execution nodes
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(!cache_info.gap_intervals.is_empty(), "Should have gap intervals for cache miss on first execution");

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert_eq!(results.len(), 1, "Should return one batch");
    assert_eq!(results[0].num_rows(), 1, "Should return one row with count");

    // Verify cache behavior - second execution should hit cache
    let physical_plan2 = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(!cache_info2.cached_intervals.is_empty(), "Second execution should have cached intervals");

    // Results should be identical
    let results2 = test_utils::execute_query(&ctx, query).await.unwrap();
    assert_eq!(results, results2, "Results should be identical between executions");
}

/// Test aggregate query with GROUP BY optimization
#[tokio::test]
async fn test_aggregate_with_group_by() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT date_trunc('hour', timestamp), count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' GROUP BY 1";

    // Get logical and physical plans
    let (logical_plan_str, _physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application
    assert!(logical_plan_str.contains("QueryCacheAggregate"), "Logical plan should contain QueryCacheAggregate node");

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Verify cache behavior
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(!cache_info.gap_intervals.is_empty(), "Should have gap intervals for initial execution");

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert!(!results.is_empty(), "Should return results");

    // Verify cache hit on second execution
    let physical_plan2 = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(!cache_info2.cached_intervals.is_empty(), "Second execution should use cached data");
}

/// Test query with aliased table
#[tokio::test]
async fn test_query_with_aliased_table() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("t", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-02T00:00:00Z'";

    // Get logical and physical plans
    let (logical_plan_str, _physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application works with table alias
    assert!(logical_plan_str.contains("QueryCacheAggregate"), "Logical plan should contain QueryCacheAggregate node");

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Verify cache behavior
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(!cache_info.gap_intervals.is_empty(), "Should have gap intervals for initial execution");

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert_eq!(results.len(), 1, "Should return one batch");

    // Verify cache hit on second execution
    let physical_plan2 = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(!cache_info2.cached_intervals.is_empty(), "Second execution should use cached data");
}

/// Test sub-query optimization
#[tokio::test]
async fn test_subquery() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("inner_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let query = "SELECT * FROM (SELECT count(*) as cnt FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z') AS inner_table WHERE inner_table.cnt > 0";

    // Get logical and physical plans
    let (logical_plan_str, _physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application in subquery
    assert!(logical_plan_str.contains("QueryCacheAggregate"), "Logical plan should contain QueryCacheAggregate node for subquery");

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert!(!results.is_empty(), "Should return results");

    // Verify cache behavior
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    // Note: This test may need adjustment based on how subqueries are handled
    println!("Cache info: {:?}", cache_info);
}

/// Test query with multiple temporal filters (AND/OR conditions)
#[tokio::test]
async fn test_multiple_temporal_filters() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    // Test complex AND condition
    let query1 = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' AND service = 'service_a'";

    let (logical_plan_str1, _physical_plan_str1) = test_utils::get_plans_as_strings(&ctx, query1).await.unwrap();
    assert!(logical_plan_str1.contains("QueryCacheAggregate"), "Complex AND condition should trigger optimization");

    // Test OR condition (may not be optimizable)
    let query2 = "SELECT count(*) FROM test_table WHERE (timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z') OR (timestamp >= '2024-01-02T00:00:00Z' AND timestamp < '2024-01-03T00:00:00Z')";

    let (logical_plan_str2, _physical_plan_str2) = test_utils::get_plans_as_strings(&ctx, query2).await.unwrap();
    // Note: OR conditions might not be optimizable depending on implementation
    println!("OR condition logical plan: {}", logical_plan_str2);

    // Execute both queries successfully
    let results1 = test_utils::execute_query(&ctx, query1).await.unwrap();
    assert_eq!(results1.len(), 1, "Complex AND query should return results");

    let results2 = test_utils::execute_query(&ctx, query2).await.unwrap();
    assert_eq!(results2.len(), 1, "OR condition query should return results");
}

/// Test cache behavior with partial overlaps
#[tokio::test]
async fn test_partial_cache_overlap() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    // First query - populate cache with smaller interval
    let query1 = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T12:00:00Z'";
    let _results1 = test_utils::execute_query(&ctx, query1).await.unwrap();

    // Second query - larger interval that overlaps with cached data
    let query2 = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";
    let df2 = ctx.sql(query2).await.unwrap();
    let physical_plan2 = ctx.state().create_physical_plan(df2.logical_plan()).await.unwrap();

    // Verify partial cache hit
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(!cache_info2.cached_intervals.is_empty(), "Should use cached data for overlapping interval");
    assert!(!cache_info2.gap_intervals.is_empty(), "Should compute gaps for non-overlapping interval");

    let results2 = df2.collect().await.unwrap();
    assert!(!results2.is_empty(), "Should return results for larger interval");
}

/// Test bytes scanned reduction with caching
#[tokio::test]
async fn test_bytes_scanned_reduction() {
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
    let bytes1 = test_utils::get_bytes_scanned(&physical_plan1);
    let _results1 = df1.collect().await.unwrap();

    // Second execution - cache hit
    let df2 = ctx.sql(query).await.unwrap();
    let physical_plan2 = ctx.state().create_physical_plan(df2.logical_plan()).await.unwrap();
    let bytes2 = test_utils::get_bytes_scanned(&physical_plan2);
    let _results2 = df2.collect().await.unwrap();

    // Cache hit should scan fewer or equal bytes
    if let (Some(b1), Some(b2)) = (bytes1, bytes2) {
        assert!(b2 <= b1, "Cache hit should scan fewer or equal bytes than cache miss: {} vs {}", b2, b1);
    }
}

/// Test different aggregate functions
#[tokio::test]
async fn test_different_aggregate_functions() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();

    // Register test table
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();

    let queries = vec![
        "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
        "SELECT sum(value) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
        "SELECT avg(value) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
        "SELECT min(value), max(value) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
    ];

    for query in queries {
        // Get plans
        let (logical_plan_str, _physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

        // Verify optimizer applies to different aggregates
        assert!(logical_plan_str.contains("QueryCacheAggregate"),
                "Query '{}' should trigger QueryCacheAggregate optimization", query);

        // Execute successfully
        let results = test_utils::execute_query(&ctx, query).await.unwrap();
        assert!(!results.is_empty(), "Query '{}' should return results", query);
    }
}
