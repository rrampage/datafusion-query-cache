use datafusion::prelude::SessionContext;
use datafusion_query_cache::MemoryQueryCache;
use std::sync::Arc;

mod test_utils;

fn build_cached_context_with_temporal(table: &str, column: &str) -> SessionContext {
    let cache = Arc::new(MemoryQueryCache::default());
    test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column(table, column)
        .build_cached()
}

fn build_cached_context() -> SessionContext {
    build_cached_context_with_temporal("test_table", "timestamp")
}

fn register_test_table(ctx: &SessionContext) {
    let table = test_utils::create_test_table("test_schema", "test_table");
    ctx.register_table("test_table", Arc::new(table)).unwrap();
}

fn build_vanilla_context() -> SessionContext {
    test_utils::TestContextBuilder::new().build_vanilla()
}

/// Test simple aggregate query optimization
#[tokio::test]
async fn test_simple_aggregate_query() {
    let ctx = build_cached_context();
    register_test_table(&ctx);

    let query = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";

    // Get logical and physical plans
    let (_logical_plan_str, physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application - physical plan should have cache-related nodes
    assert!(
        physical_plan_str.contains("CacheUpdateAggregateExec"),
        "Physical plan should contain CacheUpdateAggregateExec node on first execution: {}",
        physical_plan_str
    );

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Verify physical plan structure - should have cache-related execution nodes
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(
        !cache_info.gap_intervals.is_empty(),
        "Should have gap intervals for cache miss on first execution"
    );

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert_eq!(results.len(), 1, "Should return one batch");
    assert_eq!(results[0].num_rows(), 1, "Should return one row with count");

    // Verify cache behavior - second execution should hit cache
    let physical_plan2 = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(
        !cache_info2.cached_intervals.is_empty(),
        "Second execution should have cached intervals"
    );

    // Results should be identical
    let results2 = test_utils::execute_query(&ctx, query).await.unwrap();
    assert_eq!(results, results2, "Results should be identical between executions");
}

/// Test aggregate query with GROUP BY optimization
#[tokio::test]
async fn test_aggregate_with_group_by() {
    let ctx = build_cached_context();
    register_test_table(&ctx);

    let query = "SELECT date_trunc('hour', timestamp), count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' GROUP BY 1";

    // Get logical and physical plans
    let (_logical_plan_str, physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application - physical plan should have cache-related nodes
    assert!(
        physical_plan_str.contains("CacheUpdateAggregateExec"),
        "Physical plan should contain CacheUpdateAggregateExec node"
    );

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Verify cache behavior
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(
        !cache_info.gap_intervals.is_empty(),
        "Should have gap intervals for initial execution"
    );

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert!(!results.is_empty(), "Should return results");

    // Verify cache hit on second execution
    let physical_plan2 = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(
        !cache_info2.cached_intervals.is_empty(),
        "Second execution should use cached data"
    );
}

/// Test query with aliased table
#[tokio::test]
async fn test_query_with_aliased_table() {
    // Note: Even with table alias in query, DataFusion resolves columns to original table name
    // So we configure temporal column with the original table name
    let ctx = build_cached_context();
    register_test_table(&ctx);

    let query = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-02T00:00:00Z'";

    // Get logical and physical plans
    let (_logical_plan_str, physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application works with table alias
    // Note: Table alias handling may require special temporal column configuration
    println!("Physical plan for aliased table: {}", physical_plan_str);
    assert!(
        physical_plan_str.contains("CacheUpdateAggregateExec"),
        "Physical plan should contain CacheUpdateAggregateExec node: {}", physical_plan_str
    );

    // Create physical plan for analysis
    let df = ctx.sql(query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    // Verify cache behavior
    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(
        !cache_info.gap_intervals.is_empty(),
        "Should have gap intervals for initial execution"
    );

    // Execute query
    let results = test_utils::execute_query(&ctx, query).await.unwrap();
    assert_eq!(results.len(), 1, "Should return one batch");

    // Verify cache hit on second execution
    let physical_plan2 = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(
        !cache_info2.cached_intervals.is_empty(),
        "Second execution should use cached data"
    );
}

/// Test sub-query optimization
#[tokio::test]
async fn test_subquery() {
    let ctx = build_cached_context_with_temporal("inner_table", "timestamp");
    register_test_table(&ctx);

    let query = "SELECT * FROM (SELECT count(*) as cnt FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z') AS inner_table WHERE inner_table.cnt > 0";

    // Get logical and physical plans
    let (_logical_plan_str, physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

    // Verify optimizer rule application in subquery - may or may not apply depending on subquery handling
    // Check physical plan for cache nodes if optimization is applied
    println!("Physical plan for subquery: {}", physical_plan_str);

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
    let ctx = build_cached_context();
    register_test_table(&ctx);

    // Test complex AND condition
    let query1 = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z' AND service = 'service_a'";

    let (_logical_plan_str1, physical_plan_str1) = test_utils::get_plans_as_strings(&ctx, query1).await.unwrap();
    assert!(
        physical_plan_str1.contains("CacheUpdateAggregateExec"),
        "Complex AND condition should trigger optimization: {}",
        physical_plan_str1
    );

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
    let ctx = build_cached_context();
    register_test_table(&ctx);

    // First query - populate cache with smaller interval
    let query1 = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T12:00:00Z'";
    let _results1 = test_utils::execute_query(&ctx, query1).await.unwrap();

    // Second query - larger interval that overlaps with cached data
    let query2 = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";
    let df2 = ctx.sql(query2).await.unwrap();
    let physical_plan2 = ctx.state().create_physical_plan(df2.logical_plan()).await.unwrap();

    // Verify partial cache hit
    let cache_info2 = test_utils::collect_exec_intervals(&physical_plan2);
    assert!(
        !cache_info2.cached_intervals.is_empty(),
        "Should use cached data for overlapping interval"
    );
    assert!(
        !cache_info2.gap_intervals.is_empty(),
        "Should compute gaps for non-overlapping interval"
    );

    let results2 = df2.collect().await.unwrap();
    assert!(!results2.is_empty(), "Should return results for larger interval");
}

#[tokio::test]
async fn test_cached_vs_vanilla_alias_results_match() {
    let cached_ctx = build_cached_context();
    register_test_table(&cached_ctx);

    let vanilla_ctx = build_vanilla_context();
    register_test_table(&vanilla_ctx);

    let query = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-02T00:00:00Z'";

    let cached_results = test_utils::execute_query(&cached_ctx, query).await.unwrap();
    let vanilla_results = test_utils::execute_query(&vanilla_ctx, query).await.unwrap();

    assert_eq!(
        cached_results, vanilla_results,
        "Cached and vanilla contexts must return identical results for aliased queries"
    );
}

#[tokio::test]
async fn test_partial_cache_overlap_with_alias() {
    let ctx = build_cached_context();
    register_test_table(&ctx);

    let query1 = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-01T12:00:00Z'";
    let _ = test_utils::execute_query(&ctx, query1).await.unwrap();

    let query2 = "SELECT count(*) FROM test_table AS t WHERE t.timestamp >= '2024-01-01T00:00:00Z' AND t.timestamp < '2024-01-02T00:00:00Z'";
    let df = ctx.sql(query2).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();

    let cache_info = test_utils::collect_exec_intervals(&physical_plan);
    assert!(
        !cache_info.cached_intervals.is_empty(),
        "Aliased partial overlap should use cached intervals"
    );
    assert!(
        !cache_info.gap_intervals.is_empty(),
        "Aliased partial overlap should compute gaps for uncovered ranges"
    );

    let results = df.collect().await.unwrap();
    assert!(!results.is_empty(), "Aliased partial overlap should return results");
}

#[tokio::test]
async fn test_non_temporal_filter_aggregate_no_cache() {
    let cached_ctx = build_cached_context();
    register_test_table(&cached_ctx);

    let vanilla_ctx = build_vanilla_context();
    register_test_table(&vanilla_ctx);

    let query = "SELECT count(*) FROM test_table WHERE service = 'service_a'";

    let (_cached_logical, cached_physical) = test_utils::get_plans_as_strings(&cached_ctx, query).await.unwrap();
    let (_vanilla_logical, vanilla_physical) = test_utils::get_plans_as_strings(&vanilla_ctx, query).await.unwrap();

    assert!(
        !cached_physical.contains("CacheUpdateAggregateExec"),
        "Optimizer should not inject cache nodes for non-temporal filters: {}",
        cached_physical
    );
    assert_eq!(
        cached_physical, vanilla_physical,
        "Cached and vanilla physical plans should match when caching is not applicable"
    );

    let cached_results = test_utils::execute_query(&cached_ctx, query).await.unwrap();
    let vanilla_results = test_utils::execute_query(&vanilla_ctx, query).await.unwrap();
    assert_eq!(cached_results, vanilla_results, "Results should match for non-temporal filter");
}

#[tokio::test]
async fn test_non_temporal_filter_alias_no_cache() {
    let cached_ctx = build_cached_context();
    register_test_table(&cached_ctx);

    let vanilla_ctx = build_vanilla_context();
    register_test_table(&vanilla_ctx);

    let query = "SELECT count(*) FROM test_table AS t WHERE t.service = 'service_a'";

    let (_cached_logical, cached_physical) = test_utils::get_plans_as_strings(&cached_ctx, query).await.unwrap();
    let (_vanilla_logical, vanilla_physical) = test_utils::get_plans_as_strings(&vanilla_ctx, query).await.unwrap();

    assert!(
        !cached_physical.contains("CacheUpdateAggregateExec"),
        "Optimizer should not inject cache nodes for aliased queries with non-temporal filters: {}",
        cached_physical
    );
    assert_eq!(
        cached_physical, vanilla_physical,
        "Cached and vanilla plans should be identical for aliased non-temporal filters"
    );

    let cached_results = test_utils::execute_query(&cached_ctx, query).await.unwrap();
    let vanilla_results = test_utils::execute_query(&vanilla_ctx, query).await.unwrap();
    assert_eq!(
        cached_results, vanilla_results,
        "Aliased queries with non-temporal filters must return the same result"
    );
}

/// Test bytes scanned reduction with caching
#[tokio::test]
async fn test_bytes_scanned_reduction() {
    let ctx = build_cached_context();
    register_test_table(&ctx);

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
        assert!(
            b2 <= b1,
            "Cache hit should scan fewer or equal bytes than cache miss: {} vs {}",
            b2,
            b1
        );
    }
}

/// Test different aggregate functions
#[tokio::test]
async fn test_different_aggregate_functions() {
    let ctx = build_cached_context();
    register_test_table(&ctx);

    let queries = vec![
        "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
        "SELECT sum(value) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
        "SELECT avg(value) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
        "SELECT min(value), max(value) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
    ];

    for query in queries {
        // Get plans
        let (_logical_plan_str, physical_plan_str) = test_utils::get_plans_as_strings(&ctx, query).await.unwrap();

        // Verify optimizer applies to different aggregates
        assert!(
            physical_plan_str.contains("CacheUpdateAggregateExec"),
            "Query '{}' should trigger CacheUpdateAggregateExec optimization",
            query
        );

        // Execute successfully
        let results = test_utils::execute_query(&ctx, query).await.unwrap();
        assert!(!results.is_empty(), "Query '{}' should return results", query);
    }
}
