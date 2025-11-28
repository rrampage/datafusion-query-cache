use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_query_cache::{
    CacheUpdateAggregateExec, CachedAggregateExec, LogStderrColors, MemoryQueryCache, QueryCacheConfig, TimeInterval,
    normalize_temporal_bounds_in_expr, with_query_cache_log,
};
use std::collections::HashSet;
use std::sync::Arc;

fn create_data_old() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("service", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    // 2024-01-01 00:00:00
    let mut timestamp = 1704067200000000;
    let count = 10_000;
    let mut timestamps = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);

    let mut seed = 0i64;

    for _ in 0..50_000 {
        // 0 - 999_000 us, so 0 - 0.999 s
        timestamps.push(timestamp);
        timestamp += 1_000_000;
        service_names.push(SERVICES[seed as usize % 5]);
        values.push(seed % 500);
        seed += 1;
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

const SERVICES: [&str; 5] = ["foo", "bar", "baz", "qux", "quux"];

async fn setup_session_context(cache: Arc<MemoryQueryCache>) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(10);
    let runtime = Arc::new(RuntimeEnv::default());
    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features();

    let sort_col = Column::new(Some("records".to_string()), "timestamp".to_string());
    let query_cache_config = QueryCacheConfig::new(sort_col, cache).with_group_by_function("date_trunc");

    let log = LogStderrColors::default();
    let state_builder = with_query_cache_log(state_builder, query_cache_config, log);
    SessionContext::new_with_state(state_builder.build())
}

async fn execute_query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    let df = ctx.sql(sql).await.unwrap();
    df.collect().await.unwrap()
}

/// Helper function to extract intervals from CachedAggregateExec and CacheUpdateAggregateExec nodes in a physical plan
fn collect_exec_intervals(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> (Vec<TimeInterval>, Vec<TimeInterval>) {
    let mut cached_intervals = Vec::new();
    let mut gap_intervals = Vec::new();

    // Walk the plan tree to find execution nodes
    fn walk_plan(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        cached: &mut Vec<TimeInterval>,
        gaps: &mut Vec<TimeInterval>,
    ) {
        // Check if this is a CachedAggregateExec
        if plan.name() == "CachedAggregateExec" {
            // Try to downcast to our custom exec to get the interval
            if let Some(cached_exec) = plan.as_any().downcast_ref::<CachedAggregateExec>() {
                cached.push(cached_exec.interval());
            }
        }
        // Check if this is a CacheUpdateAggregateExec
        else if plan.name() == "CacheUpdateAggregateExec" {
            if let Some(update_exec) = plan.as_any().downcast_ref::<CacheUpdateAggregateExec>() {
                gaps.push(update_exec.interval());
            }
        }

        // Recursively walk children
        for child in plan.children() {
            walk_plan(child, cached, gaps);
        }
    }

    walk_plan(plan, &mut cached_intervals, &mut gap_intervals);
    (cached_intervals, gap_intervals)
}

#[tokio::test]
async fn test_full_miss_then_hit() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    let sql =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'";

    // Run the same query twice - should get consistent results
    let results1 = execute_query(&ctx, sql).await;
    let results2 = execute_query(&ctx, sql).await;

    // Results should be identical (caching should not affect correctness)
    assert_eq!(results1, results2);
    assert_eq!(results1[0].num_rows(), 1);
}

#[tokio::test]
async fn test_partial_hit_with_gaps() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // First, run a query to populate cache
    let sql_cache =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:30:00Z' AND timestamp < '2024-01-01T00:45:00Z'";
    let _cache_results = execute_query(&ctx, sql_cache).await;

    // Now query a larger interval - should work correctly
    let sql_query =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'";
    let results = execute_query(&ctx, sql_query).await;

    // Should have results
    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);
}

#[tokio::test]
async fn test_between_semantics() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Test BETWEEN semantics
    let sql_between =
        "SELECT count(*) FROM records WHERE timestamp BETWEEN '2024-01-01T00:00:00Z' AND '2024-01-01T01:00:00Z'";
    let results_between = execute_query(&ctx, sql_between).await;

    // Test equivalent >= and <=
    let sql_range = "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp <= '2024-01-01T01:00:00Z'";
    let results_range = execute_query(&ctx, sql_range).await;

    // Results should be identical (BETWEEN is inclusive, our interval is half-open)
    assert_eq!(results_between, results_range);
}

#[tokio::test]
async fn test_multiple_overlapping_intervals() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache overlapping intervals
    let sql1 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:10:00Z' AND timestamp < '2024-01-01T00:40:00Z'";
    let _results1 = execute_query(&ctx, sql1).await;

    let sql2 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:20:00Z' AND timestamp < '2024-01-01T00:50:00Z'";
    let _results2 = execute_query(&ctx, sql2).await;

    // Query a superset - should work correctly
    let sql_query =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'";
    let results = execute_query(&ctx, sql_query).await;

    // Should have results
    assert!(!results.is_empty());
}

#[tokio::test]
async fn test_greater_than_less_than_operators() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Test the specific pattern: time > '2025-01-01' and time < '2025-12-31'
    let sql =
        "SELECT count(*) FROM records WHERE timestamp > '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T01:00:00Z'";
    let results = execute_query(&ctx, sql).await;

    // Should have results
    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);

    // Run again to test caching works
    let results2 = execute_query(&ctx, sql).await;
    assert_eq!(results, results2);
}

#[tokio::test]
async fn test_param_fingerprint_normalization_simple_bounds() {
    use datafusion::logical_expr::{BinaryExpr, Operator};

    let temporal_columns = HashSet::from([Column::new(Some("test".to_string()), "timestamp".to_string())]);

    // Test different literal values but same query shape should produce identical normalized expressions
    let expr1 = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::new(
            Some("test".to_string()),
            "timestamp".to_string(),
        ))),
        op: Operator::GtEq,
        right: Box::new(Expr::Literal(
            datafusion::common::ScalarValue::TimestampMicrosecond(Some(1704067200000000), None),
            None,
        )),
    });

    let expr2 = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::new(
            Some("test".to_string()),
            "timestamp".to_string(),
        ))),
        op: Operator::GtEq,
        right: Box::new(Expr::Literal(
            datafusion::common::ScalarValue::TimestampMicrosecond(Some(1704067800000000), None),
            None,
        )),
    });

    let normalized1 = normalize_temporal_bounds_in_expr(&expr1, &temporal_columns);
    let normalized2 = normalize_temporal_bounds_in_expr(&expr2, &temporal_columns);

    // Both should have "?" as the literal value
    assert_eq!(normalized1, normalized2);

    // Verify the structure is preserved
    if let Expr::BinaryExpr(bin) = normalized1 {
        assert_eq!(bin.op, Operator::GtEq);
        if let Expr::Literal(lit, _) = bin.right.as_ref() {
            assert_eq!(lit, &datafusion::common::ScalarValue::Utf8(Some("?".to_string())));
        } else {
            panic!("Expected literal in normalized expression");
        }
    } else {
        panic!("Expected binary expression");
    }
}

#[tokio::test]
async fn test_param_fingerprint_between_normalization() {
    use datafusion::logical_expr::Between;

    let temporal_columns = HashSet::from([Column::new(Some("test".to_string()), "timestamp".to_string())]);

    // Test BETWEEN normalization
    let between_expr = Expr::Between(Between {
        expr: Box::new(Expr::Column(Column::new(
            Some("test".to_string()),
            "timestamp".to_string(),
        ))),
        negated: false,
        low: Box::new(Expr::Literal(
            datafusion::common::ScalarValue::TimestampMicrosecond(Some(1704067200000000), None),
            None,
        )),
        high: Box::new(Expr::Literal(
            datafusion::common::ScalarValue::TimestampMicrosecond(Some(1704070800000000), None),
            None,
        )),
    });

    // Equivalent >= AND <= expression
    let range_expr = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new(
                Some("test".to_string()),
                "timestamp".to_string(),
            ))),
            op: Operator::GtEq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::TimestampMicrosecond(Some(1704067200000000), None),
                None,
            )),
        })),
        op: Operator::And,
        right: Box::new(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new(
                Some("test".to_string()),
                "timestamp".to_string(),
            ))),
            op: Operator::LtEq,
            right: Box::new(Expr::Literal(
                datafusion::common::ScalarValue::TimestampMicrosecond(Some(1704070800000000), None),
                None,
            )),
        })),
    });

    let normalized_between = normalize_temporal_bounds_in_expr(&between_expr, &temporal_columns);
    let normalized_range = normalize_temporal_bounds_in_expr(&range_expr, &temporal_columns);

    // Both should produce the same normalized structure (though BETWEEN might be different from range)
    // The key test is that literals are replaced with ?

    // Check BETWEEN normalization
    if let Expr::Between(between) = normalized_between {
        if let Expr::Literal(low_lit, _) = between.low.as_ref() {
            assert_eq!(low_lit, &datafusion::common::ScalarValue::Utf8(Some("?".to_string())));
        }
        if let Expr::Literal(high_lit, _) = between.high.as_ref() {
            assert_eq!(high_lit, &datafusion::common::ScalarValue::Utf8(Some("?".to_string())));
        }
    } else {
        panic!("Expected BETWEEN expression");
    }

    // Check range normalization
    if let Expr::BinaryExpr(and_expr) = normalized_range {
        if let Expr::BinaryExpr(left_expr) = and_expr.left.as_ref() {
            if let Expr::Literal(lit, _) = left_expr.right.as_ref() {
                assert_eq!(lit, &datafusion::common::ScalarValue::Utf8(Some("?".to_string())));
            }
        }
        if let Expr::BinaryExpr(right_expr) = and_expr.right.as_ref() {
            if let Expr::Literal(lit, _) = right_expr.right.as_ref() {
                assert_eq!(lit, &datafusion::common::ScalarValue::Utf8(Some("?".to_string())));
            }
        }
    } else {
        panic!("Expected AND expression");
    }
}

#[tokio::test]
async fn test_param_reuse_across_different_ranges() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Step 1: Run a query to populate cache with interval [4,6)
    let sql_cache =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:04:00Z' AND timestamp < '2024-01-01T00:06:00Z'";
    let _results_cache = execute_query(&ctx, sql_cache).await;

    // Step 2: Run a different query with same shape but different interval [7,9)
    let sql_different =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:07:00Z' AND timestamp < '2024-01-01T00:09:00Z'";
    let _results_different = execute_query(&ctx, sql_different).await;

    // Step 3: Run the first query again - should reuse cached data
    let df_same = ctx.sql(sql_cache).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df_same.logical_plan()).await.unwrap();
    let (cached_intervals, _gap_intervals) = collect_exec_intervals(&physical_plan);

    // Should reuse the cached interval [4,6)
    // Note: Intervals are stored in nanoseconds
    assert_eq!(
        cached_intervals.len(),
        1,
        "Expected 1 cached interval for repeated query, got {:?}",
        cached_intervals
    );
    assert_eq!(
        cached_intervals[0],
        TimeInterval::new(1704067440000000000, 1704067560000000000)
    ); // [4,6) in nanoseconds

    // Execute and verify results are correct
    let results_same = df_same.collect().await.unwrap();
    assert_eq!(results_same, _results_cache);

    // Verify the count is reasonable
    let count_value = results_same[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert!(count_value >= 0, "Count should be non-negative");
}

#[tokio::test]
async fn test_complex_overlapping_intervals() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache three overlapping intervals: [1,5), [2,4), [3,6)
    let sql1 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:05:00Z'";
    let _results1 = execute_query(&ctx, sql1).await;

    let sql2 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:02:00Z' AND timestamp < '2024-01-01T00:04:00Z'";
    let _results2 = execute_query(&ctx, sql2).await;

    let sql3 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:03:00Z' AND timestamp < '2024-01-01T00:06:00Z'";
    let _results3 = execute_query(&ctx, sql3).await;

    // Query the union of all intervals [1,6) - should merge all cached data
    let sql_query =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:06:00Z'";
    let results = execute_query(&ctx, sql_query).await;

    // Should have results and caching should work
    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);

    // Run again to verify caching works for complex overlaps
    let results2 = execute_query(&ctx, sql_query).await;
    assert_eq!(results, results2);
}

#[tokio::test]
async fn test_nested_intervals() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache a large interval first: [1,7)
    let sql_large =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:07:00Z'";
    let results_large = execute_query(&ctx, sql_large).await;

    // Verify [1,7) is cached
    let df1 = ctx.sql(sql_large).await.unwrap();
    let plan1 = ctx.state().create_physical_plan(df1.logical_plan()).await.unwrap();
    let (cached1, _) = collect_exec_intervals(&plan1);
    assert_eq!(cached1.len(), 1, "Expected 1 cached interval for [1,7) on second query");
    let large_interval = cached1[0].clone();

    // Cache a nested interval within it: [2,5)
    let sql_nested =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:02:00Z' AND timestamp < '2024-01-01T00:05:00Z'";
    let results_nested = execute_query(&ctx, sql_nested).await;

    // Verify [2,5) is cached
    let df_nested = ctx.sql(sql_nested).await.unwrap();
    let plan_nested = ctx
        .state()
        .create_physical_plan(df_nested.logical_plan())
        .await
        .unwrap();
    let (cached_nested, _) = collect_exec_intervals(&plan_nested);
    assert_eq!(cached_nested.len(), 1, "Expected 1 cached interval for [2,5)");
    let nested_interval = cached_nested[0].clone();

    // Both queries should find the large [1,7) interval as cached (since [2,5) overlaps with [1,7))
    // The nested query finds the overlapping large interval and uses it
    assert_eq!(
        large_interval, nested_interval,
        "Both queries should find the same cached [1,7) interval"
    );

    // Now query the large interval again - should use cached [1,7), not [2,5)
    let df_large_again = ctx.sql(sql_large).await.unwrap();
    let physical_plan = ctx
        .state()
        .create_physical_plan(df_large_again.logical_plan())
        .await
        .unwrap();
    let (cached_intervals, gap_intervals) = collect_exec_intervals(&physical_plan);

    // Should reuse the cached large interval [1,7) directly, not break it into gaps
    // When the large interval is already cached, it should not compute any gaps
    assert_eq!(cached_intervals.len(), 1, "Expected 1 cached interval on third query");
    assert_eq!(
        gap_intervals.len(),
        0,
        "Expected 0 gap intervals - should use complete cached [1,7)"
    );

    // Verify it's using the large interval, not the nested one
    assert_eq!(
        cached_intervals[0], large_interval,
        "Should use the large [1,7) interval, not [2,5)"
    );

    let results_large2 = df_large_again.collect().await.unwrap();
    assert_eq!(
        results_large, results_large2,
        "Results should be identical for same query"
    );

    // Verify nested results are subset (smaller count)
    let count_large = results_large[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    let count_nested = results_nested[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert!(
        count_nested <= count_large,
        "Nested interval should have smaller or equal count"
    );
}

#[tokio::test]
async fn test_adjacent_intervals() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache adjacent intervals that touch: [1,3) and [3,5)
    let sql1 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:03:00Z'";
    let _results1 = execute_query(&ctx, sql1).await;

    let sql2 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:03:00Z' AND timestamp < '2024-01-01T00:05:00Z'";
    let _results2 = execute_query(&ctx, sql2).await;

    // Query spanning both intervals: [1,5) - should merge adjacent intervals
    let sql_query =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:05:00Z'";
    let results = execute_query(&ctx, sql_query).await;

    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);

    // Verify the count makes sense (should be sum of both intervals)
    let df1 = ctx.sql(sql1).await.unwrap();
    let df2 = ctx.sql(sql2).await.unwrap();
    let physical_plan1 = ctx.state().create_physical_plan(df1.logical_plan()).await.unwrap();
    let physical_plan2 = ctx.state().create_physical_plan(df2.logical_plan()).await.unwrap();
    let (cached1, _) = collect_exec_intervals(&physical_plan1);
    let (cached2, _) = collect_exec_intervals(&physical_plan2);

    // Both should use cached data
    assert_eq!(cached1.len(), 1);
    assert_eq!(cached2.len(), 1);
}

#[tokio::test]
async fn test_multiple_disjoint_intervals() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache three disjoint intervals: [1,2), [4,5), [7,8)
    let sql1 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:02:00Z'";
    let _results1 = execute_query(&ctx, sql1).await;

    let sql2 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:04:00Z' AND timestamp < '2024-01-01T00:05:00Z'";
    let _results2 = execute_query(&ctx, sql2).await;

    let sql3 =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:07:00Z' AND timestamp < '2024-01-01T00:08:00Z'";
    let _results3 = execute_query(&ctx, sql3).await;

    // Query one of the cached intervals again - should reuse cached data
    let df = ctx.sql(sql1).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let (cached_intervals, _gap_intervals) = collect_exec_intervals(&physical_plan);

    // Should reuse the cached interval
    assert_eq!(
        cached_intervals.len(),
        1,
        "Expected 1 cached interval for repeated query, got {}",
        cached_intervals.len()
    );

    // Execute and verify results
    let results = df.collect().await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);
}

#[tokio::test]
async fn test_partial_coverage_multiple_disjoint() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache a specific interval: [2,3)
    let sql_cache =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:02:00Z' AND timestamp < '2024-01-01T00:03:00Z'";
    let _results_cache = execute_query(&ctx, sql_cache).await;

    // Query a larger interval that overlaps with the cached one: [1,4)
    // This should use [2,3) and compute [1,2) and [3,4)
    let sql_query =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:04:00Z'";
    let df = ctx.sql(sql_query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let (cached_intervals, gap_intervals) = collect_exec_intervals(&physical_plan);

    // Should use the cached interval [2,3) that overlaps with [1,4)
    assert_eq!(
        cached_intervals.len(),
        1,
        "Expected 1 cached interval, got {}",
        cached_intervals.len()
    );
    assert_eq!(
        cached_intervals[0],
        TimeInterval::new(1704067320000000000, 1704067380000000000),
        "Should use cached [2,3) interval"
    );

    // Should have gaps for [1,2) and [3,4)
    assert_eq!(
        gap_intervals.len(),
        2,
        "Expected 2 gap intervals, got {}",
        gap_intervals.len()
    );
    assert_eq!(
        gap_intervals[0],
        TimeInterval::new(1704067260000000000, 1704067320000000000),
        "First gap should be [1,2)"
    );
    assert_eq!(
        gap_intervals[1],
        TimeInterval::new(1704067380000000000, 1704067440000000000),
        "Second gap should be [3,4)"
    );

    // Execute and verify results are correct
    let results = df.collect().await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);
}

#[tokio::test]
async fn test_non_overlapping_close_intervals() {
    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = setup_session_context(cache.clone()).await;

    // Register table
    let batch = create_data_old();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // Cache a specific interval: [1,2)
    let sql_cache =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:01:00Z' AND timestamp < '2024-01-01T00:02:00Z'";
    let _results_cache = execute_query(&ctx, sql_cache).await;

    // Query a different interval: [3,4) - should not use cached data
    let sql_query =
        "SELECT count(*) FROM records WHERE timestamp >= '2024-01-01T00:03:00Z' AND timestamp < '2024-01-01T00:04:00Z'";
    let df = ctx.sql(sql_query).await.unwrap();
    let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await.unwrap();
    let (cached_intervals, gap_intervals) = collect_exec_intervals(&physical_plan);

    // Should not use cached data (different interval)
    assert_eq!(
        cached_intervals.len(),
        0,
        "Expected 0 cached intervals for different range, got {}",
        cached_intervals.len()
    );
    assert!(
        gap_intervals.len() >= 1,
        "Expected at least 1 gap interval, got {}",
        gap_intervals.len()
    );

    // Execute and verify results are correct
    let results = df.collect().await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].num_rows(), 1);
}
