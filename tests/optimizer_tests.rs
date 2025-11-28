use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use datafusion_query_cache::MemoryQueryCache;
use insta::assert_snapshot;
use std::sync::Arc;

mod test_utils;

const TEST_QUERY: &str = "SELECT count(*) FROM test_table \
    WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";
const SMALLER_QUERY: &str = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND \
    timestamp < '2024-01-01T12:00:00Z'";
const LARGER_QUERY: &str = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND \
    timestamp < '2024-01-02T00:00:00Z'";
const TEST_TABLE_CSV: &str = "tests/data/query_cache_test_table.csv";

async fn register_csv_table(ctx: &SessionContext) -> test_utils::Result<()> {
    let schema = Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("service", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]);

    ctx.register_csv(
        "test_table",
        TEST_TABLE_CSV,
        CsvReadOptions::new().schema(&schema).has_header(true),
    )
    .await
    .map_err(|e| {
        let err: Box<dyn std::error::Error> = Box::new(e);
        err
    })?;

    Ok(())
}

async fn build_contexts() -> test_utils::Result<(SessionContext, SessionContext)> {
    let cache = Arc::new(MemoryQueryCache::default());
    let cached_ctx = test_utils::TestContextBuilder::new()
        .with_cache(cache.clone())
        .with_temporal_column("test_table", "timestamp")
        .build_cached();
    let vanilla_ctx = test_utils::TestContextBuilder::new().build_vanilla();

    register_csv_table(&cached_ctx).await?;
    register_csv_table(&vanilla_ctx).await?;

    Ok((cached_ctx, vanilla_ctx))
}

fn pretty_batches(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

fn normalize_plan(plan: &str) -> String {
    test_utils::normalize_plan_for_comparison(plan)
}

#[tokio::test]
async fn test_count_star_cache_snapshots() -> test_utils::Result<()> {
    let (cached_ctx, vanilla_ctx) = build_contexts().await?;

    let vanilla_results = test_utils::execute_query(&vanilla_ctx, TEST_QUERY).await?;
    let vanilla_results_str = pretty_batches(&vanilla_results);
    assert_snapshot!("optimizer_tests__count_star_results", vanilla_results_str);

    let (logical_plan, physical_plan) = test_utils::get_plans_as_strings(&cached_ctx, TEST_QUERY).await?;
    assert_snapshot!(
        "optimizer_tests__count_star_logical_plan",
        normalize_plan(&logical_plan)
    );
    assert_snapshot!(
        "optimizer_tests__count_star_cache_miss_physical",
        normalize_plan(&physical_plan)
    );

    assert!(
        physical_plan.contains("CacheUpdateAggregateExec"),
        "Cache miss physical plan should populate the cache"
    );

    let cached_results = test_utils::execute_query(&cached_ctx, TEST_QUERY).await?;
    let cached_results_str = pretty_batches(&cached_results);
    assert_eq!(cached_results_str, vanilla_results_str);

    let (_, physical_plan_cached) = test_utils::get_plans_as_strings(&cached_ctx, TEST_QUERY).await?;
    assert_snapshot!(
        "optimizer_tests__count_star_cache_hit_physical",
        normalize_plan(&physical_plan_cached)
    );
    assert!(
        physical_plan_cached.contains("CachedAggregateExec"),
        "Cache hit physical plan should read cached data"
    );

    let cached_results_again = test_utils::execute_query(&cached_ctx, TEST_QUERY).await?;
    let cached_results_again_str = pretty_batches(&cached_results_again);
    assert_eq!(cached_results_again_str, vanilla_results_str);

    Ok(())
}

#[tokio::test]
async fn test_partial_cache_union_snapshot() -> test_utils::Result<()> {
    let (cached_ctx, vanilla_ctx) = build_contexts().await?;

    let vanilla_results = test_utils::execute_query(&vanilla_ctx, LARGER_QUERY).await?;
    let vanilla_results_str = pretty_batches(&vanilla_results);
    assert_snapshot!("optimizer_tests__partial_cache_results", vanilla_results_str);

    test_utils::execute_query(&cached_ctx, SMALLER_QUERY).await?;

    let (_, physical_plan_after_overlap) = test_utils::get_plans_as_strings(&cached_ctx, LARGER_QUERY).await?;
    assert_snapshot!(
        "optimizer_tests__partial_cache_physical",
        normalize_plan(&physical_plan_after_overlap)
    );

    assert!(
        physical_plan_after_overlap.contains("UnionExec"),
        "Partial cache plan should combine cached and gap computations"
    );
    assert!(
        physical_plan_after_overlap.contains("CachedAggregateExec"),
        "Partial cache plan should read cached aggregates"
    );
    assert!(
        physical_plan_after_overlap.contains("CacheUpdateAggregateExec"),
        "Partial cache plan should compute uncovered intervals"
    );

    let cached_results = test_utils::execute_query(&cached_ctx, LARGER_QUERY).await?;
    let cached_results_str = pretty_batches(&cached_results);
    assert_eq!(cached_results_str, vanilla_results_str);

    Ok(())
}
