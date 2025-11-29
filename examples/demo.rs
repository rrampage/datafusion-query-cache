use chrono::{DateTime, FixedOffset};
use datafusion::arrow::array::{
    Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_query_cache::{LogStderrColors, MemoryQueryCache, QueryCacheConfig, with_query_cache_log};
use std::collections::HashMap;
use std::sync::Arc;

mod common;
use common::print_cache_contents;

struct QueryExample {
    name: &'static str,
    sql: &'static str,
    description: &'static str,
}

async fn run_query_comparison() -> Result<(), Box<dyn std::error::Error>> {
    let examples = vec![
        /*QueryExample {
            name: "simple_aggregate",
            sql: "SELECT round(avg(value), 2), count(*) FROM records WHERE value > 1",
            description: "Simple aggregation without GROUP BY",
        },
        QueryExample {
            name: "group_by_hour",
            sql: "SELECT date_trunc('hour', timestamp), round(avg(value), 2), count(*) FROM records WHERE value > 1 GROUP BY 1 ORDER BY 1 DESC",
            description: "Aggregation with GROUP BY using date_trunc",
        },
        QueryExample {
            name: "multiple_aggregates",
            sql: "SELECT round(avg(value), 2), count(*), min(value), max(value) FROM records WHERE value > 1",
            description: "Multiple aggregation functions",
        },
        QueryExample {
            name: "group_by_service",
            sql: "SELECT service, round(avg(value), 2), count(*) FROM records WHERE value > 1 GROUP BY service ORDER BY service",
            description: "GROUP BY on categorical column",
        },*/
        // 2024-01-01 19:00 -> 19 hours
        // 2024-01-02 00:00 -> 19 hours from the cache and 5 hours incremental fetch
        /*QueryExample {
            name: "time_series_hour",
            sql: "SELECT date_trunc('hour', timestamp) as bucket, count(*) as requests_per_hour FROM records where timestamp > '2024-01-01T00:00:00Z' GROUP BY bucket ORDER BY bucket DESC",
            description: "Time series aggregation: count requests per hour bucket",
        },*/
        QueryExample {
            name: "time_series_hour_aggregate",
            sql: "SELECT count(*) as requests_per_hour FROM records where timestamp >= '2024-01-01T00:00:00Z'",
            description: "Time series aggregation: count requests per hour bucket",
        },
        /*QueryExample {
            name: "time_series_per_service",
            sql: "SELECT date_trunc('minute', timestamp) as bucket, service, count(*) as requests FROM records GROUP BY bucket, service ORDER BY bucket DESC, service",
            description: "Time series per service: requests per minute bucket by service",
        },
        QueryExample {
            name: "time_series_avg_value",
            sql: "SELECT date_trunc('hour', timestamp) as bucket, round(avg(value), 2) as avg_value, min(value), max(value) FROM records GROUP BY bucket ORDER BY bucket DESC",
            description: "Time series metrics: average, min, max value per hour",
        },
        QueryExample {
            name: "time_series_filtered",
            sql: "SELECT date_trunc('hour', timestamp) as bucket, count(*) as high_value_requests FROM records WHERE value > 100 GROUP BY bucket ORDER BY bucket DESC",
            description: "Time series with filtering: count high-value requests per hour bucket",
        },*/
    ];

    let divide = DateTime::parse_from_rfc3339("2024-01-01T17:18:19Z").unwrap();
    let batch1 = create_data(DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(), divide);
    let batch2 = create_data(divide, DateTime::parse_from_rfc3339("2024-01-02T00:00:00Z").unwrap());

    let partitions = vec![vec![batch1.clone(), batch2]];
    let mut plan_comparisons = HashMap::new();

    for example in &examples {
        println!("\n{}", "=".repeat(100));
        println!("EXAMPLE: {}", example.name.to_uppercase());
        println!("{}", "=".repeat(100));
        println!("Description: {}", example.description);
        println!("SQL: {}", example.sql);

        // Run with caching
        println!("\n{}", "-".repeat(50));
        println!("WITH CACHING");
        println!("{}", "-".repeat(50));

        let cache = Arc::new(MemoryQueryCache::default());
        let ctx_cached = session_ctx(cache.clone(), divide.timestamp_nanos_opt()).await;
        let table = MemTable::try_new(batch1.schema(), vec![vec![batch1.clone()]]).unwrap();
        ctx_cached.register_table("records", Arc::new(table)).unwrap();

        let cached_plan =
            execute_query_and_get_plan(&ctx_cached, &cache, example.sql, "First Run (with caching)").await?;
        print_cache_contents(&cache).await;

        // Add more data and run again
        let ctx_cached2 = session_ctx(cache.clone(), None).await;
        let table = MemTable::try_new(batch1.schema(), partitions.clone()).unwrap();
        ctx_cached2.register_table("records", Arc::new(table)).unwrap();

        let cached_plan2 =
            execute_query_and_get_plan(&ctx_cached2, &cache, example.sql, "Second Run (cache hit expected)").await?;
        print_cache_contents(&cache).await;

        // Run without caching
        println!("\n{}", "-".repeat(50));
        println!("WITHOUT CACHING");
        println!("{}", "-".repeat(50));

        let ctx_no_cache = SessionContext::new();
        let table = MemTable::try_new(batch1.schema(), partitions.clone()).unwrap();
        ctx_no_cache.register_table("records", Arc::new(table)).unwrap();

        let no_cache_plan = execute_query_and_get_plan_no_cache(&ctx_no_cache, example.sql).await?;

        // Store plans for comparison
        plan_comparisons.insert(example.name, (cached_plan2, no_cache_plan));
    }

    // Print plan comparisons
    println!("\n{}", "=".repeat(100));
    println!("QUERY PLAN COMPARISONS");
    println!("{}", "=".repeat(100));

    for (name, (cached_plan, no_cache_plan)) in &plan_comparisons {
        println!("\n{}", "-".repeat(80));
        println!("EXAMPLE: {}", name.to_uppercase());
        println!("{}", "-".repeat(80));

        println!("\n--- WITH CACHING ---");
        println!("{}", cached_plan);

        println!("\n--- WITHOUT CACHING ---");
        println!("{}", no_cache_plan);

        println!("\n--- KEY DIFFERENCES ---");
        if cached_plan.contains("QueryCacheAggregate") {
            println!("• WITH CACHING: Uses QueryCacheAggregate extension node for caching");
        }
        if cached_plan.contains("CachedAggregateExec") {
            println!("• WITH CACHING: Uses CachedAggregateExec to read from cache");
        }
        if cached_plan.contains("CacheUpdateAggregateExec") {
            println!("• WITH CACHING: Uses CacheUpdateAggregateExec to update cache");
        }
        if cached_plan.contains("UnionExec") && cached_plan.contains("CachedAggregateExec") {
            println!("• WITH CACHING: Uses UnionExec to combine cached and new data");
        }
        if !cached_plan.contains("QueryCacheAggregate") {
            println!("• WITHOUT CACHING: Standard DataFusion execution plan");
        }
    }

    Ok(())
}

async fn execute_query_and_get_plan(
    ctx: &SessionContext,
    cache: &MemoryQueryCache,
    sql: &str,
    query_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    println!("\n--- {} ---", query_name.to_uppercase());

    // Print cache contents before query
    println!("\nBEFORE QUERY:");
    print_cache_contents(cache).await;

    // Get logical plan
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan();
    println!("\nLOGICAL PLAN (after optimization):");
    println!("{}", logical_plan.display_indent_schema());

    // Get physical plan
    let physical_plan = ctx.state().create_physical_plan(logical_plan).await?;
    let plan_str = format!("{}", displayable(physical_plan.as_ref()).indent(true));
    println!("\nPHYSICAL PLAN:");
    println!("{}", plan_str);

    // Execute query
    println!("\nEXECUTING QUERY...");
    let batches = df.collect().await?;
    println!("\nQUERY RESULTS:");
    print_batches(&batches)?;

    Ok(plan_str)
}

async fn execute_query_and_get_plan_no_cache(
    ctx: &SessionContext,
    sql: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // Get logical plan
    let df = ctx.sql(sql).await?;
    let logical_plan = df.logical_plan();
    println!("LOGICAL PLAN:");
    println!("{}", logical_plan.display_indent_schema());

    // Get physical plan
    let physical_plan = ctx.state().create_physical_plan(logical_plan).await?;
    let plan_str = format!("{}", displayable(physical_plan.as_ref()).indent(true));
    println!("PHYSICAL PLAN:");
    println!("{}", plan_str);

    // Execute query
    println!("\nEXECUTING QUERY...");
    let batches = df.collect().await?;
    println!("\nQUERY RESULTS:");
    print_batches(&batches)?;

    Ok(plan_str)
}

#[tokio::main]
async fn main() {
    if let Err(e) = run_query_comparison().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

async fn session_ctx(cache: Arc<MemoryQueryCache>, override_now: Option<i64>) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(10);
    let runtime = Arc::new(RuntimeEnv::default());
    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features();

    let sort_col = Column::new(Some("records".to_string()), "timestamp".to_string());
    let query_cache_config = QueryCacheConfig::new(sort_col, cache)
        .with_group_by_function("date_trunc")
        .with_override_now(override_now);

    let log = LogStderrColors::default();
    let state_builder = with_query_cache_log(state_builder, query_cache_config, log);
    SessionContext::new_with_state(state_builder.build())
}

fn create_data(start: DateTime<FixedOffset>, stop: DateTime<FixedOffset>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("service", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let mut timestamp = start.timestamp_nanos_opt().unwrap();
    let mut timestamps = Vec::new();
    let mut service_names = Vec::new();
    let mut values = Vec::new();

    let end = stop.timestamp_nanos_opt().unwrap();

    let mut seed = 0;
    loop {
        timestamps.push(timestamp);
        timestamp += 1_000_000_000;
        service_names.push(SERVICES[usize::try_from(seed).unwrap() % 5]);
        values.push(seed);
        if timestamp >= end {
            break;
        }
        seed += 1;
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

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
