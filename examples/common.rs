use chrono::{DateTime, FixedOffset};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion_query_cache::MemoryQueryCache;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::common::Column;
use datafusion_query_cache::with_query_cache_log;
use datafusion_query_cache::LogStderrColors;
use datafusion_query_cache::QueryCacheConfig;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::physical_plan::displayable;
use std::collections::HashMap;


pub struct IntervalDemoQuery {
    name: &'static str,
    sql: &'static str,
    description: &'static str,
    explanation: &'static str,
}

pub const SERVICES: [&str; 5] = ["api-gateway", "database", "cache", "external-api", "static-files"];

impl IntervalDemoQuery {
    pub fn new(name: &'static str, sql: &'static str, description: &'static str, explanation: &'static str) -> Self {
        Self { name, sql, description, explanation }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn sql(&self) -> &'static str {
        self.sql
    }

    pub fn description(&self) -> &'static str {
        self.description
    }

    pub fn explanation(&self) -> &'static str {
        self.explanation
    }
}

pub async fn print_cache_contents(cache: &MemoryQueryCache) {
    println!("\n=== CACHE CONTENTS ===");
    let cache_display = cache.display();
    if cache_display.trim().is_empty() || cache_display.contains("MemoryQueryCache:") && !cache_display.contains("timestamp:") {
        println!("Cache is empty");
    } else {
        println!("{}", cache_display);
    }
    println!("=====================\n");
}

pub fn create_time_series_data(start: DateTime<FixedOffset>, stop: DateTime<FixedOffset>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("service", DataType::Utf8, true),
        Field::new("response_time", DataType::Int64, true),
        Field::new("status_code", DataType::Int64, true),
    ]));

    let mut timestamp = start.timestamp();
    let mut timestamps = Vec::new();
    let mut services = Vec::new();
    let mut response_times = Vec::new();
    let mut status_codes = Vec::new();

    let end = stop.timestamp();

    let mut seed = 0u64;
    while timestamp < end {
        timestamps.push(timestamp);

        let hour_of_day = (timestamp / 1_000_000_000 / 3600) % 24;
        let service_idx = if hour_of_day < 6 {
            (seed % 3) as usize
        } else if hour_of_day < 18 {
            (seed % 5) as usize
        } else {
            ((seed % 4) + 1) as usize
        };
        services.push(SERVICES[service_idx]);

        let base_response = match service_idx {
            0 => 50,
            1 => 200,
            2 => 100,
            3 => 300,
            4 => 75,
            _ => 150,
        };

        let load_factor = if hour_of_day >= 9 && hour_of_day <= 17 { 1.5 } else { 1.0 };
        let response_time = (base_response as f64 * load_factor * (0.5 + (seed as f64 * 0.1).sin().abs())) as i64;
        response_times.push(response_time.max(10));

        let status = match seed % 100 {
            0..=85 => 200,
            86..=95 => 404,
            96..=98 => 500,
            _ => 429,
        };
        status_codes.push(status);

        timestamp += 1_000_000_000;
        seed += 1;
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(StringArray::from(services)),
            Arc::new(Int64Array::from(response_times)),
            Arc::new(Int64Array::from(status_codes)),
        ],
    )
    .unwrap()
}

pub async fn session_ctx(cache: Arc<MemoryQueryCache>) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(10);
    let runtime = Arc::new(RuntimeEnv::default());
    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features();

    let sort_col = Column::new(Some("events".to_string()), "timestamp".to_string());
    let query_cache_config = QueryCacheConfig::new(sort_col, cache)
        .with_group_by_function("date_trunc");

    let log = LogStderrColors::default();
    let state_builder = with_query_cache_log(state_builder, query_cache_config, log);
    SessionContext::new_with_state(state_builder.build())
}

pub async fn run_queries(ctx: &SessionContext, cache: &MemoryQueryCache, queries: Vec<IntervalDemoQuery>) -> Result<(), Box<dyn std::error::Error>> {
    let mut execution_times = HashMap::new();

    for (i, example) in queries.iter().enumerate() {
        println!("\n{} QUERY {}: {}", "=".repeat(20), i + 1, "=".repeat(20));
        println!("📝 {}", example.name().to_uppercase());
        println!("📊 {}", example.description());
        println!("💡 {}", example.explanation());
        println!("🔍 SQL: {}", example.sql());

        // Show cache state before query
        print_cache_contents(cache).await;

        // Execute query and measure performance
        let start_time = std::time::Instant::now();
        let df = ctx.sql(example.sql()).await?;
        let logical_plan = df.logical_plan().clone();
        let results = df.collect().await.unwrap();
        let execution_time = start_time.elapsed();

        execution_times.insert(example.name(), execution_time);

        println!("⚡ Execution time: {:.2}ms", execution_time.as_millis());
        println!("📋 Results:");
        print_batches(&results)?;

        // Show cache state after query
        print_cache_contents(cache).await;

        // Analyze physical plan for caching components
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
        let plan_str = format!("{}", displayable(physical_plan.as_ref()).indent(true));

        println!("🔧 Physical Plan Analysis:");
        if plan_str.contains("CachedAggregateExec") {
            println!("  ✅ Uses CachedAggregateExec - reading from cache");
        }
        if plan_str.contains("CacheUpdateAggregateExec") {
            println!("  ✅ Uses CacheUpdateAggregateExec - computing new results and caching");
        }
        if plan_str.contains("UnionExec") {
            println!("  ✅ Uses UnionExec - combining cached and new data");
        }
        if !plan_str.contains("CachedAggregateExec") && !plan_str.contains("CacheUpdateAggregateExec") {
            println!("  ⚠️  No caching components found - standard DataFusion execution");
        }
        println!("{}", plan_str);

        // Performance summary
        println!("\n{} PERFORMANCE SUMMARY {}", "=".repeat(25), "=".repeat(25));
        println!("🎯 Query execution times (lower is better, shows caching benefits):");
        for (name, duration) in &execution_times {
            println!("  {:.<30} {:.2}ms", name, duration.as_millis());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {}

