use chrono::{DateTime};
use datafusion::datasource::MemTable;
use std::sync::Arc;
use datafusion_query_cache::{MemoryQueryCache};

mod common;
use common::{create_time_series_data, IntervalDemoQuery, session_ctx, run_queries};

async fn run_interval_demo() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔥 INTERVAL-BASED QUERY CACHING DEMO 🔥");
    println!("=======================================");
    println!("This demo shows how interval caching enables efficient reuse of cached results");
    println!("across overlapping time ranges in time-series analytical queries.\n");

    // Create a larger dataset spanning 24 hours
    let start_time = DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap();
    let end_time = DateTime::parse_from_rfc3339("2024-01-02T00:00:00Z").unwrap();

    let data_batch = create_time_series_data(start_time, end_time);
    println!("📊 Created time-series dataset: {} records spanning 24 hours", data_batch.num_rows());

    let examples = vec![
        /*IntervalDemoQuery::new(
            "narrow_hour_window",
            "SELECT count(*) as requests FROM events WHERE timestamp >= '2024-01-01T02:00:00Z' AND timestamp < '2024-01-01T03:00:00Z'",
            "Count requests in a 1-hour window (2:00-3:00)",
            "First query - populates cache with [2:00, 3:00) interval",
        ),
        IntervalDemoQuery::new(
            "overlapping_hour_window",
            "SELECT count(*) as requests FROM events WHERE timestamp >= '2024-01-01T01:00:00Z' AND timestamp < '2024-01-01T04:00:00Z'",
            "Count requests in a wider 3-hour window (1:00-4:00)",
            "Should reuse cached [2:00, 3:00) and compute gaps [1:00, 2:00) and [3:00, 4:00)",
        ),*/
        IntervalDemoQuery::new(
            "service_breakdown",
            "SELECT service, count(*) as requests, round(avg(response_time), 2) as avg_response_time, min(response_time) as min_response, max(response_time) as max_response FROM events WHERE timestamp >= '2024-01-01T01:30:00Z' AND timestamp < '2024-01-01T02:30:00Z' GROUP BY service ORDER BY requests DESC",
            "Service breakdown with aggregates in 1-hour window (1:30-2:30)",
            "Complex GROUP BY query - demonstrates caching works with multiple aggregation functions",
        ),
        IntervalDemoQuery::new(
            "wider_service_analysis",
            "SELECT service, count(*) as requests, round(avg(response_time), 2) as avg_response_time, min(response_time) as min_response, max(response_time) as max_response FROM events WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T06:00:00Z' GROUP BY service ORDER BY requests DESC",
            "Service analysis across 6-hour window (0:00-6:00)",
            "Wide time range - can reuse cached fragments from previous queries",
        ),
        /*IntervalDemoQuery::new(
            "hourly_traffic_pattern",
            "SELECT date_trunc('hour', timestamp) as hour, count(*) as requests_per_hour FROM events WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-01T12:00:00Z' GROUP BY hour ORDER BY hour",
            "Hourly traffic patterns for first 12 hours",
            "Time bucketing with GROUP BY - shows caching works with date_trunc functions",
        ),
        IntervalDemoQuery::new(
            "performance_monitoring",
            "SELECT date_trunc('hour', timestamp) as hour, round(avg(response_time), 2) as avg_response, round(approx_percentile_cont(0.95, 2500) WITHIN GROUP (ORDER BY response_time), 2) as p95_response FROM events WHERE timestamp >= '2024-01-01T06:00:00Z' AND timestamp < '2024-01-01T18:00:00Z' GROUP BY hour ORDER BY hour",
            "Performance monitoring with P95 response times (6:00-18:00)",
            "Advanced analytics - percentile functions work with interval caching",
        ),
        IntervalDemoQuery::new(
            "full_day_summary",
            "SELECT count(*) as total_requests, round(avg(response_time), 2) as avg_response_time, count(distinct service) as unique_services FROM events WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'",
            "Full day summary statistics (24 hours)",
            "Maximum reuse - should leverage all cached intervals from previous queries",
        ),*/
    ];

    let cache = Arc::new(MemoryQueryCache::default());
    let ctx = session_ctx(cache.clone()).await;
    let table = MemTable::try_new(data_batch.schema(), vec![vec![data_batch.clone()]]).unwrap();
    ctx.register_table("events", Arc::new(table)).unwrap();

    run_queries(&ctx, &cache, examples).await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run_interval_demo().await {
        eprintln!("❌ Error: {}", e);
        std::process::exit(1);
    }
}
