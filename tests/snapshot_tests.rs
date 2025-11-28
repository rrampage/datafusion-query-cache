//! Snapshot tests for query cache logical and physical plans
//!
//! These tests capture the expected output of query plans at different stages.
//! They use inline snapshots following DataFusion's testing patterns.

use datafusion::physical_plan::ExecutionPlan;
use datafusion_query_cache::MemoryQueryCache;
use std::sync::Arc;

mod test_utils;

/// Snapshot test for simple aggregate query logical plan
#[test]
fn test_simple_aggregate_logical_plan_snapshot() -> test_utils::Result<()> {
    let sql = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";
    let plan = test_utils::test_sql(sql)?;

    // Note: Optimizer not yet implemented, so this shows the unoptimized plan
    insta::assert_snapshot!(
        format!("{plan}"),
        @r#"
Projection: count(Int64(1)) AS count(*)
  Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]
    Filter: test_table.timestamp >= Utf8("2024-01-01T00:00:00Z") AND test_table.timestamp < Utf8("2024-01-02T00:00:00Z")
      TableScan: test_table
"#
    );
    Ok(())
}

/// Snapshot test for simple aggregate query physical plan
#[test]
fn test_simple_aggregate_physical_plan_snapshot() -> test_utils::Result<()> {
    let sql = "SELECT count(*) FROM test_table WHERE timestamp >= '2024-01-01T00:00:00Z' AND timestamp < '2024-01-02T00:00:00Z'";
    let plan = test_utils::test_sql_physical(sql)?;

    // Note: Physical plan shows optimized execution with cache
    insta::assert_snapshot!(
        format_physical_plan_for_snapshot(&plan),
        @r#"
ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
  AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
    CacheUpdateAggregateExec { interval: TimeInterval { start_ns: 1704067200000000000, end_ns: 1704153600000000000 }, input: CoalescePartitionsExec }
      CoalescePartitionsExec
        AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          CoalesceBatchesExec: target_batch_size=8192
            FilterExec: timestamp@0 >= 1704067200000000 AND timestamp@0 < 1704153600000000 AND timestamp@0 >= 1704067200000000 AND timestamp@0 < 1704153600000000
              DataSourceExec: partitions=1, partition_sizes=[1]
"#
    );
    Ok(())
}

/// Format physical plan for snapshot (similar to test_utils::format_physical_plan but cleaned up)
fn format_physical_plan_for_snapshot(plan: &Arc<dyn ExecutionPlan>) -> String {
    use datafusion::physical_plan::displayable;
    format!("{}", displayable(plan.as_ref()).indent(true))
}

/*
 * Note: This is a simplified version of snapshot tests following DataFusion patterns.
 * Only basic logical and physical plan snapshots are included.
 * Additional snapshots can be added as the optimizer implementation progresses.
 */
