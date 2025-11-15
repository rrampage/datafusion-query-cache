use std::any::Any;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::DateTime;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{internal_err, plan_err, Column, DFSchemaRef, Result as DataFusionResult, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Expr, Extension, Filter, LogicalPlan, Operator, TableScan, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::expressions::{
    BinaryExpr as PhysicalBinaryExpr, Column as PhysicalColumn, Literal as PhysicalLiteral,
};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::TryFutureExt;

use crate::cache::{normalize_fingerprint_for_caching, OccupiedIntervalCacheEntry, QueryCache, TimeInterval};
use crate::log::{log_info, log_warn, AbstractLog};
use crate::QueryCacheConfig;

#[derive(Debug)]
pub(crate) struct QCAggregateOptimizerRule<Log: AbstractLog> {
    log: Log,
    config: Arc<QueryCacheConfig>,
}

impl<Log: AbstractLog> QCAggregateOptimizerRule<Log> {
    pub fn new(log: Log, config: Arc<QueryCacheConfig>) -> Self {
        Self { log, config }
    }

    /// Find a column used in a group by expression that matches one of our temporal columns
    fn find_temporal_group_by(&self, expr: &Expr) -> Option<Column> {
        let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
            return None;
        };
        if !self.config.allow_group_by_function(func.name()) {
            return None;
        }
        let second_arg = args.get(1)?;

        if let Expr::Column(column) = second_arg {
            if self.config.allow_temporal_column(column) {
                return Some(column.clone());
            }
        }

        None
    }
}
impl<Log: AbstractLog> OptimizerRule for QCAggregateOptimizerRule<Log> {
    fn name(&self) -> &str {
        "query-cache-agg-group-by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn rewrite(
        &self,
        mut plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        let mut fingerprint = plan.display_indent_schema().to_string();
        
        // Log the input plan for this optimizer rule
        log_info!(
            self.log,
            &fingerprint,
            "Optimizer rule '{}' examining plan:\n{}",
            self.name(),
            plan.display_indent_schema()
        );
        
        let LogicalPlan::Aggregate(agg) = &plan else {
            // not an aggregation, continue rewrite
            log_info!(
                self.log,
                &fingerprint,
                "Plan is not an Aggregate, skipping query cache optimization"
            );
            return Ok(Transformed::no(plan));
        };

        let Aggregate { input, group_expr, .. } = agg;
        let agg_input = input.as_ref().clone();
        let mut temporal_group_bys = group_expr.iter().filter_map(|e| self.find_temporal_group_by(e));

        let temporal_group_by = temporal_group_bys.next();

        if temporal_group_bys.next().is_some() {
            // I've no idea if this is even possible, and what we could do if it is, do nothing for now
            self.log.info(
                &fingerprint,
                "multiple group bys using temporal columns, caching not possible!",
            )?;
            return Ok(Transformed::no(plan));
        }

        println!("BBBB TEMPORALCOLUMN: {:?}", &self.config.default_temporal_column());

        let (interval, input, temporal_columns_for_param) = if let LogicalPlan::Filter(filter) = &agg_input {
            let needle_columns = if let Some(temporal_group_by) = &temporal_group_by {
                Cow::Owned(HashSet::from([temporal_group_by.clone()]))
            } else {
                Cow::Borrowed(&self.config.temporal_columns)
            };

            let interval = match StaticInterval::find(&filter.predicate, &needle_columns) {
                StaticInterval::Found(interval) => Some(interval),
                StaticInterval::Stable => None,
                _ => {
                    // we found an unstable expression, we can't rewrite the plan
                    self.log
                        .info(&fingerprint, "we found an unstable expression, caching not possible")?;
                    return Ok(Transformed::no(plan));
                }
            };
            (interval, filter.input.as_ref().clone(), needle_columns.into_owned())
        } else {
            (None, agg_input.clone(), HashSet::new())
        };

        // Create parameterized fingerprint for cache keying
        let param_fingerprint = if !temporal_columns_for_param.is_empty() {
            let normalized_plan = normalize_temporal_bounds_in_plan(&plan, &temporal_columns_for_param);
            let param_fingerprint_str = normalized_plan.display_indent_schema().to_string();
            println!("BBBB PRENORMALIZED: Param fingerprint: '{}'", param_fingerprint_str);
            // Normalize the fingerprint by removing schema-specific information from TableScan
            normalize_fingerprint_for_caching(&param_fingerprint_str)
        } else {
            // Even without temporal columns, normalize for consistency
            normalize_fingerprint_for_caching(&fingerprint)
        };
        println!("BBBB NORMALIZED: Param fingerprint: '{}'", param_fingerprint);

        if temporal_group_by.is_none() {
            // if temporal_group_by is none, we need to make sure the sort column is in the projection
            // Find the TableScan through Filter/Projection nodes
            let mut current_plan = input.clone();
            let scan = loop {
                match &current_plan {
                    LogicalPlan::TableScan(scan) => break Some(scan.clone()),
                    LogicalPlan::Filter(filter) => current_plan = filter.input.as_ref().clone(),
                    LogicalPlan::Projection(proj) => current_plan = proj.input.as_ref().clone(),
                    _ => break None,
                }
            };

            let Some(scan) = scan else {
                // TODO we need to support this, e.g. a subquery
                self.log
                    .info(&fingerprint, "input not a table scan (through filters/projections), caching not possible")?;
                return Ok(Transformed::no(plan));
            };

            // TODO check table name
            let field_name = self.config.default_temporal_column().name.clone();
            if !scan.projected_schema.fields().iter().any(|f| f.name() == &field_name) {
                let new_col_id = scan
                    .source
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .find_map(|(id, f)| (f.name() == &field_name).then_some(id));

                let Some(new_col_id) = new_col_id else {
                    log_info!(
                        self.log,
                        &fingerprint,
                        "sort column '{}' not found in table, caching not possible",
                        field_name
                    );
                    return Ok(Transformed::no(plan));
                };

                let mut new_projection = scan.projection.expect("no projection found");
                new_projection.push(new_col_id);
                new_projection.sort_unstable();

                let new_table_scan = TableScan::try_new(
                    scan.table_name,
                    scan.source,
                    Some(new_projection),
                    scan.filters,
                    scan.fetch,
                )
                .map(LogicalPlan::TableScan)?;

                let inner_plan = if let LogicalPlan::Filter(filter) = &agg_input {
                    LogicalPlan::Filter(Filter::try_new(filter.predicate.clone(), Arc::new(new_table_scan))?)
                } else {
                    new_table_scan
                };
                plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(inner_plan),
                    agg.group_expr.clone(),
                    agg.aggr_expr.clone(),
                )?);
                fingerprint = plan.display_indent_schema().to_string();
            }
        }

        // Static intervals are now supported

        let temporal_column = temporal_group_by.unwrap_or_else(|| self.config.default_temporal_column().clone());

        // do we need to check the input is a table scan?
        log_info!(
            self.log,
            &fingerprint,
            "query valid for caching, sort column {}",
            temporal_column
        );
        
        let transformed_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(QCAggregatePlanNode::new(
                plan.clone(),
                temporal_column,
                interval,
                Some(param_fingerprint.clone()),
            )?),
        });
        
        log_info!(
            self.log,
            &param_fingerprint,
            "Plan transformed with QueryCacheAggregate extension node:\n{}",
            transformed_plan.display_indent_schema()
        );
        
        Ok(Transformed::yes(transformed_plan))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct QCAggregatePlanNode {
    input: LogicalPlan,
    fingerprint: String,
    temporal_column: Column,
    interval: Option<TimeInterval>,
}

impl fmt::Display for QCAggregatePlanNode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

/// Placeholder for th cached aggregation in the logical plan, there's no logic here, just boilerplate
impl QCAggregatePlanNode {
    fn new(
        input: LogicalPlan,
        temporal_column: Column,
        interval: Option<TimeInterval>,
        fingerprint: Option<String>,
    ) -> DataFusionResult<Self> {
        if let LogicalPlan::Extension(e) = input {
            if let Some(node) = e.node.as_any().downcast_ref::<QCAggregatePlanNode>() {
                // already a `QCAggregatePlanNode`, return it
                Ok(node.clone())
            } else {
                plan_err!("unexpected extension node, expected QCAggregatePlanNode")
            }
        } else if matches!(input, LogicalPlan::Aggregate(..)) {
            let fingerprint = fingerprint.unwrap_or_else(|| input.display_indent_schema().to_string());
            Ok(Self {
                input,
                fingerprint,
                temporal_column,
                interval,
            })
        } else {
            plan_err!(
                "unexpected input to QCAggregatePlanNode, mut be Aggregate or Extension(QCAggregatePlanNode), got {}",
                input.display()
            )
        }
    }
}

impl UserDefinedLogicalNodeCore for QCAggregatePlanNode {
    fn name(&self) -> &str {
        "QueryCacheAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut expressions = vec![Expr::Column(self.temporal_column.clone())];
        if let Some(interval) = &self.interval {
            expressions.push(Expr::Literal(ScalarValue::Int64(Some(interval.start_ns)), None));
            expressions.push(Expr::Literal(ScalarValue::Int64(Some(interval.end_ns)), None));
        }
        expressions
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "QueryCacheAggregate: {}", self.input.display())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DataFusionResult<Self> {
        let mut iter_exprs = exprs.into_iter();
        let Some(Expr::Column(column)) = iter_exprs.next() else {
            return plan_err!("UserDefinedLogicalNodeCore  expected temporal column as first expressoin");
        };
        let interval = if let (Some(Expr::Literal(ScalarValue::Int64(Some(start_ns)), _)), Some(Expr::Literal(ScalarValue::Int64(Some(end_ns)), _))) = (iter_exprs.next(), iter_exprs.next()) {
            if iter_exprs.next().is_some() {
                return plan_err!("UserDefinedLogicalNodeCore expected one, two, or three expressions");
            }
            Some(TimeInterval::new(start_ns, end_ns))
        } else if iter_exprs.next().is_some() {
            return plan_err!("UserDefinedLogicalNodeCore expected either one column or column + two int64 literals");
        } else {
            None
        };

        let mut iter_inputs = inputs.into_iter();
        let Some(input) = iter_inputs.next() else {
            return plan_err!("UserDefinedLogicalNodeCore expected one inputs");
        };
        if iter_inputs.next().is_some() {
            plan_err!("UserDefinedLogicalNodeCore expected one inputs")
        } else {
            Self::new(input, column, interval, None)
        }
    }
}

/// A physical planner that knows how to convert a `QCAggregatePlanNode` into physical plan,
/// using `QCInnerAggregateExec`.
#[derive(Debug)]
pub(crate) struct QCAggregateExecPlanner<Log: AbstractLog> {
    log: Log,
    config: Arc<QueryCacheConfig>,
}

impl<Log: AbstractLog> QCAggregateExecPlanner<Log> {
    pub fn new(log: Log, config: Arc<QueryCacheConfig>) -> Self {
        Self { log, config }
    }
}

#[async_trait]
impl<Log: AbstractLog> ExtensionPlanner for QCAggregateExecPlanner<Log> {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(agg_node) = node.as_any().downcast_ref::<QCAggregatePlanNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            // maybe Ok(None) is ok here?
            return plan_err!("QueryCacheGroupByExec expected one input");
        }

        let exec = physical_inputs[0].clone();

        log_info!(
            self.log,
            &agg_node.fingerprint,
            "Physical planner processing QueryCacheAggregate extension node"
        );

        if find_existing_inner_exec(&exec) {
            // already a `QCInnerAggregateExec` (or contains a `QCInnerAggregateExec`), return it
            log_info!(
                self.log,
                &agg_node.fingerprint,
                "Physical plan already contains QueryCacheAggregateExec, reusing existing plan"
            );
            return Ok(Some(exec));
        }

        let Some(agg_exec): Option<&AggregateExec> = exec.as_any().downcast_ref() else {
            // should this be an error?
            log_warn!(
                self.log,
                &agg_node.fingerprint,
                "QueryCacheGroupByExec expected an AggregateExec input, found {}",
                exec.name()
            );
            return Ok(Some(exec));
        };

        log_info!(
            self.log,
            &agg_node.fingerprint,
            "Input physical plan structure:\n  - AggregateExec mode: {:?}\n  - Aggregate expressions: {}",
            agg_exec.mode(),
            agg_exec.aggr_expr().len()
        );

        // Try to extract interval from agg_node, or from the filter predicate if not available
        let requested_interval = if let Some(interval) = agg_node.interval {
            interval
        } else {
            // Try to extract interval from the input logical plan's filter predicate
            extract_interval_from_logical_plan(&agg_node.input, &agg_node.temporal_column)
                .unwrap_or_else(|| TimeInterval::new(i64::MIN, i64::MAX))
        };

        println!("CACHE_DEBUG: Looking up fingerprint: '{}'", &agg_node.fingerprint);
        let cached_intervals = self.config.cache().lookup(&agg_node.fingerprint, &requested_interval).await?;
        log_info!(
            self.log,
            &agg_node.fingerprint,
            "Cache lookup result: found {} cached intervals for requested interval [{}, {})",
            cached_intervals.len(),
            requested_interval.start_ns,
            requested_interval.end_ns
        );

        let now = self.config.override_now.unwrap_or_else(|| {
            session_state
                .execution_props()
                .query_execution_start_time
                .timestamp_nanos_opt()
                // we'll be in trouble after 2262!
                .unwrap()
        });

        let partial_agg_exec = agg_exec.input().clone();

        // Select maximal non-overlapping cached intervals by greedy algorithm
        let selected_cached = select_maximal_cached_intervals(&cached_intervals, &requested_interval);
        log_info!(
            self.log,
            &agg_node.fingerprint,
            "Selected {} non-overlapping cached intervals from {} candidates",
            selected_cached.len(),
            cached_intervals.len()
        );

        // Compute uncovered gaps
        let gaps = compute_gaps(&requested_interval, &selected_cached);
        log_info!(
            self.log,
            &agg_node.fingerprint,
            "Found {} uncovered gaps to compute",
            gaps.len()
        );

        // Build execution plans for cached data and gaps
        let mut input_plans = Vec::new();

        // Add cached data readers
        for cached_entry in &selected_cached {
            let cached_exec = CachedAggregateExec::new_exec_plan(cached_entry.clone(), partial_agg_exec.properties());
            input_plans.push(cached_exec);
        }

        // Add gap computation plans (wrapped in CacheUpdateAggregateExec)
        for gap in &gaps {
            let gap_exec = with_interval_bounds(&partial_agg_exec, &agg_node.temporal_column, gap)?;
            let cache_update_exec = CacheUpdateAggregateExec::new_exec_plan(
                self.config.cache().clone(),
                &agg_node.fingerprint,
                gap,
                gap_exec,
                now,
            );
            input_plans.push(cache_update_exec);
        }

        let input_exec = if input_plans.len() == 1 {
            input_plans.into_iter().next().unwrap()
        } else {
            let union_exec = Arc::new(UnionExec::new(input_plans));
            Arc::new(CoalescePartitionsExec::new(union_exec))
        };
        let input_schema = input_exec.schema();

        let final_plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            agg_exec.group_expr().clone(),
            agg_exec.aggr_expr().to_vec(),
            agg_exec.filter_expr().to_vec(),
            input_exec,
            input_schema,
        )?);

        log_info!(
            self.log,
            &agg_node.fingerprint,
            "Physical plan created: Final AggregateExec with CacheUpdateAggregateExec input"
        );

        Ok(Some(final_plan))
    }
}

/// Extract interval from a logical plan's filter predicate if available
fn extract_interval_from_logical_plan(plan: &LogicalPlan, temporal_column: &Column) -> Option<TimeInterval> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            // Look at the aggregate's input for a filter
            extract_interval_from_logical_plan(&agg.input, temporal_column)
        }
        LogicalPlan::Filter(filter) => {
            // Try to extract interval from the filter predicate
            let temporal_columns = HashSet::from([temporal_column.clone()]);
            match StaticInterval::find(&filter.predicate, &temporal_columns) {
                StaticInterval::Found(interval) => Some(interval),
                _ => None,
            }
        }
        LogicalPlan::Projection(proj) => {
            // Look through projections
            extract_interval_from_logical_plan(&proj.input, temporal_column)
        }
        _ => None,
    }
}

/// Select maximal non-overlapping cached intervals using greedy algorithm
fn select_maximal_cached_intervals(
    cached_entries: &[Arc<dyn OccupiedIntervalCacheEntry>],
    _requested: &TimeInterval,
) -> Vec<Arc<dyn OccupiedIntervalCacheEntry>> {
    // Extract intervals and sort by length descending
    let mut intervals_with_entries: Vec<(TimeInterval, Arc<dyn OccupiedIntervalCacheEntry>)> = cached_entries
        .iter()
        .map(|entry| (entry.interval(), entry.clone()))
        .collect();

    intervals_with_entries.sort_by(|a, b| b.0.length().cmp(&a.0.length()));

    let mut selected = Vec::new();
    let mut covered = TimeInterval::new(i64::MAX, i64::MIN); // Empty interval initially

    for (interval, entry) in intervals_with_entries {
        // Check if this interval overlaps with any already selected
        let overlaps_selected = selected.iter().any(|(_selected_interval, selected_entry): &(TimeInterval, Arc<dyn OccupiedIntervalCacheEntry>)| {
            selected_entry.interval().overlaps(&interval)
        });

        if !overlaps_selected {
            selected.push((interval, entry));
            // Update covered range
            covered.start_ns = covered.start_ns.min(interval.start_ns);
            covered.end_ns = covered.end_ns.max(interval.end_ns);
        }
    }

    selected.into_iter().map(|(_, entry)| entry).collect()
}

/// Compute gaps between requested interval and selected cached intervals
fn compute_gaps(
    requested: &TimeInterval,
    selected_cached: &[Arc<dyn OccupiedIntervalCacheEntry>],
) -> Vec<TimeInterval> {
    if selected_cached.is_empty() {
        return vec![*requested];
    }

    // Get all cached intervals sorted by start time
    let mut cached_intervals: Vec<TimeInterval> = selected_cached
        .iter()
        .map(|entry| entry.interval())
        .collect();
    cached_intervals.sort_by_key(|i| i.start_ns);

    let mut gaps = Vec::new();

    // Gap before first cached interval
    if cached_intervals[0].start_ns > requested.start_ns {
        gaps.push(TimeInterval::new(requested.start_ns, cached_intervals[0].start_ns));
    }

    // Gaps between cached intervals
    for i in 0..cached_intervals.len() - 1 {
        let current_end = cached_intervals[i].end_ns;
        let next_start = cached_intervals[i + 1].start_ns;
        if current_end < next_start {
            gaps.push(TimeInterval::new(current_end, next_start));
        }
    }

    // Gap after last cached interval
    if cached_intervals.last().unwrap().end_ns < requested.end_ns {
        gaps.push(TimeInterval::new(cached_intervals.last().unwrap().end_ns, requested.end_ns));
    }

    gaps
}

/// apply interval bounds to an `AggregateExec`
fn with_interval_bounds(
    partial_agg_exec: &Arc<dyn ExecutionPlan>,
    bound_column: &Column,
    interval: &TimeInterval,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    // For gap computation, we need to apply the interval bounds to the data source.
    // If partial_agg_exec is an AggregateExec, we apply the filter to its input.
    // If the input is a ProjectionExec with empty projection, we apply the filter to its input.
    let (plan_to_filter, column_offset, projection_exprs) =
        if let Some(agg_exec) = partial_agg_exec.as_any().downcast_ref::<AggregateExec>() {
            // Find where to apply the filter
            let input = agg_exec.input();
            if let Some(proj_exec) = input.as_any().downcast_ref::<ProjectionExec>() {
                // If the input is a projection with empty projection, apply filter to its input
                if proj_exec.expr().is_empty() {
                    (proj_exec.input().clone(), 0, None)
                } else {
                    // Projection has expressions; apply filter before the projection
                    (proj_exec.input().clone(), 0, Some(proj_exec.expr().to_vec()))
                }
            } else {
                // Apply filter to the aggregate input
                (input.clone(), 0, None)
            }
        } else {
            // Not an AggregateExec, apply filter directly
            (partial_agg_exec.clone(), 0, None)
        };

    // Find the column in the plan_to_filter
    let find_column_result = find_timestamp_column_in_plan(&plan_to_filter, bound_column);

    let (column_id, time_unit, time_zone) = match find_column_result {
        Some(result) => result,
        None => return plan_err!("Timestamp column '{}' not found in plan", bound_column.name),
    };

    let scalar_time_zone = time_zone.clone();

    // Convert nanosecond bounds to appropriate time unit
    println!("BBBB TIME_UNIT: {:?}", time_unit);
    let (lower_bound, upper_bound) = match time_unit {
        TimeUnit::Nanosecond => (
            ScalarValue::TimestampNanosecond(Some(interval.start_ns), scalar_time_zone.clone()),
            ScalarValue::TimestampNanosecond(Some(interval.end_ns), scalar_time_zone.clone()),
        ),
        TimeUnit::Microsecond => (
            ScalarValue::TimestampMicrosecond(
                Some(interval.start_ns / 1000),
                scalar_time_zone.clone(),
            ),
            ScalarValue::TimestampMicrosecond(
                Some(interval.end_ns / 1000),
                scalar_time_zone.clone(),
            ),
        ),
        TimeUnit::Millisecond => (
            ScalarValue::TimestampMillisecond(
                Some(interval.start_ns / 1_000_000),
                scalar_time_zone.clone(),
            ),
            ScalarValue::TimestampMillisecond(
                Some(interval.end_ns / 1_000_000),
                scalar_time_zone.clone(),
            ),
        ),
        TimeUnit::Second => (
            ScalarValue::TimestampSecond(
                Some(interval.start_ns / 1_000_000_000),
                scalar_time_zone.clone(),
            ),
            ScalarValue::TimestampSecond(
                Some(interval.end_ns / 1_000_000_000),
                scalar_time_zone.clone(),
            ),
        ),
    };

    let lower_bound_predicate = Arc::new(PhysicalBinaryExpr::new(
        Arc::new(PhysicalColumn::new(&bound_column.name, column_id + column_offset)),
        Operator::GtEq,
        Arc::new(PhysicalLiteral::new(lower_bound)),
    ));

    let upper_bound_predicate = Arc::new(PhysicalBinaryExpr::new(
        Arc::new(PhysicalColumn::new(&bound_column.name, column_id + column_offset)),
        Operator::Lt,
        Arc::new(PhysicalLiteral::new(upper_bound)),
    ));

    let interval_predicate = Arc::new(PhysicalBinaryExpr::new(
        lower_bound_predicate,
        Operator::And,
        upper_bound_predicate,
    ));

    // Create new filter
    let new_filter_exec = if let Some(filter) = plan_to_filter.as_any().downcast_ref::<FilterExec>() {
        let new_predicate = PhysicalBinaryExpr::new(filter.predicate().clone(), Operator::And, interval_predicate);
        let new_filter = FilterExec::try_new(Arc::new(new_predicate), filter.input().clone())?;
        new_filter.with_projection(filter.projection().cloned())?
    } else {
        FilterExec::try_new(interval_predicate, plan_to_filter)?
    };

    println!("BBBB NEW_FILTER_EXEC: {:?}", new_filter_exec);

    // Now reconstruct the plan with the new filter
    if let Some(agg_exec) = partial_agg_exec.as_any().downcast_ref::<AggregateExec>() {
        let new_input: Arc<dyn ExecutionPlan> = if let Some(exprs) = projection_exprs {
            // Projection existed, so reapply its expressions on top of the filtered plan
            Arc::new(ProjectionExec::try_new(exprs, Arc::new(new_filter_exec))?)
        } else {
            // No projection to reapply, use the filtered plan directly
            Arc::new(new_filter_exec)
        };

        let input_schema = new_input.schema();
        Ok(Arc::new(AggregateExec::try_new(
            *agg_exec.mode(),
            agg_exec.group_expr().clone(),
            agg_exec.aggr_expr().to_vec(),
            agg_exec.filter_expr().to_vec(),
            new_input,
            input_schema,
        )?))
    } else {
        Ok(Arc::new(new_filter_exec))
    }
}

/// Find timestamp column in the execution plan by traversing through projections and filters
fn find_timestamp_column_in_plan(
    plan: &Arc<dyn ExecutionPlan>,
    column: &Column,
) -> Option<(usize, TimeUnit, Option<Arc<str>>)> {
    // Check current plan's schema
    if let Some(result) = find_column_in_schema(&plan.schema(), column) {
        return Some(result);
    }

    // Traverse through child plans
    for child in plan.children() {
        if let Some(result) = find_timestamp_column_in_plan(child, column) {
            return Some(result);
        }
    }

    None
}

fn find_column_in_schema(
    schema: &datafusion::arrow::datatypes::Schema,
    column: &Column,
) -> Option<(usize, TimeUnit, Option<Arc<str>>)> {
    schema.fields().iter().enumerate().find_map(|(id, f)| {
        if f.name() == &column.name {
            if let DataType::Timestamp(time_unit, tz) = f.data_type() {
                Some((id, *time_unit, tz.clone()))
            } else {
                None
            }
        } else {
            None
        }
    })
}


/// check for an existing `QCInnerAggregateExec` in the plan
fn find_existing_inner_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    match plan.name() {
        "QueryCacheAggregateExec" => true,
        "CoalescePartitionsExec" => {
            let coalesce = plan.as_any().downcast_ref::<CoalescePartitionsExec>().unwrap();
            find_existing_inner_exec(coalesce.input())
        }
        "AggregateExec" => {
            let agg = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
            find_existing_inner_exec(agg.input())
        }
        _ => false,
        // name => {
        //     dbg!(name);
        //     false
        // }
    }
}

/// A wrapper for `AggregateExec` that caches the result of the aggregation
#[derive(Debug)]
pub struct CacheUpdateAggregateExec {
    fingerprint: String,
    interval: TimeInterval,
    cache: Arc<dyn QueryCache>,
    input: Arc<dyn ExecutionPlan>,
    /// from `session_state.execution_props().query_execution_start_time.timestamp_nanos_opt()`
    now: i64,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CacheUpdateAggregateExec {
    fn new_exec_plan(
        cache: Arc<dyn QueryCache>,
        fingerprint: &str,
        interval: &TimeInterval,
        input: Arc<dyn ExecutionPlan>,
        now: i64
    ) -> Arc<dyn ExecutionPlan> {
        // we need one partition so we can store one result in the cache, use `CoalescePartitionsExec`
        // if input is not already one
        let input = if input.name() == "CoalescePartitionsExec" {
            input
        } else {
            Arc::new(CoalescePartitionsExec::new(input))
        };
        let properties = input.properties().clone();

        Arc::new(Self {
            fingerprint: fingerprint.to_string(),
            interval: *interval,
            cache,
            input,
            now,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Returns the time interval this cache update execution covers
    pub fn interval(&self) -> TimeInterval {
        self.interval
    }
}

impl DisplayAs for CacheUpdateAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{}({})", self.name(), self.input.name()),
            DisplayFormatType::Verbose => write!(f, "{} {{ interval: {:?}, input: {} }}", self.name(), self.interval(), self.input.name()),
            DisplayFormatType::TreeRender => todo!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for CacheUpdateAggregateExec {
    fn name(&self) -> &str {
        "CacheUpdateAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Self::new_exec_plan(
                self.cache.clone(),
                &self.fingerprint,
                &self.interval,
                children[0].clone(),
                self.now,
            ))
        } else {
            internal_err!("CacheUpdateAggregateExec expected one child")
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DataFusionResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CacheUpdateAggregateExec does not support partitioning");
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            execute_store(
                self.cache.clone(),
                self.fingerprint.clone(),
                self.interval,
                self.input.clone(),
                self.now,
                context,
                metrics
            )
                .map_ok(|partitions| futures::stream::iter(partitions.into_iter().map(Ok)))
                .try_flatten_stream(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn execute_store(
    cache: Arc<dyn QueryCache>,
    fingerprint: String,
    interval: TimeInterval,
    input: Arc<dyn ExecutionPlan>,
    _now: i64,
    context: Arc<TaskContext>,
    metrics: BaselineMetrics,
) -> DataFusionResult<Vec<RecordBatch>> {
    let batches = collect(input, context).await?;
    // store the result for future use
    cache.put(&fingerprint, interval, &batches).await?;
    metrics.record_output(batches.iter().map(RecordBatch::num_rows).sum());
    metrics.done();
    Ok(batches)
}

/// A wrapper for `AggregateExec` that caches the result of the aggregation
#[derive(Debug)]
pub struct CachedAggregateExec {
    cache_entry: Arc<dyn OccupiedIntervalCacheEntry>,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CachedAggregateExec {
    fn new_exec_plan(
        cache_entry: Arc<dyn OccupiedIntervalCacheEntry>,
        inner_properties: &PlanProperties,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            cache_entry,
            schema: inner_properties.eq_properties.schema().clone(),
            properties: inner_properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Returns the time interval this cached execution covers
    pub fn interval(&self) -> TimeInterval {
        self.cache_entry.interval()
    }
}

impl DisplayAs for CachedAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{}", self.name()),
            DisplayFormatType::Verbose => write!(f, "{} {{ interval: {:?} }}", self.name(), self.interval()),
            DisplayFormatType::TreeRender => todo!(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for CachedAggregateExec {
    fn name(&self) -> &str {
        "CachedAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {}", self.name())
        }
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> DataFusionResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CachedAggregateExec does not support partitioning");
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            execute_get(self.cache_entry.clone(), metrics)
                .map_ok(|partitions| futures::stream::iter(partitions.into_iter().map(Ok)))
                .try_flatten_stream(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn execute_get(
    cache_entry: Arc<dyn OccupiedIntervalCacheEntry>,
    metrics: BaselineMetrics,
) -> DataFusionResult<Vec<RecordBatch>> {
    let batches = cache_entry.get().await.map(<[RecordBatch]>::to_vec)?;
    metrics.record_output(batches.iter().map(RecordBatch::num_rows).sum());
    metrics.done();
    Ok(batches)
}

/// Normalize temporal bounds in expressions by replacing literals with placeholders
/// Public for testing purposes
pub fn normalize_temporal_bounds_in_expr(expr: &Expr, temporal_columns: &HashSet<Column>) -> Expr {
    match expr {
        Expr::BinaryExpr(bin_expr) => {
            let BinaryExpr { left, op, right } = bin_expr;

            // Check if this is a temporal comparison
            let is_temporal_comparison = match (left.as_ref(), op) {
                (Expr::Column(col), Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq) => {
                    temporal_columns.contains(col)
                }
                _ => false,
            };

            if is_temporal_comparison {
                // Replace the literal with a placeholder
                let placeholder = Expr::Literal(ScalarValue::Utf8(Some("?".to_string())), None);
                let normalized_right = if matches!(right.as_ref(), Expr::Literal(..)) {
                    placeholder
                } else {
                    normalize_temporal_bounds_in_expr(right, temporal_columns)
                };
                Expr::BinaryExpr(BinaryExpr {
                    left: left.clone(),
                    op: *op,
                    right: Box::new(normalized_right),
                })
            } else {
                // Recursively normalize both sides
                Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(normalize_temporal_bounds_in_expr(left, temporal_columns)),
                    op: *op,
                    right: Box::new(normalize_temporal_bounds_in_expr(right, temporal_columns)),
                })
            }
        }
        Expr::Between(between) => {
            let Between { expr, negated, low, high } = between;

            // Check if this is a temporal BETWEEN
            let is_temporal_between = if let Expr::Column(col) = expr.as_ref() {
                temporal_columns.contains(col)
            } else {
                false
            };

            if is_temporal_between {
                // Replace literals with placeholders
                let placeholder = Expr::Literal(ScalarValue::Utf8(Some("?".to_string())), None);
                let normalized_low = if matches!(low.as_ref(), Expr::Literal(..)) {
                    placeholder.clone()
                } else {
                    normalize_temporal_bounds_in_expr(low, temporal_columns)
                };
                let normalized_high = if matches!(high.as_ref(), Expr::Literal(..)) {
                    placeholder
                } else {
                    normalize_temporal_bounds_in_expr(high, temporal_columns)
                };

                Expr::Between(Between {
                    expr: expr.clone(),
                    negated: *negated,
                    low: Box::new(normalized_low),
                    high: Box::new(normalized_high),
                })
            } else {
                // Recursively normalize
                Expr::Between(Between {
                    expr: Box::new(normalize_temporal_bounds_in_expr(expr, temporal_columns)),
                    negated: *negated,
                    low: Box::new(normalize_temporal_bounds_in_expr(low, temporal_columns)),
                    high: Box::new(normalize_temporal_bounds_in_expr(high, temporal_columns)),
                })
            }
        }
        // For other expression types, recursively normalize
        Expr::Not(e) => Expr::Not(Box::new(normalize_temporal_bounds_in_expr(e, temporal_columns))),
        Expr::Negative(e) => Expr::Negative(Box::new(normalize_temporal_bounds_in_expr(e, temporal_columns))),
        Expr::ScalarFunction(func) => {
            let args = func.args.iter()
                .map(|arg| normalize_temporal_bounds_in_expr(arg, temporal_columns))
                .collect();
            Expr::ScalarFunction(ScalarFunction {
                func: func.func.clone(),
                args,
            })
        }
        // For literals, columns, and other types, return as-is
        _ => expr.clone(),
    }
}

/// Normalize temporal bounds in a logical plan for fingerprinting
/// Public for testing purposes
pub fn normalize_temporal_bounds_in_plan(plan: &LogicalPlan, temporal_columns: &HashSet<Column>) -> LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => {
            let normalized_predicate = normalize_temporal_bounds_in_expr(&filter.predicate, temporal_columns);
            LogicalPlan::Filter(Filter::try_new(
                normalized_predicate,
                Arc::new(normalize_temporal_bounds_in_plan(filter.input.as_ref(), temporal_columns)),
            ).expect("Failed to create normalized filter"))
        }
        LogicalPlan::Aggregate(agg) => {
            let normalized_group_expr = agg.group_expr.iter()
                .map(|expr| normalize_temporal_bounds_in_expr(expr, temporal_columns))
                .collect();
            let normalized_aggr_expr = agg.aggr_expr.iter()
                .map(|expr| normalize_temporal_bounds_in_expr(expr, temporal_columns))
                .collect();
            LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(normalize_temporal_bounds_in_plan(agg.input.as_ref(), temporal_columns)),
                normalized_group_expr,
                normalized_aggr_expr,
            ).expect("Failed to create normalized aggregate"))
        }
        // For other plan types, return as-is (we only normalize expressions in aggregations with filters)
        _ => plan.clone(),
    }
}


/// Extract static time intervals from expressions (e.g., BETWEEN, >=, <=)
#[derive(Debug)]
enum StaticInterval {
    /// Found a complete static interval
    Found(TimeInterval),
    /// Expression is stable but doesn't define a complete interval
    Stable,
    /// Expression contains dynamic elements or is unsupported
    Abandon,
}

impl StaticInterval {
    fn find(expr: &Expr, columns: &HashSet<Column>) -> Self {
        match expr {
            Expr::BinaryExpr(bin_expr) => Self::find_bin_expr(bin_expr, columns),
            Expr::Between(between) => Self::find_between(between, columns),
            Expr::Literal(..) | Expr::Column(_) => Self::Stable,
            Expr::Not(e) | Expr::Negative(e) => match Self::find(e, columns) {
                Self::Stable => Self::Stable,
                _ => Self::Abandon,
            },
            Expr::ScalarFunction(scalar) => Self::find_scalar_function(scalar),
            _ => Self::Abandon,
        }
    }

    fn find_bin_expr(bin_expr: &BinaryExpr, columns: &HashSet<Column>) -> Self {
        let BinaryExpr { left, op, right } = bin_expr;

        match op {
            Operator::Gt | Operator::GtEq => {
                if let Expr::Column(col) = left.as_ref() {
                    if columns.contains(col) {
                        if let Some(lower_ns) = Self::extract_timestamp_ns(right) {
                            let start_ns = if *op == Operator::Gt { lower_ns + 1 } else { lower_ns };
                            return Self::Found(TimeInterval::new(start_ns, i64::MAX));
                        }
                    }
                }
            }
            Operator::Lt | Operator::LtEq => {
                if let Expr::Column(col) = left.as_ref() {
                    if columns.contains(col) {
                        if let Some(upper_ns) = Self::extract_timestamp_ns(right) {
                            let end_ns = if *op == Operator::Lt { upper_ns } else { upper_ns + 1 };
                            return Self::Found(TimeInterval::new(i64::MIN, end_ns));
                        }
                    }
                }
            }
            Operator::And => {
                // Try to combine bounds from both sides
                let left_result = Self::find(left, columns);
                let right_result = Self::find(right, columns);

                return match (left_result, right_result) {
                    (Self::Found(left_int), Self::Found(right_int)) => {
                        // Combine the intervals
                        let start_ns = left_int.start_ns.max(right_int.start_ns);
                        let end_ns = left_int.end_ns.min(right_int.end_ns);
                        if start_ns < end_ns {
                            Self::Found(TimeInterval::new(start_ns, end_ns))
                        } else {
                            Self::Abandon // Invalid interval
                        }
                    }
                    (Self::Found(int), Self::Stable) | (Self::Stable, Self::Found(int)) => Self::Found(int),
                    (Self::Stable, Self::Stable) => Self::Stable,
                    _ => Self::Abandon,
                };
            }
            _ => return Self::Abandon,
        }

        // For other operators, check both sides
        let left = Self::find(left, columns);
        let right = Self::find(right, columns);
        left.either(right)
    }

    fn find_between(between: &Between, columns: &HashSet<Column>) -> Self {
        if let Expr::Column(ref col) = *between.expr {
            if !columns.contains(col) {
                return Self::Stable;
            }
        } else {
            return Self::Abandon;
        }

        let Some(lower_ns) = Self::extract_timestamp_ns(&between.low) else {
            return Self::Abandon;
        };
        let Some(upper_ns) = Self::extract_timestamp_ns(&between.high) else {
            return Self::Abandon;
        };

        // BETWEEN is inclusive, so convert to half-open [lower, upper+1)
        Self::Found(TimeInterval::new(lower_ns, upper_ns + 1))
    }

    fn find_scalar_function(_scalar: &ScalarFunction) -> Self {
        // Static functions are not supported for interval caching
        Self::Abandon
    }

    fn extract_timestamp_ns(expr: &Expr) -> Option<i64> {
        match expr {
            Expr::Literal(ScalarValue::TimestampNanosecond(Some(ns), _), _) => Some(*ns),
            Expr::Literal(ScalarValue::TimestampMicrosecond(Some(us), _), _) => Some(us * 1_000),
            Expr::Literal(ScalarValue::TimestampMillisecond(Some(ms), _), _) => Some(ms * 1_000_000),
            Expr::Literal(ScalarValue::TimestampSecond(Some(s), _), _) => Some(s * 1_000_000_000),
            // Handle cast expressions: CAST(string_literal AS TIMESTAMP)
            Expr::Cast(cast) => {
                if let Expr::Literal(ScalarValue::Utf8(Some(timestamp_str)), _) = cast.expr.as_ref() {
                    // Try to parse as RFC3339 timestamp
                    if let Ok(dt) = DateTime::parse_from_rfc3339(timestamp_str) {
                        dt.timestamp_nanos_opt().map(|ns| ns)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Find the value which is more important
    fn either(self, other: Self) -> Self {
        match (self, other) {
            (Self::Abandon, _) | (_, Self::Abandon) => Self::Abandon,
            (Self::Found(int), _) | (_, Self::Found(int)) => Self::Found(int),
            (Self::Stable, Self::Stable) => Self::Stable,
        }
    }
}

