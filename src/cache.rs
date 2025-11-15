use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::Result as DataFusionResult;

/// Half-open time interval [start_ns, end_ns)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimeInterval {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TimeInterval {
    pub fn new(start_ns: i64, end_ns: i64) -> Self {
        Self { start_ns, end_ns }
    }

    /// Check if this interval is completely contained within another interval
    pub fn is_contained_in(&self, other: &TimeInterval) -> bool {
        self.start_ns >= other.start_ns && self.end_ns <= other.end_ns
    }

    /// Check if this interval overlaps with another
    pub fn overlaps(&self, other: &TimeInterval) -> bool {
        self.start_ns < other.end_ns && other.start_ns < self.end_ns
    }

    /// Get the length of this interval
    pub fn length(&self) -> i64 {
        self.end_ns - self.start_ns
    }
}

#[async_trait]
pub trait QueryCache: Send + Sync + fmt::Debug {
    async fn lookup(&self, query_fingerprint: &str, req: &TimeInterval) -> DataFusionResult<Vec<Arc<dyn OccupiedIntervalCacheEntry>>>;
    async fn put(&self, query_fingerprint: &str, interval: TimeInterval, record_batch: &[RecordBatch]) -> DataFusionResult<()>;
}

pub async fn print_cache_state<C: QueryCache>(cache: &C, fingerprint: &str) -> DataFusionResult<()> {
    let intervals = cache.lookup(fingerprint, &TimeInterval::new(i64::MIN, i64::MAX)).await?;
    if intervals.is_empty() {
        println!("Cache[{}] = <empty>", fingerprint);
    } else {
        println!("Cache[{}] = {} intervals", fingerprint, intervals.len());
        for (i, entry) in intervals.iter().enumerate() {
            let interval = entry.interval();
            let batches = entry.get().await?;
            println!("  interval[{}]: [{}, {}) batches={}", i, interval.start_ns, interval.end_ns, batches.len());
            for (j, batch) in batches.iter().enumerate() {
                println!("    batch[{}]: {:?}", j, batch);
            }
        }
    }
    Ok(())
}

#[async_trait]
pub trait OccupiedIntervalCacheEntry: Send + Sync + fmt::Debug {
    /// The time interval this cache entry covers
    fn interval(&self) -> TimeInterval;

    /// Returns the record batch stored in this cache entry.
    async fn get(&self) -> DataFusionResult<&[RecordBatch]>;
}

#[derive(Clone, Default)]
pub struct MemoryQueryCache {
    cache: Arc<Mutex<HashMap<String, Vec<(TimeInterval, Arc<Vec<RecordBatch>>)>>>>,
}

impl fmt::Debug for MemoryQueryCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct MemoryQueryCacheEntry {
            interval: TimeInterval,
            num_rows: usize,
        }
        struct Entries<'a>(&'a MemoryQueryCache);

        impl<'a> fmt::Debug for Entries<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let binding = self.0.cache.lock().unwrap();
                let entries = binding.iter().flat_map(|(fingerprint, intervals)| {
                    intervals.iter().map(move |(interval, rb)| {
                        (
                            fingerprint,
                            MemoryQueryCacheEntry {
                                interval: *interval,
                                num_rows: rb.iter().map(RecordBatch::num_rows).sum(),
                            },
                        )
                    })
                });
                f.debug_map().entries(entries).finish()
            }
        }

        f.debug_struct("MemoryQueryCache")
            .field("entries", &Entries(self))
            .finish()
    }
}

/// Normalize a fingerprint string for consistent caching by removing schema-specific details
pub fn normalize_fingerprint_for_caching(fingerprint: &str) -> String {
    let mut result = fingerprint.to_string();

    // Replace timestamp literals with placeholders, keeping timezone normalization
    let timestamp_ns_re = regex::Regex::new(
        r#"TimestampNanosecond\(\d+,\s*(?:None|Some\("[^"]*"\))\)"#,
    )
    .unwrap();
    result = timestamp_ns_re
        .replace_all(&result, "TimestampNanosecond(?, None)")
        .to_string();

    let timestamp_us_re = regex::Regex::new(
        r#"TimestampMicrosecond\(\d+,\s*(?:None|Some\("[^"]*"\))\)"#,
    )
    .unwrap();
    result = timestamp_us_re
        .replace_all(&result, "TimestampMicrosecond(?, None)")
        .to_string();

    // Replace string timestamp literals
    let string_timestamp_re = regex::Regex::new(r"Utf8\([^)]+\)").unwrap();
    result = string_timestamp_re.replace_all(&result, "Utf8(?)").to_string();

    // Replace TableScan schema information with a canonical representation
    let table_scan_re = regex::Regex::new(r"TableScan: \w+ \[[^\]]+\]").unwrap();
    result = table_scan_re.replace_all(&result, "TableScan: $table [normalized_schema]").to_string();

    result
}

impl MemoryQueryCache {
    pub fn display(&self) -> String {
        struct DisplayMemoryQueryCache<'a>(&'a MemoryQueryCache);

        impl fmt::Display for DisplayMemoryQueryCache<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                writeln!(f, "## MemoryQueryCache:")?;
                for (fingerprint, intervals) in self.0.cache.lock().unwrap().iter() {
                    for (interval, record_batch) in intervals {
                        let table = pretty_format_batches(record_batch).map_err(|_| fmt::Error)?;
                        writeln!(f, "Fingerprint (cache key): {fingerprint}\ninterval: [{}, {}) data:\n{table}",
                                interval.start_ns, interval.end_ns)?;
                    }
                }
                Ok(())
            }
        }

        DisplayMemoryQueryCache(self).to_string()
    }

    fn put(&self, fingerprint: &str, interval: TimeInterval, record_batch: &[RecordBatch]) {
        let mut cache = self.cache.lock().unwrap();
        let fingerprint_key = fingerprint.to_owned();
        let intervals = cache.entry(fingerprint_key).or_insert_with(Vec::new);
        intervals.push((interval, Arc::new(record_batch.to_vec())));
    }
}

#[async_trait]
impl QueryCache for MemoryQueryCache {
    async fn lookup(&self, query_fingerprint: &str, req: &TimeInterval) -> DataFusionResult<Vec<Arc<dyn OccupiedIntervalCacheEntry>>> {
        println!("AAAA CACHE_DEBUG: Looking up fingerprint: '{}'", query_fingerprint);
        println!("AAAA CACHE_DEBUG: Requested interval: [{}, {})", req.start_ns, req.end_ns);
        let cache = self.cache.lock().unwrap();
        let query_fingerprint_string = query_fingerprint.to_string();

        // Find the matching intervals
        // let found_intervals: Option<Vec<(TimeInterval, Arc<Vec<RecordBatch>>)>> = None;
        let found_intervals = cache.get(&query_fingerprint_string);
        if let Some(intervals) = found_intervals {
            println!("AAAA CACHE_DEBUG: Found intervals: {:?}", intervals.iter().map(|(interval, _)| interval.clone()).collect::<Vec<_>>());
        } else {
            println!("AAAA CACHE_DEBUG: No intervals found");
        }

        if let Some(intervals) = found_intervals {
            // Find all intervals that overlap with the requested interval
            let entries: Vec<Arc<dyn OccupiedIntervalCacheEntry>> = intervals
                .iter()
                .filter(|(interval, _)| interval.overlaps(req))
                .map(|(interval, record_batch)| OccupiedMemoryIntervalCacheEntry {
                    fingerprint: query_fingerprint.to_string(),
                    interval: *interval,
                    record_batch: record_batch.clone(),
                })
                .map(|e| Arc::new(e) as Arc<dyn OccupiedIntervalCacheEntry>)
                .collect();
            Ok(entries)
        } else {
            Ok(Vec::new())
        }
    }

    async fn put(&self, query_fingerprint: &str, interval: TimeInterval, record_batch: &[RecordBatch]) -> DataFusionResult<()> {
        self.put(query_fingerprint, interval, record_batch);
        Ok(())
    }
}

#[derive(Debug)]
struct OccupiedMemoryIntervalCacheEntry {
    fingerprint: String,
    interval: TimeInterval,
    record_batch: Arc<Vec<RecordBatch>>,
}

#[async_trait]
impl OccupiedIntervalCacheEntry for OccupiedMemoryIntervalCacheEntry {
    fn interval(&self) -> TimeInterval {
        self.interval
    }

    async fn get(&self) -> DataFusionResult<&[RecordBatch]> {
        Ok(&self.record_batch)
    }
}

// TODO disk cache
