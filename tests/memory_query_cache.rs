use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion_query_cache::{MemoryQueryCache, QueryCache, TimeInterval};
use std::sync::Arc;

/// Helper function to create a simple dummy RecordBatch for testing
fn create_dummy_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_single_interval_lookup() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();
    let fingerprint = "test_query";

    // Put an interval [10, 20) into cache
    let interval = TimeInterval::new(10, 20);
    cache
        .put(fingerprint, interval.clone(), &[batch.clone()])
        .await
        .unwrap();

    // Lookup the exact same interval - should find it
    let entries = cache.lookup(fingerprint, &interval).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].interval(), interval);

    // Verify the data
    let data = entries[0].get().await.unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0].num_rows(), 1);
}

#[tokio::test]
async fn test_nested_intervals_contained() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();
    let fingerprint = "test_query";

    // Put a large interval [10, 50) into cache
    let large_interval = TimeInterval::new(10, 50);
    cache.put(fingerprint, large_interval, &[batch.clone()]).await.unwrap();

    // Lookup a nested interval [20, 30) that is completely contained within [10, 50)
    let nested_interval = TimeInterval::new(20, 30);
    let entries = cache.lookup(fingerprint, &nested_interval).await.unwrap();

    // Should find the large interval that contains the nested one
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].interval(), TimeInterval::new(10, 50));
}

#[tokio::test]
async fn test_nested_intervals_container() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();
    let fingerprint = "test_query";

    // Put a small interval [20, 30) into cache
    let small_interval = TimeInterval::new(20, 30);
    cache.put(fingerprint, small_interval, &[batch.clone()]).await.unwrap();

    // Lookup a large interval [10, 50) that completely contains the cached [20, 30)
    let large_interval = TimeInterval::new(10, 50);
    let entries = cache.lookup(fingerprint, &large_interval).await.unwrap();

    // Should find the small interval that overlaps with the large one
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].interval(), TimeInterval::new(20, 30));
}

#[tokio::test]
async fn test_overlapping_intervals_partial() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();
    let fingerprint = "test_query";

    // Put two overlapping intervals: [10, 30) and [20, 40)
    let interval1 = TimeInterval::new(10, 30);
    let interval2 = TimeInterval::new(20, 40);
    cache
        .put(fingerprint, interval1.clone(), &[batch.clone()])
        .await
        .unwrap();
    cache
        .put(fingerprint, interval2.clone(), &[batch.clone()])
        .await
        .unwrap();

    // Lookup interval [15, 35) that overlaps with both cached intervals
    let lookup_interval = TimeInterval::new(15, 35);
    let entries = cache.lookup(fingerprint, &lookup_interval).await.unwrap();

    // Should find both overlapping intervals
    assert_eq!(entries.len(), 2);

    // Check that we got the expected intervals (order doesn't matter)
    let found_intervals: std::collections::HashSet<_> = entries.iter().map(|e| e.interval()).collect();
    let expected_intervals: std::collections::HashSet<_> = [interval1, interval2].into_iter().collect();
    assert_eq!(found_intervals, expected_intervals);
}

#[tokio::test]
async fn test_non_overlapping_intervals() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();
    let fingerprint = "test_query";

    // Put interval [10, 20) into cache
    let cached_interval = TimeInterval::new(10, 20);
    cache.put(fingerprint, cached_interval, &[batch.clone()]).await.unwrap();

    // Lookup a completely separate interval [30, 40) that doesn't overlap
    let lookup_interval = TimeInterval::new(30, 40);
    let entries = cache.lookup(fingerprint, &lookup_interval).await.unwrap();

    // Should find no overlapping intervals
    assert_eq!(entries.len(), 0);
}

#[tokio::test]
async fn test_adjacent_intervals_touching() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();
    let fingerprint = "test_query";

    // Put adjacent intervals with a gap: [10, 20) and [25, 35)
    let interval1 = TimeInterval::new(10, 20);
    let interval2 = TimeInterval::new(25, 35);
    cache.put(fingerprint, interval1, &[batch.clone()]).await.unwrap();
    cache.put(fingerprint, interval2, &[batch.clone()]).await.unwrap();

    // Lookup interval [12, 18) that overlaps with first but not second
    let lookup_interval = TimeInterval::new(12, 18);
    let entries = cache.lookup(fingerprint, &lookup_interval).await.unwrap();

    // Should find only the first interval
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].interval(), TimeInterval::new(10, 20));
}

#[tokio::test]
async fn test_multiple_fingerprints() {
    let cache = MemoryQueryCache::default();
    let batch = create_dummy_batch();

    // Put intervals for different fingerprints
    let interval1 = TimeInterval::new(10, 20);
    let interval2 = TimeInterval::new(10, 20);
    cache.put("query1", interval1, &[batch.clone()]).await.unwrap();
    cache.put("query2", interval2, &[batch.clone()]).await.unwrap();

    // Lookup should only return intervals for the specific fingerprint
    let entries1 = cache.lookup("query1", &TimeInterval::new(10, 20)).await.unwrap();
    let entries2 = cache.lookup("query2", &TimeInterval::new(10, 20)).await.unwrap();

    assert_eq!(entries1.len(), 1);
    assert_eq!(entries2.len(), 1);
    assert_eq!(entries1[0].interval(), TimeInterval::new(10, 20));
    assert_eq!(entries2[0].interval(), TimeInterval::new(10, 20));

    // Lookup for non-existent fingerprint should return empty
    let entries_empty = cache.lookup("query3", &TimeInterval::new(10, 20)).await.unwrap();
    assert_eq!(entries_empty.len(), 0);
}

#[tokio::test]
async fn test_empty_cache_lookup() {
    let cache = MemoryQueryCache::default();

    // Lookup on empty cache should return empty
    let entries = cache
        .lookup("any_fingerprint", &TimeInterval::new(10, 20))
        .await
        .unwrap();
    assert_eq!(entries.len(), 0);
}

#[tokio::test]
async fn test_multiple_batches_per_interval() {
    let cache = MemoryQueryCache::default();
    let batch1 = create_dummy_batch();
    let batch2 = create_dummy_batch();
    let fingerprint = "test_query";

    // Put multiple batches for the same interval
    let interval = TimeInterval::new(10, 20);
    cache
        .put(fingerprint, interval.clone(), &[batch1.clone(), batch2.clone()])
        .await
        .unwrap();

    // Lookup should return the interval with both batches
    let entries = cache.lookup(fingerprint, &interval).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].interval(), interval);

    let data = entries[0].get().await.unwrap();
    assert_eq!(data.len(), 2); // Two batches
    assert_eq!(data[0].num_rows(), 1);
    assert_eq!(data[1].num_rows(), 1);
}
