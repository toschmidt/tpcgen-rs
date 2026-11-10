use crate::DEFAULT_BATCH_SIZE;
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatchReader;
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{DeleteGenerator, DeleteGeneratorIterator};

/// Generate the orders deleted by an update set (dbgen's `delete.<n>`) as
/// Arrow [`RecordBatch`]es of one column, `orderkey`
pub struct DeleteArrow {
    inner: DeleteGeneratorIterator,
    batch_size: usize,
}

impl DeleteArrow {
    /// Return the schema without initializing a data generator.
    pub fn schema_ref() -> SchemaRef {
        Arc::clone(&DELETE_SCHEMA)
    }

    pub fn new(generator: DeleteGenerator) -> Self {
        Self {
            inner: generator.iter(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchReader for DeleteArrow {
    fn schema(&self) -> SchemaRef {
        Self::schema_ref()
    }
}

impl Iterator for DeleteArrow {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let orderkey = Int64Array::from_iter_values(
            self.inner
                .by_ref()
                .take(self.batch_size)
                .map(|r| r.orderkey),
        );
        if orderkey.is_empty() {
            return None;
        }
        Some(RecordBatch::try_new(
            self.schema(),
            vec![Arc::new(orderkey)],
        ))
    }
}

static DELETE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_delete_schema);
fn make_delete_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "orderkey",
        DataType::Int64,
        false,
    )]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_keys_as_the_generator() {
        let keys: Vec<i64> = DeleteGenerator::new(0.01, 1)
            .iter()
            .map(|r| r.orderkey)
            .collect();
        let batches: Vec<RecordBatch> = DeleteArrow::new(DeleteGenerator::new(0.01, 1))
            .with_batch_size(7)
            .collect::<Result<_, _>>()
            .unwrap();
        let arrow: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert!(!keys.is_empty());
        assert_eq!(arrow, keys);
    }
}
