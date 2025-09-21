/// Query execution result.
#[derive(Debug, Default)]
pub struct ScyllaDBQueryResult {
    /// Number of retrieved items. Or the number of items in the light-weight transaction.
    pub rows_num: u64,
    /// Only valid when using a light-weight transaction.
    pub rows_affected: u64,
}

impl Extend<ScyllaDBQueryResult> for ScyllaDBQueryResult {
    fn extend<T: IntoIterator<Item = ScyllaDBQueryResult>>(&mut self, query_results: T) {
        for query_result in query_results {
            self.rows_num += query_result.rows_num;
            self.rows_affected += query_result.rows_affected;
        }
    }
}
