/// The result of executing one or more ScyllaDB statements.
#[derive(Debug, Default)]
pub struct ScyllaDBQueryResult {
    /// Number of rows returned by the query, or the number of rows returned by a lightweight transaction.
    pub rows_num: u64,
    /// Number of rows affected by a lightweight transaction.
    ///
    /// This field is meaningful only for lightweight transactions.
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
