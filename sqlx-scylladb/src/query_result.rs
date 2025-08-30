#[derive(Debug, Default)]
pub struct ScyllaDBQueryResult {
    pub rows_affected: u64,
}

impl Extend<ScyllaDBQueryResult> for ScyllaDBQueryResult {
    fn extend<T: IntoIterator<Item = ScyllaDBQueryResult>>(&mut self, _: T) {
        ()
    }
}
