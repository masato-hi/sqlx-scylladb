pub use scylla::{DeserializeRow, DeserializeValue, SerializeRow, SerializeValue};

/// Re-exported [scylla-cql](https://docs.rs/scylla-cql/latest/scylla_cql/) crate.
pub mod cql {
    pub use scylla_cql::frame::response::result::{
        CollectionType, ColumnType, NativeType, UserDefinedType,
    };
}

/// Re-exported [scylla::cluster] module.
pub mod cluster {
    /// Re-exported [scylla::cluster::metadata] module.
    pub mod metadata {
        pub use scylla::cluster::metadata::ColumnType;
    }
}

/// Re-exported [scylla::errors] module.
pub mod errors {
    pub use scylla::errors::SerializationError;
}

/// Re-exported [scylla::serialize] module.
pub mod serialize {
    /// Re-exported [scylla::serialize::row] module.
    pub mod row {
        pub use scylla::serialize::row::{RowSerializationContext, SerializeRow};
    }
    /// Re-exported [scylla::serialize::value] module.
    pub mod value {
        pub use scylla::serialize::value::SerializeValue;
    }
    /// Re-exported  [scylla::serialize::writers] module.
    pub mod writers {
        pub use scylla::serialize::writers::{CellWriter, RowWriter, WrittenCellProof};
    }
}
