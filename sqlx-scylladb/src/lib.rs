#![warn(missing_docs)]
#![doc = include_str!("lib.md")]

pub use sqlx_scylladb_core::*;

/// External crates re-exported for use by generated code and integrations.
pub mod ext {
    pub use ::scylla;
    pub use ::scylla_cql;
    pub use ::sqlx;
    pub use ::sqlx_core::ext::ustr;
}

/// Runtime-generic ScyllaDB driver. Requires the `any` feature.
#[cfg(feature = "any")]
pub mod any {
    pub use sqlx_scylladb_core::any::*;
}

/// Procedural macros provided by this crate. Requires the `macros` feature.
#[cfg(feature = "macros")]
pub mod macros {
    pub use sqlx_scylladb_macros::*;
}
