#![warn(missing_docs)]
#![doc = include_str!("lib.md")]

pub use sqlx_scylladb_core::*;

/// Re-exported external crates.
pub mod ext {
    pub use sqlx_scylladb_ext::*;
}

/// Runtime-generic database driver. `any` feature is required.
#[cfg(feature = "any")]
pub mod any {
    pub use sqlx_scylladb_core::any::*;
}

/// `macros` feature is required.
#[cfg(feature = "macros")]
pub mod macros {
    pub use sqlx_scylladb_macros::*;
}
