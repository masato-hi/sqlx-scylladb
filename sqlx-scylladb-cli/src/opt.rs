use std::ops::{Deref, Not};

use clap::{
    Args, Parser,
    builder::{Styles, styling::AnsiColor},
};

const HELP_STYLES: Styles = Styles::styled()
    .header(AnsiColor::Blue.on_default().bold())
    .usage(AnsiColor::Blue.on_default().bold())
    .literal(AnsiColor::White.on_default())
    .placeholder(AnsiColor::Green.on_default());

#[derive(Parser, Debug)]
#[clap(version, about, author, styles = HELP_STYLES)]
pub struct Opt {
    // https://github.com/launchbadge/sqlx/pull/3724 placed this here,
    // but the intuitive place would be in the arguments for each subcommand.
    #[clap(flatten)]
    pub no_dotenv: NoDotenvOpt,

    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Parser, Debug)]
pub enum Command {
    #[clap(alias = "db")]
    Database(DatabaseOpt),

    #[clap(alias = "mig")]
    Migrate(MigrateOpt),
}

/// Group of commands for creating and dropping your database.
#[derive(Parser, Debug)]
pub struct DatabaseOpt {
    #[clap(subcommand)]
    pub command: DatabaseCommand,
}

#[derive(Parser, Debug)]
pub enum DatabaseCommand {
    /// Creates the database specified in your DATABASE_URL.
    Create {
        #[clap(flatten)]
        connect_opts: ConnectOpts,
    },

    /// Drops the database specified in your DATABASE_URL.
    Drop {
        #[clap(flatten)]
        confirmation: Confirmation,

        #[clap(flatten)]
        connect_opts: ConnectOpts,

        /// PostgreSQL only: force drops the database.
        #[clap(long, short, default_value = "false")]
        force: bool,
    },

    /// Drops the database specified in your DATABASE_URL, re-creates it, and runs any pending migrations.
    Reset {
        #[clap(flatten)]
        confirmation: Confirmation,

        #[clap(flatten)]
        source: Source,

        #[clap(flatten)]
        connect_opts: ConnectOpts,

        /// PostgreSQL only: force drops the database.
        #[clap(long, short, default_value = "false")]
        force: bool,
    },

    /// Creates the database specified in your DATABASE_URL and runs any pending migrations.
    Setup {
        #[clap(flatten)]
        source: Source,

        #[clap(flatten)]
        connect_opts: ConnectOpts,
    },
}

/// Group of commands for creating and running migrations.
#[derive(Parser, Debug)]
pub struct MigrateOpt {
    #[clap(subcommand)]
    pub command: MigrateCommand,
}

#[derive(Parser, Debug)]
pub enum MigrateCommand {
    /// Create a new migration with the given description.
    ///
    /// A version number will be automatically assigned to the migration.
    ///
    /// For convenience, this command will attempt to detect if sequential versioning is in use,
    /// and if so, continue the sequence.
    ///
    /// Sequential versioning is inferred if:
    ///
    /// * The version numbers of the last two migrations differ by exactly 1, or:
    ///
    /// * only one migration exists and its version number is either 0 or 1.
    ///
    /// Otherwise timestamp versioning is assumed.
    ///
    /// This behavior can overridden by `--sequential` or `--timestamp`, respectively.
    Add {
        description: String,

        #[clap(flatten)]
        source: Source,

        /// If true, creates a pair of up and down migration files with same version
        /// else creates a single sql file
        #[clap(short)]
        reversible: bool,

        /// If set, use timestamp versioning for the new migration. Conflicts with `--sequential`.
        #[clap(short, long)]
        timestamp: bool,

        /// If set, use sequential versioning for the new migration. Conflicts with `--timestamp`.
        #[clap(short, long, conflicts_with = "timestamp")]
        sequential: bool,
    },

    /// Run all pending migrations.
    Run {
        #[clap(flatten)]
        source: Source,

        /// List all the migrations to be run without applying
        #[clap(long)]
        dry_run: bool,

        #[clap(flatten)]
        ignore_missing: IgnoreMissing,

        #[clap(flatten)]
        connect_opts: ConnectOpts,

        /// Apply migrations up to the specified version. If unspecified, apply all
        /// pending migrations. If already at the target version, then no-op.
        #[clap(long)]
        target_version: Option<i64>,
    },

    /// Revert the latest migration with a down file.
    Revert {
        #[clap(flatten)]
        source: Source,

        /// List the migration to be reverted without applying
        #[clap(long)]
        dry_run: bool,

        #[clap(flatten)]
        ignore_missing: IgnoreMissing,

        #[clap(flatten)]
        connect_opts: ConnectOpts,

        /// Revert migrations down to the specified version. If unspecified, revert
        /// only the last migration. Set to 0 to revert all migrations. If already
        /// at the target version, then no-op.
        #[clap(long)]
        target_version: Option<i64>,
    },

    /// List all available migrations.
    Info {
        #[clap(flatten)]
        source: Source,

        #[clap(flatten)]
        connect_opts: ConnectOpts,
    },
}

/// Argument for the migration scripts source.
#[derive(Args, Debug)]
pub struct Source {
    /// Path to folder containing migrations.
    #[clap(long, default_value = "migrations")]
    source: String,
}

impl Deref for Source {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.source
    }
}

/// Argument for the database URL.
#[derive(Args, Debug)]
pub struct ConnectOpts {
    #[clap(flatten)]
    pub no_dotenv: NoDotenvOpt,

    /// Location of the DB, by default will be read from the DATABASE_URL env var or `.env` files.
    #[clap(long, short = 'D', env)]
    pub database_url: Option<String>,

    /// Location of the DB, by default will be read from the SCYLLADB_URL env var or `.env` files.
    #[clap(long, env)]
    pub scylladb_url: Option<String>,

    /// The maximum time, in seconds, to try connecting to the database server before
    /// returning an error.
    #[clap(long, default_value = "10")]
    pub connect_timeout: u64,
}

#[derive(Args, Debug)]
pub struct NoDotenvOpt {
    /// Do not automatically load `.env` files.
    #[clap(long)]
    // Parsing of this flag is actually handled _before_ calling Clap,
    // by `crate::maybe_apply_dotenv()`.
    #[allow(unused)] // TODO: switch to `#[expect]`
    pub no_dotenv: bool,
}

impl ConnectOpts {
    /// Require a database URL to be provided, otherwise
    /// return an error.
    pub fn required_db_url(&self) -> anyhow::Result<&str> {
        let database_url = if let Some(scylladb_url) = self.scylladb_url.as_deref() {
            Some(scylladb_url)
        } else {
            self.database_url.as_deref()
        };

        database_url.ok_or_else(
            || anyhow::anyhow!(
                "the `--database-url` option or the `DATABASE_URL` or `SCYLLADB_URL` environment variable must be provided"
            )
        )
    }
}

/// Argument for automatic confirmation.
#[derive(Args, Copy, Clone, Debug)]
pub struct Confirmation {
    /// Automatic confirmation. Without this option, you will be prompted before dropping
    /// your database.
    #[clap(short)]
    pub yes: bool,
}

/// Argument for ignoring applied migrations that were not resolved.
#[derive(Args, Copy, Clone, Debug)]
pub struct IgnoreMissing {
    /// Ignore applied migrations that are missing in the resolved migrations
    #[clap(long)]
    ignore_missing: bool,
}

impl Deref for IgnoreMissing {
    type Target = bool;

    fn deref(&self) -> &Self::Target {
        &self.ignore_missing
    }
}

impl Not for IgnoreMissing {
    type Output = bool;

    fn not(self) -> Self::Output {
        !self.ignore_missing
    }
}
