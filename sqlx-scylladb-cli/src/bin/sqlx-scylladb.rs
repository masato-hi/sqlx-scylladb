use clap::Parser;
use console::style;
use sqlx_scylladb_cli::{Opt, install_default_drivers, maybe_apply_dotenv};

#[tokio::main]
async fn main() {
    maybe_apply_dotenv();

    install_default_drivers();

    let opt = Opt::parse();

    // no special handling here
    if let Err(error) = sqlx_scylladb_cli::run(opt).await {
        println!("{} {}", style("error:").bold().red(), error);
        std::process::exit(1);
    }
}
