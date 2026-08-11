//! Checkpoint Fabric CLI entry point.

use clap::Parser;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let cli = checkpoint_fabric::cli::Cli::parse();
    if let Err(e) = checkpoint_fabric::cli::run(&cli) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
