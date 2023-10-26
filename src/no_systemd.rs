use clap::{ArgMatches, Command};
use log::info;

pub fn add_args(app_args: Command) -> Command {
    app_args
}

pub fn start(_args: &ArgMatches) {
    tracing_subscriber::fmt::init();
    info!("Server starting");
}

pub fn ready() {
    info!("Server ready");
}

pub fn exiting() {
    info!("Server exiting");
}
