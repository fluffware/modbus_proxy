use clap::{Arg, ArgAction, ArgMatches, Command};
use log::{info, warn, LevelFilter};
use std::sync::atomic::{AtomicBool, Ordering};
use systemd::daemon::notify;
use systemd::daemon::{STATE_READY, STATE_STOPPING};
use systemd::journal::JournalLog;

static DAEMON: AtomicBool = AtomicBool::new(true);

pub fn add_args(app_args: Command) -> Command {
    app_args.arg(
        Arg::new("NO_SYSTEMD")
            .long("no-systemd")
            .action(ArgAction::SetTrue)
            .help("Don't expect to be run from systemd"),
    )
}

pub fn start(args: &ArgMatches) {
    DAEMON.store(
        !args.get_one::<bool>("NO_SYSTEMD").unwrap_or(&false),
        Ordering::Relaxed,
    );
    if DAEMON.load(Ordering::Relaxed) {
        if let Err(e) = JournalLog::init() {
            eprintln!("Failed to start logging: {}", e);
        }
        log::set_max_level(LevelFilter::Info);
    } else {
        tracing_subscriber::fmt::init();
        info!("Server starting");
    }
}

pub fn ready() {
    if DAEMON.load(Ordering::Relaxed) {
        if let Err(e) = notify(false, [(STATE_READY, "1")].iter()) {
            warn!("Failed to notify systemd of ready state: {}", e);
        }
    } else {
        info!("Server ready");
    }
}

pub fn exiting() {
    if DAEMON.load(Ordering::Relaxed) {
        if let Err(e) = notify(false, [(STATE_STOPPING, "1")].iter()) {
            warn!("Failed to notify systemd of stopping: {}", e);
        }
    } else {
        info!("Server exiting");
    }
    log::logger().flush()
}
