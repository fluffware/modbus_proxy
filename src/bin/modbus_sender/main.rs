use clap::Parser;
use futures::FutureExt;
use log::{error, info};
use std::net::IpAddr;
use std::net::SocketAddr;
use std::process::ExitCode;
use tokio::signal;
use tokio_modbus::client::tcp;
use tokio_modbus::client::Reader;
use tokio_modbus::slave::Slave;

#[derive(Parser, Debug)]
struct CmdArgs {
    /// IP-address of server
    ip_address: IpAddr,
}

#[tokio::main]
pub async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    let args = CmdArgs::parse();
    let socket = SocketAddr::new(args.ip_address, 502);
    let mut mb = match tcp::connect_slave(socket, Slave(1)).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to connect to server: {}", e);
            return ExitCode::FAILURE;
        }
    };

    let mut read_count = 0;

    let stop = signal::ctrl_c().fuse();
    tokio::pin!(stop);
    let mut stopped = false;

    'next: while !stopped {
        let start = 0;
        let count = 20;
        let reader = mb.read_holding_registers(start, count);
        tokio::pin!(reader);
        let regs;
        loop {
            #[rustfmt::skip]
            tokio::select! {
                res = &mut reader => {
		    regs = match res {
			Ok(r) => r,
			Err(e) => {
			    error!("Failed to read from server: {}", e);
			    break 'next;
			}
                    };
		    break;
                }
                _ = &mut stop => {
                    stopped = true;
		}
            }
        }
        read_count += 1;
        if stopped {
            break;
        }
        for i in 0..count {
            assert_eq!(regs[usize::from(i)], i + start);
        }
    }
    info!("{} requests sent", read_count);
    ExitCode::SUCCESS
}
