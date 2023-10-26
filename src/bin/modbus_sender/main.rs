use clap::Parser;
use log::error;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::process::ExitCode;
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
    loop {
	let start = 0;
	let count = 20;
        let regs = match mb.read_holding_registers(start, count).await {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to read from server: {}", e);
                break;
            }
        };
	for i in 0..count {
	    assert_eq!(regs[usize::from(i)], i+start);
	}
    }
    ExitCode::SUCCESS
}
