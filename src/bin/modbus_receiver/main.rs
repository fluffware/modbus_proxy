use bytes::Bytes;
use clap::Parser;
use futures::future::select_all;
use log::error;
use std::future::{self, Future};
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
use tokio_modbus::client::{tcp, Context};
use tokio_modbus::server::tcp::Server as TcpServer;
use tokio_modbus::slave::Slave;

const ILLEGAL_FUNCTION: u8 = 1;
//const ILLEGAL_DATA_ADDRESS: u8 = 2;
fn err_resp(
    req: &tokio_modbus::prelude::Request,
    exception: u8,
) -> tokio_modbus::prelude::Response {
    tokio_modbus::prelude::Response::Custom(
        0x80 | Bytes::from(req.clone()).slice(0..1)[0],
        vec![exception],
    )
}

struct ModbusState {
    read_count: u32,
}

#[derive(Clone)]
struct ModbusService {
    state: Arc<Mutex<ModbusState>>,
}

impl ModbusService {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ModbusState { read_count: 0 })),
        }
    }
    pub fn print_status(&self) {
	let state = self.state.lock().unwrap();
	println!("Read: {:6}", state.read_count);
    }
}

impl tokio_modbus::server::Service for ModbusService {
    type Request = tokio_modbus::prelude::Request;
    type Response = tokio_modbus::prelude::Response;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;
    fn call(&self, req: Self::Request) -> Self::Future {
        let mut state = self.state.lock().unwrap();
        let resp = match req {
            Self::Request::ReadHoldingRegisters(start, count) => {
                state.read_count += 1;
                let mut reply = Vec::with_capacity(usize::from(count));
                for i in 0..count {
                    reply.push(i + start);
                }
                Ok(Self::Response::ReadHoldingRegisters(reply))
            }
            /*
                Self::Request::WriteSingleRegister(addr, value) => {}
                Self::Request::WriteMultipleRegisters(addr, ref value) => {}
                Self::Request::ReadInputRegisters(start, count) => {}

                Self::Request::ReadCoils(start, count) => {}
                Self::Request::WriteSingleCoil(addr, value) => {}
                Self::Request::WriteMultipleCoils(addr, ref value) => {}
                Self::Request::ReadDiscreteInputs(start, count) => {}
            */
            _ => Ok(err_resp(&req, ILLEGAL_FUNCTION)),
        };
        Box::pin(future::ready(resp))
    }
}

struct ModbusNewService {
    service: ModbusService,
}

impl ModbusNewService {
    pub fn new(service: ModbusService) -> Self {
        ModbusNewService { service }
    }
}

impl tokio_modbus::server::NewService for ModbusNewService {
    type Request = tokio_modbus::prelude::Request;
    type Response = tokio_modbus::prelude::Response;
    type Error = std::io::Error;
    type Instance = ModbusService;
    fn new_service(&self) -> std::io::Result<Self::Instance> {
        Ok(self.service.clone())
    }
}

#[derive(Parser, Debug)]
struct CmdArgs {
    /// Bind server to this IP-address
    #[arg(long,default_value_t=IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    ip_address: IpAddr,
}

#[tokio::main]
pub async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    let args = CmdArgs::parse();

    let socket = SocketAddr::new(args.ip_address, 502);
    let server = TcpServer::new(socket);
    let service = ModbusService::new();
    let service_factory = ModbusNewService::new(service.clone());
    let serving = server.serve(service_factory);
    tokio::pin!(serving);
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    loop {
	#[rustfmt::skip]
	tokio::select!{
	    res = &mut serving => {
		match res {
		    Ok(_) => break,
		    Err(e) => {
			error!("Server failed: {}", e);
			return ExitCode::FAILURE;
		    }
		}
	    }
	    _ = tick.tick() => {
		service.print_status();
	    }
	}
    }
    ExitCode::SUCCESS
}
