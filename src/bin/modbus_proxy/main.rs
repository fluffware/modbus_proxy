use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use futures::stream::FuturesUnordered;
use futures_util::StreamExt;
use log::{debug, error, warn};
use modbus_proxy::daemon;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

type DynResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug)]
struct ProxyRequest {
    transaction_id: u16,
    request: Vec<u8>,
    respond: oneshot::Sender<ProxyResponse>,
}

#[derive(Debug)]
struct ProxyResponse {
    transaction_id: u16,
    response: Vec<u8>,
}

fn parse_message(msg_buf: &mut [u8], msg_len: &mut usize) -> Option<(u16, Vec<u8>)> {
    if *msg_len <= 8 {
        return None;
    }
    let transaction_id = u16::from_be_bytes(msg_buf[0..2].try_into().unwrap());
    if u16::from_be_bytes(msg_buf[2..4].try_into().unwrap()) != 0 {
        *msg_len = 0;
        return None;
    }
    let req_len = u16::from_be_bytes(msg_buf[4..6].try_into().unwrap());
    let req_end = (req_len + 6) as usize;
    if req_end > *msg_len {
        return None;
    }
    let msg = Vec::from(&msg_buf[6..(req_len + 6) as usize]);
    if req_end < *msg_len {
        msg_buf.copy_within(req_end..*msg_len, 0);
    }
    *msg_len -= req_end;
    Some((transaction_id, msg))
}

async fn connection_handler(
    mut stream: TcpStream,
    requests: mpsc::Sender<ProxyRequest>,
    cancel: Arc<Notify>,
) -> DynResult<()> {
    let mut msg_buf = [0u8; 1024];
    let mut msg_len = 0;
    if let Err(e) = stream.set_nodelay(true) {
        warn!("Failed to set TCP_NODELAY on stream: {e}");
    }
    let mut write_buffer = Vec::new(); // Reuse this buffer to avoid reallocating it.
    let mut pending = FuturesUnordered::<oneshot::Receiver<ProxyResponse>>::new();
    'main_loop: loop {
        #[rustfmt::skip]
        tokio::select! {
            res = stream.read(&mut msg_buf[msg_len..]) => {
                match res {
                    Ok(rlen) => {
                        if rlen == 0 {
                            break 'main_loop;
                        }
			msg_len += rlen;
			if let Some((transaction_id, request)) = parse_message(&mut msg_buf, &mut msg_len) {
			    let (respond, response) = oneshot::channel();
			    pending.push(response);
			    requests.send(ProxyRequest{transaction_id, request, respond}).await?;
			}
			if msg_len == msg_buf.len() {
			    warn!("Buffer overflow. Skipping.");
			    // Buffer is full, but still no message. Start over.
			    msg_len = 0;
			}
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }	   
            Some(next) = pending.next(), if !pending.is_empty() => {
		if let Ok(resp) = next {
		    let [th,tl] = resp.transaction_id.to_be_bytes();
		    let [lh, ll] = (resp.response.len() as u16).to_be_bytes();
		    let header = [th,tl, 0,0, lh,ll];
                    write_buffer.clear();
                    write_buffer.extend_from_slice(&header);
                    write_buffer.extend_from_slice(&resp.response);
		    stream.write_all(&write_buffer).await?;
		}
            }
            _ = cancel.notified() => {
                break 'main_loop;
            }
        }
    }
    Ok(())
}

async fn tcp_listener(
    socket: Vec<SocketAddr>,
    requests: mpsc::Sender<ProxyRequest>,
    cancel: Arc<Notify>,
) -> DynResult<()> {
    let mut child_handlers = FuturesUnordered::new();
    // Dummy task that blocks until canceled
    let cancel_block = cancel.clone();
    child_handlers.push(tokio::spawn(async move {
        cancel_block.notified().await;
        Ok(())
    }));
    let listener = TcpListener::bind(socket.as_slice()).await?;
    loop {
        #[rustfmt::skip]
        tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok((stream, sock)) => {
                        debug!("Connection from {sock}");
                        let h = tokio::spawn(connection_handler(stream, requests.clone(),
                                                                cancel.clone()));
                        child_handlers.push(h);
                    }
                    Err(e) => {
                        error!("accept failed: {e}");
                    }
                }
            }
            next_handler = child_handlers.next() => {
                match next_handler {
                    Some(res) => {
                        match res {
                            Ok(Err(e)) => {
				error!("Connection handler exited with error: {e}");
                            }
                            Ok(Ok(())) => {}
                            Err(e) => {
				error!("Connection handler failed: {e}");
                            }
                        }
                    }
                    None => break
                }
            }
        }
    }
    Ok(())
}

const SLAVE_DEVICE_FAILURE: u8 = 0x04;

fn error_response(req: ProxyRequest, code: u8) {
    let func = req.request.get(1).unwrap_or(&0);
    req.respond
        .send(ProxyResponse {
            transaction_id: req.transaction_id,
            response: vec![0x80 | func, code],
        })
        .unwrap();
}
async fn tcp_client(mut requests: mpsc::Receiver<ProxyRequest>, addr: SocketAddr) -> DynResult<()> {
    'main_loop: loop {
        let conn = TcpStream::connect(addr);
        tokio::pin!(conn);
        let mut stream;
        'connect: loop {
            #[rustfmt::skip]
            tokio::select! {
		res = requests.recv() => {
		    debug!("Rejected request while not connected");
                    let Some(req) = res else {break 'main_loop};
                    error_response(req, SLAVE_DEVICE_FAILURE);
		}
		res = conn.as_mut() => {
                    match res {
			Ok(s) => {
			    stream = s;
			    break 'connect;
			}
			Err(e) => {
			    error!("Failed to connect to server {addr}: {e}");
			    tokio::time::sleep(Duration::from_secs(5)).await;
			    continue 'main_loop;
			}
                    }
		}
            }
        }

        if let Err(e) = stream.set_nodelay(true) {
            warn!("Failed to set TCP_NODELAY on stream: {e}");
        }
        debug!("Connected to {}", addr);
        let mut write_buffer = Vec::new(); // Reuse this buffer to avoid reallocating it.
        let mut msg_buf = [0u8; 1024];
        let mut msg_len = 0;
        let mut current_req = None;
        'connected: loop {
            #[rustfmt::skip]
            tokio::select! {
		res = requests.recv(), if current_req.is_none()=> {
                    let Some(req) = res else {break 'main_loop};
                    let [th,tl] = req.transaction_id.to_be_bytes();
                    let [lh, ll] = (req.request.len() as u16).to_be_bytes();
		    write_buffer.clear();

                    let header = [th,tl, 0,0, lh,ll];
                    write_buffer.extend_from_slice(&header);
                    write_buffer.extend_from_slice(&req.request);
                    if let Err(e) = stream.write_all(&write_buffer).await {
			error!("Failed to write request to server: {e}");
			break 'connected;
                    }
                    
		    if let Err(e) = stream.flush().await {
			error!("Failed to flush request to server: {e}");
			break 'connected;
		    }
                    
                    current_req = Some(req);
                }
		_ = tokio::time::sleep(Duration::from_millis(1000)), if current_req.is_some() => {
		    if let Some(req) = current_req.take() {
			error_response(req, SLAVE_DEVICE_FAILURE);
		    }
		    warn!("Client timeout.");
		}
                res = stream.read(&mut msg_buf[msg_len..]) => {
                    match res {
                        Ok(rlen) => {
                            if rlen == 0 {
                                break 'connected;
                            }
                            msg_len += rlen;
                            if let Some((transaction_id, response)) = parse_message(&mut msg_buf, &mut msg_len) {
				if let Some(req) = current_req.take() {
				    let _ = req.respond.send(ProxyResponse{transaction_id, response});
				}
                            }
                            if msg_len == msg_buf.len() {
				// Buffer is full, but still no message. Start over.
				msg_len = 0;
                            }
			    
                        }
                        Err(e) => {
                            error!("Failed to read from client: {e}");
                            break 'connected;
                        }
                    }
                }
            }
        }
        if let Some(req) = current_req.take() {
            error_response(req, SLAVE_DEVICE_FAILURE);
        }
    }
    Ok(())
}

//const DEFAULT_SERIAL_DEVICE: &str = "/dev/ttyUSB0";
const DEFAULT_TCP_PORT: u16 = 502; // Modbus port

const DEFAULT_SERIAL_SPEED: u32 = 9600;

const DEFAULT_REQUEST_DELAY: u64 = 10; // 10ms

#[derive(Subcommand, Debug)]
enum Client {
    Rtu(SerialClientArgs),
    Tcp(TcpClientArgs),
}

#[derive(Args, Debug)]
struct TcpClientArgs {
    /// Client address.
    client_addr: IpAddr,
    /// Client TCP port.
    #[arg(long)]
    client_port: Option<u16>,
}

#[derive(Args, Debug)]
struct SerialClientArgs {
    /// Serial port
    serial_device: String,

    /// Serial speed (bps)
    #[arg(long, short = 's', default_value_t=DEFAULT_SERIAL_SPEED)]
    serial_speed: u32,
    /// Select odd parity
    #[arg(long, short = 'o')]
    odd_parity: bool,
    /// Select even parity
    #[arg(long, short = 'e', conflicts_with("odd_parity"))]
    even_parity: bool,
}

#[derive(Parser, Debug)]
struct CmdArgs {
    /// Proxy TCP port listening for clients
    #[arg(long, short='p', default_value_t=DEFAULT_TCP_PORT)]
    proxy_port: u16,
    /// Local IP address of proxy
    #[arg(long, short = 'b')]
    proxy_addr: Option<IpAddr>,

    #[command(subcommand)]
    sub_commands: Client,

    /// Minimum time (in milliseconds) from previous response until next requests to server.
    #[arg(long, short = 't', default_value_t=DEFAULT_REQUEST_DELAY)]
    request_delay: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cmd = CmdArgs::command();
    let cmd = daemon::add_args(cmd);
    let matches = cmd.get_matches();
    let args = match CmdArgs::from_arg_matches(&matches) {
        Ok(a) => a,
        Err(e) => {
            error!("{e}");
            return ExitCode::FAILURE;
        }
    };
    daemon::start(&matches);
    /*
    let parity = if args.serial_client.even_parity {
        Parity::Even
    } else if args.serial_client.odd_parity {
        Parity::Odd
    } else {
        Parity::None
    };
    let ser_conf = tokio_serial::new(args.serial_device, args.serial_speed).parity(parity);
    let ser = match SerialStream::open(&ser_conf) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to open serial device: {e}");
            return ExitCode::FAILURE;
        }
    });
     */
    let bind_addr: Vec<SocketAddr> = if let Some(addr) = args.proxy_addr {
        vec![SocketAddr::from((addr, args.proxy_port))]
    } else {
        vec![
            SocketAddr::from((Ipv6Addr::UNSPECIFIED, args.proxy_port)),
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, args.proxy_port)),
        ]
    };
    let (request_send, request_recv) = mpsc::channel(20);
    let cancel = Arc::new(Notify::new());
    let net_task = tokio::spawn(tcp_listener(bind_addr, request_send, cancel.clone()));
    tokio::pin!(net_task);
    let client_task: JoinHandle<DynResult<()>> = match args.sub_commands {
        Client::Tcp(args) => {
            let addr = args.client_addr;
            let port = args.client_port.unwrap_or(502);
            let client_addr = SocketAddr::from((addr, port));
            tokio::spawn(tcp_client(request_recv, client_addr))
        }
        Client::Rtu(_args) => {
            panic!("RTU not implemented");
        }
    };
    tokio::pin!(client_task);
    daemon::ready();
    'main_loop: loop {
        #[rustfmt::skip]
        tokio::select! {
            res = signal::ctrl_c() => {
                if let Err(e) = res {
                    error!("Failed to wait for ctrl-c: {}",e);
                }
                cancel.notify_waiters();
            },
            res = net_task.as_mut() => {
                match res {
                    Ok(res) => {
                        if let Err(e) = res {
                            error!("{e}")
                        }
                    },
                    Err(e) => error!("Network task failed: {e}"),
                }
                break 'main_loop;
            }
            res = client_task.as_mut() => {
		match res {
                    Ok(res) => {
                        if let Err(e) = res {
                            error!("{e}")
                        }
                    },
		    Err(e) => error!("Client task failed: {e}"),
		}
		break 'main_loop;
            }
        }
    }

    daemon::exiting();
    ExitCode::SUCCESS
}
