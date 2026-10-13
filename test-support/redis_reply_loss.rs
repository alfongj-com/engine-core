//! Forward real Redis traffic and drop one successful checkpoint CAS reply.
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};
use twmq::redis;

pub(crate) struct ReplyLossProxy {
    pub(crate) url: String,
    pub(crate) armed: Arc<AtomicBool>,
    pub(crate) dropped: Arc<AtomicBool>,
    task: JoinHandle<()>,
}
impl Drop for ReplyLossProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ReplyLossProxy {
    #[allow(dead_code)] // The helper is also included by observation-only tests.
    pub(crate) async fn start(upstream: &str) -> Self {
        Self::start_at(upstream, None).await
    }
    /// Lose the selected complete INFO/INFO/GET observation reply. Earlier
    /// observations are forwarded unchanged, so ordinal two cuts after ACK.
    #[allow(dead_code)] // The helper is also included by CAS-only test modules.
    pub(crate) async fn observation_loss(upstream: &str, ordinal: usize) -> Self {
        assert!(ordinal > 0);
        Self::start_at(upstream, Some(ordinal)).await
    }
    async fn start_at(upstream: &str, observation_ordinal: Option<usize>) -> Self {
        let client = redis::Client::open(upstream).unwrap();
        let database = client.get_connection_info().redis.db;
        let redis::ConnectionAddr::Tcp(host, port) = client.get_connection_info().addr.clone()
        else {
            panic!("transport test requires local TCP Redis");
        };
        assert!(matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port_here = listener.local_addr().unwrap().port();
        let armed = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let observations = Arc::new(AtomicUsize::new(0));
        let arm = armed.clone();
        let drop_notice = dropped.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        let address = (host.clone(), port);
                        let armed = arm.clone();
                        let dropped = drop_notice.clone();
                        let observations = observations.clone();
                        connections.spawn(async move {
                            let server = TcpStream::connect(address).await?;
                            let (client_read, mut client_write) = client.into_split();
                            let (server_read, mut server_write) = server.into_split();
                            let mut client_read = BufReader::new(client_read);
                            let mut server_read = BufReader::new(server_read);
                            loop {
                                let command = frame(&mut client_read).await?;
                                server_write.write_all(&command).await?;
                                let response = frame(&mut server_read).await?;
                                // EVALSHA's NOSCRIPT response must be forwarded;
                                // lose only a successful applied transition.
                                let selected_reply = match observation_ordinal {
                                    None => command.windows(4).any(|part| part == b"EVAL")
                                        && response == b":1\r\n",
                                    Some(ordinal) => command == b"*1\r\n$4\r\nEXEC\r\n"
                                        && response.windows(7).any(|part| part == b"run_id:")
                                        && armed.load(Ordering::SeqCst)
                                        && observations.fetch_add(1, Ordering::SeqCst) + 1 == ordinal,
                                };
                                if selected_reply && armed.swap(false, Ordering::SeqCst)
                                {
                                    dropped.store(true, Ordering::SeqCst);
                                    return Ok::<(), io::Error>(());
                                }
                                client_write.write_all(&response).await?;
                            }
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {},
                }
            }
        });
        Self {
            url: format!("redis://127.0.0.1:{port_here}/{database}"),
            armed,
            dropped,
            task,
        }
    }
}

// Read one complete RESP2/RESP3 frame, including nested arrays/maps. Forward
// bytes unchanged; the test proxy does not implement or imitate Redis commands.
async fn frame<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut remaining = 1usize;
    while remaining > 0 {
        remaining -= 1;
        let offset = bytes.len();
        if reader.read_until(b'\n', &mut bytes).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        let line = &bytes[offset..];
        if line.len() < 3 || !line.ends_with(b"\r\n") {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let kind = line[0];
        if matches!(kind, b'$' | b'=' | b'!' | b'*' | b'~' | b'>' | b'%' | b'|') {
            let count = std::str::from_utf8(&line[1..line.len() - 2])
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or(io::ErrorKind::InvalidData)?;
            if count < 0 {
                continue;
            }
            let count = usize::try_from(count).map_err(|_| io::ErrorKind::InvalidData)?;
            if count > 16 * 1024 * 1024 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            if matches!(kind, b'$' | b'=' | b'!') {
                let start = bytes.len();
                bytes.resize(start + count + 2, 0);
                reader.read_exact(&mut bytes[start..]).await?;
            } else {
                remaining += count * if matches!(kind, b'%' | b'|') { 2 } else { 1 };
            }
        }
    }
    Ok(bytes)
}
