//! Framed TCP transport.
//!
//! Real multi-process operation with bounded frames, timeouts, malformed-frame
//! rejection, stale-peer handling, and graceful shutdown. No in-memory channels
//! are used for cross-process communication.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use crate::errors::{FabricError, FabricResult};
use crate::protocol::{self, Frame, Op};

/// Default read/write timeout for idle connections.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum bytes buffered while scanning for a frame boundary.
pub const MAX_BUFFER: usize = protocol::MAX_FRAME_PAYLOAD + protocol::HEADER_LEN + 1;

/// A client connection to a fabric peer.
pub struct Connection {
    stream: TcpStream,
    read_buf: Vec<u8>,
    next_request_id: u64,
}

impl Connection {
    pub fn connect(addr: &SocketAddr) -> FabricResult<Self> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| FabricError::TransportError(format!("connect {addr}: {e}")))?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(DEFAULT_IDLE_TIMEOUT))?;
        stream.set_write_timeout(Some(DEFAULT_IDLE_TIMEOUT))?;
        Ok(Self {
            stream,
            read_buf: Vec::with_capacity(1 << 16),
            next_request_id: 1,
        })
    }

    pub fn peer_addr(&self) -> FabricResult<SocketAddr> {
        self.stream
            .peer_addr()
            .map_err(|e| FabricError::TransportError(e.to_string()))
    }

    /// Send a request frame and await its matching response.
    pub fn call(&mut self, op: Op, payload: &[u8]) -> FabricResult<Vec<u8>> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.wrapping_add(1);
        let frame = Frame {
            op,
            flags: 0,
            request_id,
            payload: payload.to_vec(),
        };
        self.write_frame(&frame)?;
        let response = self.read_frame()?;
        if response.request_id != request_id {
            return Err(FabricError::ProtocolError(format!(
                "mismatched request id: expected {request_id}, got {}",
                response.request_id
            )));
        }
        if !response.is_response() {
            return Err(FabricError::ProtocolError(
                "peer sent a request frame in response to a request".into(),
            ));
        }
        Ok(response.payload)
    }

    /// Send one frame.
    pub fn write_frame(&mut self, frame: &Frame) -> FabricResult<()> {
        let wire = protocol::encode(frame)?;
        self.stream.write_all(&wire)?;
        self.stream.flush()?;
        Ok(())
    }

    /// Read one frame, enforcing bounds and timeouts.
    pub fn read_frame(&mut self) -> FabricResult<Frame> {
        loop {
            if let Some((frame, consumed)) = protocol::decode(&self.read_buf)? {
                self.read_buf.drain(..consumed);
                return Ok(frame);
            }
            if self.read_buf.len() > MAX_BUFFER {
                return Err(FabricError::ProtocolError(
                    "receive buffer exceeded maximum frame size".into(),
                ));
            }
            let mut chunk = [0u8; 64 * 1024];
            let n = self.stream.read(&mut chunk).map_err(|e| {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    FabricError::Timeout("transport idle timeout".into())
                } else if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset
                    || e.kind() == std::io::ErrorKind::ConnectionAborted
                {
                    FabricError::TransportError(format!("peer closed connection: {e}"))
                } else {
                    FabricError::TransportError(format!("read: {e}"))
                }
            })?;
            if n == 0 {
                return Err(FabricError::TransportError(
                    "peer closed connection (eof)".into(),
                ));
            }
            self.read_buf.extend_from_slice(&chunk[..n]);
        }
    }

    /// Read one frame with a custom timeout (re-applies read timeout).
    pub fn read_frame_timeout(&mut self, timeout: Duration) -> FabricResult<Frame> {
        self.stream.set_read_timeout(Some(timeout))?;
        let r = self.read_frame();
        self.stream.set_read_timeout(Some(DEFAULT_IDLE_TIMEOUT))?;
        r
    }

    pub fn shutdown(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Bounded RPC client. A failed call is never replayed automatically: once a
/// request frame has been written, a missing response is an ambiguous outcome
/// and replaying a mutating operation could execute it twice. The connection is
/// refreshed for the caller's *next* operation instead.
pub struct RpcClient {
    addr: SocketAddr,
    conn: Connection,
}

impl RpcClient {
    pub fn connect(addr: &SocketAddr) -> FabricResult<Self> {
        Ok(Self {
            addr: *addr,
            conn: Connection::connect(addr)?,
        })
    }

    pub fn call(&mut self, op: Op, payload: &[u8]) -> FabricResult<Vec<u8>> {
        match self.conn.call(op, payload) {
            Ok(v) => Ok(v),
            Err(original) => {
                if let Ok(conn) = Connection::connect(&self.addr) {
                    self.conn = conn;
                }
                Err(original)
            }
        }
    }

    pub fn call_json<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        &mut self,
        op: Op,
        payload: &T,
    ) -> FabricResult<R> {
        let bytes = serde_json::to_vec(payload)?;
        let response = self.call(op, &bytes)?;
        serde_json::from_slice(&response)
            .map_err(|e| FabricError::ProtocolError(format!("bad response payload: {e}")))
    }

    /// Decode a generic error envelope, returning the embedded error string.
    pub fn decode_error(payload: &[u8]) -> Option<String> {
        serde_json::from_slice::<protocol::Envelope>(payload)
            .ok()
            .and_then(|e| e.error)
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.addr
    }
}

/// A request handler for a server side. Clonable via `Arc`; must be `Send + Sync`
/// because it is shared across connection threads.
pub type RequestHandler =
    Arc<dyn Fn(&mut Connection, Op, u64, &[u8]) -> FabricResult<Vec<u8>> + Send + Sync>;

/// A simple counting semaphore with `try_acquire` semantics.
struct CountSemaphore {
    available: std::sync::Mutex<usize>,
    cv: std::sync::Condvar,
}

impl CountSemaphore {
    fn new(n: usize) -> Self {
        Self {
            available: std::sync::Mutex::new(n),
            cv: std::sync::Condvar::new(),
        }
    }

    fn try_acquire(&self) -> bool {
        let mut a = self.available.lock().unwrap();
        if *a > 0 {
            *a -= 1;
            true
        } else {
            false
        }
    }

    fn release(&self) {
        let mut a = self.available.lock().unwrap();
        *a += 1;
        self.cv.notify_one();
    }
}

/// A permit guard that releases the semaphore on drop.
struct Permit<'a> {
    sem: &'a CountSemaphore,
}

impl<'a> Drop for Permit<'a> {
    fn drop(&mut self) {
        self.sem.release();
    }
}

/// Bounded TCP server dispatching frames to a handler.
///
/// Concurrency is bounded by `max_connections`. Shutdown is graceful and bounded:
/// the accept loop unblocks, worker threads observe the stop flag on their next
/// frame read, and joins are bounded by the idle read timeout.
pub struct Server {
    listener: TcpListener,
    handler: RequestHandler,
    max_connections: usize,
    running: std::sync::Arc<std::sync::atomic::AtomicBool>,
    workers: std::sync::Mutex<Vec<std::sync::mpsc::Receiver<()>>>,
    shutdown_budget: Duration,
}

impl Server {
    pub fn bind(addr: SocketAddr, handler: RequestHandler) -> FabricResult<Self> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| FabricError::TransportError(format!("bind {addr}: {e}")))?;
        Ok(Self::from_parts(listener, handler))
    }

    /// Build a server from an already-bound listener (used when the handler
    /// must be created after the bind, e.g. inside `Arc::new_cyclic`).
    pub fn from_parts(listener: TcpListener, handler: RequestHandler) -> Self {
        Self {
            listener,
            handler,
            max_connections: 64,
            running: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            workers: std::sync::Mutex::new(Vec::new()),
            shutdown_budget: DEFAULT_IDLE_TIMEOUT.saturating_add(Duration::from_secs(10)),
        }
    }

    pub fn with_max_connections(mut self, n: usize) -> Self {
        self.max_connections = n.max(1);
        self
    }

    pub fn local_addr(&self) -> FabricResult<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| FabricError::TransportError(e.to_string()))
    }

    /// Accept loop. Returns when stopped or on a fatal accept error.
    pub fn serve(&self) -> FabricResult<()> {
        let running = self.running.clone();
        let semaphore = std::sync::Arc::new(CountSemaphore::new(self.max_connections));

        loop {
            if !running.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(());
            }

            if !semaphore.try_acquire() {
                // At capacity: bounded wait, then re-check the stop flag.
                std::thread::sleep(Duration::from_millis(25));
                continue;
            }
            let (stream, _peer) = match self.listener.accept() {
                Ok(x) => x,
                Err(e) => {
                    semaphore.release();
                    if running.load(std::sync::atomic::Ordering::SeqCst) {
                        log::warn!("accept failed: {e}");
                    }
                    continue;
                }
            };
            if !running.load(std::sync::atomic::Ordering::SeqCst) {
                semaphore.release();
                let _ = stream.shutdown(std::net::Shutdown::Both);
                return Ok(());
            }

            if let Err(e) = stream.set_nodelay(true) {
                log::debug!("set_nodelay: {e}");
            }
            if let Err(e) = stream.set_read_timeout(Some(DEFAULT_IDLE_TIMEOUT)) {
                log::debug!("set_read_timeout: {e}");
            }
            let handler = self.handler.clone();
            let running2 = running.clone();
            let sem2 = semaphore.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
            self.workers.lock().unwrap().push(done_rx);
            std::thread::spawn(move || {
                let mut conn = Connection {
                    stream,
                    read_buf: Vec::with_capacity(1 << 16),
                    next_request_id: 1,
                };
                let result = serve_connection(&mut conn, &handler, &running2);
                if let Err(e) = result {
                    log::debug!("connection ended: {e}");
                }
                drop(Permit { sem: &sem2 });
                let _ = done_tx.send(());
            });
        }
    }

    /// Stop accepting and join worker threads within a bounded budget.
    pub fn shutdown(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Ok(addr) = self.listener.local_addr() {
            // Self-connect unblocks a pending accept().
            let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(200));
        }
        let workers = std::mem::take(&mut *self.workers.lock().unwrap());
        let deadline = std::time::Instant::now() + self.shutdown_budget;
        for rx in workers {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let _ = rx.recv_timeout(remaining);
        }
    }
}

fn serve_connection(
    conn: &mut Connection,
    handler: &RequestHandler,
    running: &std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> FabricResult<()> {
    loop {
        if !running.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let frame = match conn.read_frame() {
            Ok(f) => f,
            Err(e) => {
                // Malformed frames and protocol violations close the connection.
                log::debug!("closing connection: {e}");
                return Err(e);
            }
        };
        if frame.is_close() {
            return Ok(());
        }
        if frame.is_response() {
            return Err(FabricError::ProtocolError(
                "server received unexpected response frame".into(),
            ));
        }
        let response = handler(conn, frame.op, frame.request_id, &frame.payload);
        match response {
            Ok(payload) => {
                conn.write_frame(&Frame::response(frame.op, frame.request_id, payload))?;
            }
            Err(e) => {
                let envelope = protocol::Envelope::err(&e.to_string());
                let payload: Vec<u8> = serde_json::to_vec(&envelope)?;
                conn.write_frame(&Frame::response(frame.op, frame.request_id, payload))?;
            }
        }
    }
}

/// Read a frame from a raw stream with a bounded buffer (server-side helper).
pub fn read_frame_from_stream(stream: &mut TcpStream, buf: &mut Vec<u8>) -> FabricResult<Frame> {
    loop {
        if let Some((frame, consumed)) = protocol::decode(buf)? {
            buf.drain(..consumed);
            return Ok(frame);
        }
        if buf.len() > MAX_BUFFER {
            return Err(FabricError::ProtocolError(
                "receive buffer exceeded maximum frame size".into(),
            ));
        }
        let mut chunk = [0u8; 64 * 1024];
        let n = stream.read(&mut chunk).map_err(|e| {
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut
            {
                FabricError::Timeout("transport idle timeout".into())
            } else {
                FabricError::TransportError(format!("read: {e}"))
            }
        })?;
        if n == 0 {
            return Err(FabricError::TransportError("eof".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Write a frame to a raw stream.
pub fn write_frame_to_stream(stream: &mut TcpStream, frame: &Frame) -> FabricResult<()> {
    let wire = protocol::encode(frame)?;
    stream.write_all(&wire)?;
    stream.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_over_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf = Vec::new();
            let frame = read_frame_from_stream(&mut stream, &mut buf).unwrap();
            assert_eq!(frame.payload, b"ping");
            write_frame_to_stream(
                &mut stream,
                &Frame::response(Op::Pong, frame.request_id, b"pong".to_vec()),
            )
            .unwrap();
        });

        let mut client = Connection::connect(&addr).unwrap();
        let resp = client.call(Op::Ping, b"ping").unwrap();
        assert_eq!(resp, b"pong");
        server.join().unwrap();
    }

    #[test]
    fn garbage_closes_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf = Vec::new();
            let res = read_frame_from_stream(&mut stream, &mut buf);
            assert!(res.is_err());
            // assert a malformed-frame rejection happened
            assert!(matches!(res, Err(FabricError::ProtocolError(_))));
        });

        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"GARBAGEGARBAGEGARBAGEGARBAGE").unwrap();
        stream.flush().unwrap();
        // small sleep to let the server read and fail
        std::thread::sleep(Duration::from_millis(300));
        server.join().unwrap();
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut wire = b"CFAB".to_vec();
        wire.push(protocol::PROTOCOL_VERSION);
        wire.push(Op::Ping as u8);
        wire.push(0);
        wire.push(0);
        wire.extend_from_slice(&0u64.to_be_bytes());
        wire.extend_from_slice(&(protocol::MAX_FRAME_PAYLOAD as u32 + 1).to_be_bytes());
        wire.extend_from_slice(&0u32.to_be_bytes());
        // decode-level rejection of oversized lengths
        assert!(protocol::decode(&wire).is_err());

        // stream-level rejection
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut buf = Vec::new();
            let res = read_frame_from_stream(&mut stream, &mut buf);
            assert!(matches!(res, Err(FabricError::ProtocolError(_))));
        });
        let mut client = TcpStream::connect(addr).unwrap();
        client.write_all(&wire).unwrap();
        client.flush().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn idle_timeout_fires() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_millis(300)))
                .unwrap();
            let mut buf = Vec::new();
            let res = read_frame_from_stream(&mut stream, &mut buf);
            assert!(matches!(res, Err(FabricError::Timeout(_))));
        });
        let _client = TcpStream::connect(addr).unwrap();
        std::thread::sleep(Duration::from_millis(700));
        server.join().unwrap();
    }

    #[test]
    fn server_bounded_and_graceful() {
        let server = Server::bind(
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(|_, op, id, payload| {
                Ok(protocol::Frame::response(op, id, payload.to_vec()).payload)
            }),
        )
        .unwrap();
        let server = Arc::new(server);
        let addr = server.local_addr().unwrap();
        let serve_thread = {
            let srv = server.clone();
            std::thread::spawn(move || srv.serve())
        };
        std::thread::sleep(Duration::from_millis(100));
        let mut c = Connection::connect(&addr).unwrap();
        let resp = c.call(Op::Ping, b"x").unwrap();
        assert_eq!(resp, b"x");
        server.shutdown();
        let _ = serve_thread.join();
        drop(server);
        // repeated restart on the same port after the listener is released
        let server2 = Server::bind(
            addr,
            Arc::new(|_, op, id, payload| {
                Ok(protocol::Frame::response(op, id, payload.to_vec()).payload)
            }),
        )
        .unwrap();
        assert_eq!(server2.local_addr().unwrap(), addr);
        server2.shutdown();
    }

    #[test]
    fn rpc_client_reconnects() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut buf = Vec::new();
                if let Ok(frame) = read_frame_from_stream(&mut stream, &mut buf) {
                    let resp = format!("echo-{}", String::from_utf8_lossy(&frame.payload));
                    write_frame_to_stream(
                        &mut stream,
                        &Frame::response(frame.op, frame.request_id, resp.into_bytes()),
                    )
                    .unwrap();
                }
            }
        });
        let mut client = RpcClient::connect(&addr).unwrap();
        let r1 = client.call(Op::Ping, b"a").unwrap();
        assert_eq!(r1, b"echo-a");
        // force a stale connection by closing client side; next call reconnects
        client.conn.shutdown();
        assert!(client.call(Op::Ping, b"ambiguous").is_err());
        let r2 = client.call(Op::Ping, b"b").unwrap();
        assert_eq!(r2, b"echo-b");
        server.join().unwrap();
    }
}
