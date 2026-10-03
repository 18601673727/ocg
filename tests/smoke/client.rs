use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Instant;

use serde_json::Value;

use super::harness::{remaining, Result, BODY_LIMIT};

pub struct DeadlineSocket {
    stream: TcpStream,
    deadline: Instant,
}

impl DeadlineSocket {
    pub fn new(stream: TcpStream, deadline: Instant) -> Self {
        Self { stream, deadline }
    }
}

impl Read for DeadlineSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.stream
            .set_read_timeout(Some(remaining(self.deadline)?))?;
        self.stream.read(bytes)
    }
}

impl Write for DeadlineSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.stream
            .set_write_timeout(Some(remaining(self.deadline)?))?;
        self.stream.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

pub fn line(reader: &mut impl BufRead, limit: usize) -> Result<Option<String>> {
    let mut bytes = Vec::new();
    let size = reader
        .take((limit + 1) as u64)
        .read_until(b'\n', &mut bytes)?;
    if size > limit {
        return Err(format!("protocol line exceeds {limit} bytes").into());
    }
    if size == 0 {
        return Ok(None);
    }
    if bytes.last() != Some(&b'\n') {
        return Err(format!(
            "stream closed inside a line: {:?}",
            String::from_utf8_lossy(&bytes)
        )
        .into());
    }
    Ok(Some(
        String::from_utf8(bytes)?
            .trim_end_matches(['\r', '\n'])
            .to_owned(),
    ))
}

pub struct Response {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    reader: BufReader<DeadlineSocket>,
}

impl Response {
    pub fn open(
        address: SocketAddr,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        payload: Option<&Value>,
        deadline: Instant,
    ) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&address, remaining(deadline)?)?;
        let mut socket = DeadlineSocket::new(stream, deadline);
        let body = payload
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default();
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        );
        for (name, value) in headers {
            if name.contains(['\r', '\n']) || value.contains(['\r', '\n']) {
                return Err("invalid smoke request header".into());
            }
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        socket.write_all(request.as_bytes())?;
        socket.write_all(&body)?;
        let mut reader = BufReader::new(socket);
        let status_line = line(&mut reader, 8192)?.ok_or("HTTP closed before status")?;
        let mut fields = status_line.split_whitespace();
        if fields.next() != Some("HTTP/1.1") {
            return Err(format!("unexpected HTTP status line: {status_line}").into());
        }
        let status = fields.next().ok_or("missing HTTP status")?.parse()?;
        let mut headers = Vec::new();
        let mut header_bytes = status_line.len();
        loop {
            let header = line(&mut reader, 8192)?.ok_or("HTTP closed inside headers")?;
            header_bytes += header.len();
            if header_bytes > 65536 {
                return Err("HTTP headers exceed 64 KiB".into());
            }
            if header.is_empty() {
                break;
            }
            let (name, value) = header.split_once(':').ok_or("malformed HTTP header")?;
            headers.push((name.to_ascii_lowercase(), value.trim().to_owned()));
        }
        Ok(Self {
            status,
            headers,
            body: Vec::new(),
            reader,
        })
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    pub fn read_body(&mut self) -> Result<()> {
        // Both the control API and the fixture use Content-Length or close.
        // Refuse unsupported framing rather than accidentally parsing it as JSON.
        if self.header("transfer-encoding").is_some() {
            return Err("unexpected Transfer-Encoding in smoke response".into());
        }
        let length = self
            .header("content-length")
            .map(str::parse::<usize>)
            .transpose()?;
        if length.is_some_and(|length| length > BODY_LIMIT) {
            return Err("HTTP body exceeds smoke limit".into());
        }
        let mut bytes = [0; 4096];
        loop {
            if length == Some(self.body.len()) {
                break;
            }
            let room = length.map_or(bytes.len(), |length| {
                (length - self.body.len()).min(bytes.len())
            });
            let size = self.reader.read(&mut bytes[..room])?;
            if size == 0 {
                if length.is_some() {
                    return Err("HTTP closed before Content-Length was received".into());
                }
                break;
            }
            if self.body.len() + size > BODY_LIMIT {
                return Err("HTTP body exceeds smoke limit".into());
            }
            self.body.extend_from_slice(&bytes[..size]);
        }
        Ok(())
    }

    pub fn consume_sse(&mut self, events: &mut Vec<Value>) -> Result<String> {
        if self.status != 200
            || !self
                .header("content-type")
                .is_some_and(|value| value.starts_with("text/event-stream"))
        {
            self.read_body()?;
            return Err(format!(
                "expected SSE, HTTP {}: {}",
                self.status,
                String::from_utf8_lossy(&self.body)
            )
            .into());
        }
        consume_sse(&mut self.reader, events)
    }
}

fn consume_sse(reader: &mut impl BufRead, events: &mut Vec<Value>) -> Result<String> {
    let mut data = Vec::new();
    let mut text = String::new();
    let mut total = 0;
    loop {
        let Some(line) = line(reader, BODY_LIMIT)? else {
            return Err(format!("SSE closed before terminal event; partial data: {data:?}").into());
        };
        total += line.len() + 1;
        if total > BODY_LIMIT {
            return Err("SSE exceeds smoke limit".into());
        }
        if line.is_empty() {
            if data.is_empty() {
                continue;
            }
            let payload = data.join("\n");
            data.clear();
            let event: Value = serde_json::from_str(&payload)
                .map_err(|error| format!("malformed SSE data {payload:?}: {error}"))?;
            if events.len() >= 4096 {
                return Err("too many SSE events".into());
            }
            events.push(event.clone());
            if let Some(error) = event.get("error") {
                return Err(format!("OCG SSE error: {error}").into());
            }
            if let Some(delta) = event.get("delta").and_then(Value::as_str) {
                text.push_str(delta);
            }
            if event.get("done") == Some(&Value::Bool(true)) {
                return Ok(text);
            }
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value).to_owned());
        } else if line == "data" {
            data.push(String::new());
        }
    }
}

#[test]
fn shared_sse_handles_fragmented_events_and_reports_malformed_or_closed_streams() -> Result<()> {
    let wire = b": heartbeat\r\nid: 1\r\ndata: {\"delta\":\r\ndata: \"first\"}\r\n\r\ndata: {\"delta\":\"\\u4e2d\"}\n\ndata: {\"done\":true}\n\n";
    let mut reader = BufReader::with_capacity(1, io::Cursor::new(wire));
    let mut events = Vec::new();
    assert_eq!(consume_sse(&mut reader, &mut events)?, "first中");
    assert_eq!(events.len(), 3);
    for (wire, diagnostic) in [
        ("data: not-json\n\n", "malformed SSE data"),
        (
            "data: {\"delta\":\"partial\"}\n\n",
            "SSE closed before terminal event",
        ),
        ("data: {\"delta\":", "stream closed inside a line"),
        ("data: {\"error\":\"fixture failure\"}\n\n", "OCG SSE error"),
    ] {
        let mut reader = BufReader::with_capacity(2, io::Cursor::new(wire));
        let error = consume_sse(&mut reader, &mut Vec::new())
            .expect_err("invalid SSE unexpectedly accepted");
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
    assert!(line(&mut io::Cursor::new("oversized\n"), 3).is_err());
    Ok(())
}

#[test]
fn shared_http_deadline_bounds_an_unresponsive_peer() -> Result<()> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))?;
    let deadline = Instant::now() + super::harness::Timeouts::default().readiness_probe;
    // The OS accepts the connection while this peer deliberately sends nothing.
    let result = Response::open(listener.local_addr()?, "GET", "/", &[], None, deadline);
    let error = match result {
        Ok(_) => return Err("unresponsive HTTP peer unexpectedly answered".into()),
        Err(error) => error,
    };
    let io_error = error
        .downcast_ref::<io::Error>()
        .ok_or("timeout was not an IO error")?;
    assert!(
        matches!(
            io_error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ),
        "{error}"
    );
    assert!(remaining(deadline).is_err());
    Ok(())
}
