//! A Job's outputs as Server-Sent Events: `GET /api/v1/jobs/{job}/events`.
//!
//! The stream carries the Job's stored outputs, read from the database as
//! [`DatabaseHandle::job_outputs`] returns them, and nothing else that could
//! be taken for one: it keeps no history of its own, numbers nothing and
//! never writes. A client that loses it resumes from the last `seq` it
//! processed, with `Last-Event-ID` or `?after_seq=`, and receives every
//! later output exactly as stored, from any process that opens the same
//! database. Delivery across reconnections is at least once: a client
//! deduplicates by Job and `seq`. Within one stream no output is sent
//! twice, skipped or reordered.
//!
//! Events, each one complete JSON object on one `data` line:
//!
//! - `job`, without an `id`: the Job's record as [`super::jobs::job_view`]
//!   describes it, when the stream opens and again whenever its lifecycle
//!   changes without concluding. It is an observation, not an output.
//! - `output`, with the output's stored `seq` as its `id`: `{"api_version",
//!   "job", "output"}`, the output as the outputs route describes it.
//! - `settlement`, without an `id`, last: `{"api_version", "job",
//!   "settlement", "stop_confirmed_at_us"}`, sent once every output has
//!   been sent and the record shows the Job concluded: completed, failed,
//!   cancelled, or unknown with its local execution confirmed stopped. An
//!   unknown outcome whose stop is not confirmed has not concluded: its
//!   outputs can no longer change, but its execution may still run, so the
//!   stream reports it as a `job` and waits on.
//! - `stream_error`, without an `id`, last: `{"api_version", "error"}`
//!   with a fixed code and message, when the stream cannot go on: an output
//!   too large to send, which is never cut or skipped, or a failed read.
//! - `: heartbeat` comments while nothing else is sent.
//!
//! A stream that ends without `settlement` or `stream_error`, because the
//! listener stops or the stream reached its lifetime, is resumed from its
//! cursor.
//!
//! Correctness rests on two facts of the record. An output's `seq` is one
//! more than the Job's previous output's, in the transaction that stores
//! it, so every read sees a gapless prefix of them. Once a Job has any
//! outcome, unknown included, no output is added to it; a final result is
//! stored with its outcome. The stream therefore reads the Job before its
//! outputs: when that record has an outcome, the outputs read after it are
//! all the Job will ever have, and once they are sent, the settlement is
//! true.
//!
//! The stream is read only as the connection takes it: no output is read
//! before the client has taken the previous one, at most one page of
//! outputs is held, and a client that takes nothing for a bounded time is
//! disconnected. Disconnecting never touches the Job.

use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::contracts::*;
use super::jobs::{job_view, settlement_view};
use super::outputs::output_view;
use ntex::http::StatusCode;
use ntex::http::header::{self, HeaderMap};
use ntex::io::IoRef;
use ntex::util::{Bytes, Stream};
use ntex::web::HttpResponse;
use serde_json::Value;

use super::{
    INVALID_REQUEST, JOB_NOT_FOUND, OUTPUT_TOO_LARGE, Refusal, SHUTTING_DOWN, Shared, decimal,
};
use crate::db::entities::{JobId, JobOutputRecord, JobRecord, Timestamp};

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "JobEvent")]
pub(crate) struct JobEventView {
    job: super::jobs::JobView,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "OutputEvent")]
pub(crate) struct OutputEventView {
    job: String,
    output: super::outputs::OutputView,
}

#[derive(serde::Serialize, ts_rs::TS)]
#[ts(rename = "SettlementEvent")]
pub(crate) struct SettlementEventView {
    job: String,
    settlement: super::jobs::SettlementView,
    stop_confirmed_at_us: Option<i64>,
}

/// The request header a reconnecting client names its cursor with.
const LAST_EVENT_ID: &str = "last-event-id";

/// The most outputs read, and held, at once.
const PAGE: usize = 16;

/// Verified 4 KiB mitigation for confirmed HTTP/1.1 streamed-response corruption
/// with ntex 3.12.3 / ntex-bytes 1.11.0; ntex-bytes 1.12.0 also reproduces it.
/// This is not an upstream fix. https://github.com/ntex-rs/ntex/issues/1121
/// currently requests a private security-reporting channel, not confirmed fix
/// tracking. Do not remove on dependency upgrades: removal requires a verified
/// upstream fix and successful raw-wire regression testing.
pub(super) const PIECE: usize = 4 * 1024;

/// The bounds of every stream.
#[derive(Clone, Copy, Debug)]
pub(super) struct StreamLimits {
    /// The most streams open at once.
    pub(super) streams: usize,
    /// How long a stream stays silent before a heartbeat.
    pub(super) heartbeat: Duration,
    /// How long a client may take nothing before it is disconnected.
    pub(super) stall: Duration,
    /// How long one stream lasts at most.
    pub(super) lifetime: Duration,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            streams: 64,
            heartbeat: Duration::from_secs(15),
            stall: Duration::from_secs(30),
            lifetime: Duration::from_secs(30 * 60),
        }
    }
}

pub(super) const INVALID_CURSOR: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "invalid_cursor",
    "a replay cursor must be one non-negative decimal sequence number",
);
pub(super) const AMBIGUOUS_CURSOR: Refusal = Refusal::new(
    StatusCode::BAD_REQUEST,
    "ambiguous_cursor",
    "a replay cursor is given by Last-Event-ID or after_seq, not both",
);
pub(super) const TOO_MANY_STREAMS: Refusal = Refusal::new(
    StatusCode::SERVICE_UNAVAILABLE,
    "too_many_streams",
    "too many event streams are open; retry later",
);

/// The request's cursor: `after_seq`, the only query, or its one
/// `Last-Event-ID`; neither is the start.
pub(super) fn cursor(query: &str, headers: &HeaderMap) -> Result<Option<i64>, Refusal> {
    let queried = match query {
        "" => None,
        query => {
            let value = query.strip_prefix("after_seq=").ok_or(INVALID_REQUEST)?;
            Some(decimal::<i64>(value).ok_or(INVALID_CURSOR)?)
        }
    };
    let mut values = headers.get_all(LAST_EVENT_ID);
    let named = match (values.next(), values.next()) {
        (None, _) => None,
        (Some(value), None) => Some(
            value
                .to_str()
                .ok()
                .and_then(decimal::<i64>)
                .ok_or(INVALID_CURSOR)?,
        ),
        (Some(_), Some(_)) => return Err(INVALID_CURSOR),
    };
    match (queried, named) {
        (Some(_), Some(_)) => Err(AMBIGUOUS_CURSOR),
        (cursor, None) | (None, cursor) => Ok(cursor),
    }
}

/// Opens the Job's stream after `after_seq`, or refuses it before anything
/// is sent: when the listener is stopping, too many streams are open, the
/// Job does not exist or cannot be described, or its next output is too
/// large to send.
pub(super) async fn open(
    shared: &Arc<Shared>,
    io: Option<IoRef>,
    job: JobId,
    after_seq: Option<i64>,
) -> Result<HttpResponse, Refusal> {
    if shared.closing.load(Ordering::SeqCst) {
        return Err(SHUTTING_DOWN);
    }
    let slot = Slot::take(shared)?;
    let database = &shared.database;
    let changes = database.subscribe_catalog();
    let outputs = database.subscribe_outputs(job);
    let record = database.job(job).await?.ok_or(JOB_NOT_FOUND)?;
    job_view(&record)?;
    let page = database.job_outputs(job, after_seq, PAGE).await?;

    if let Some(first) = page.first() {
        output_event(shared, job, first)?;
    }
    let now = Instant::now();
    let feed = Feed {
        shared: shared.clone(),
        changes,
        outputs,
        job,
        cursor: after_seq,
        drained: page.len() < PAGE,
        pending: page.into(),
        record,
        announced: None,
        ended: false,
        opened: now,
        written: now,
        _slot: slot,
    };
    let polled = Rc::new(Cell::new(now));
    let watched = io.map(|io| watch(io, polled.clone(), shared.stream_limits.stall));
    Ok(HttpResponse::build(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .streaming(Body {
            feed: Some(feed),
            next: None,
            rest: None,
            polled,
            _watched: watched,
        }))
}

/// One of the open streams, given back when dropped.
pub(super) struct Slot(Arc<Shared>);

impl Slot {
    // fetch_update is deprecated on 1.99; its replacement is newer than our MSRV.
    #[allow(deprecated)]
    pub(super) fn take(shared: &Arc<Shared>) -> Result<Self, Refusal> {
        shared
            .streams
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |open| {
                (open < shared.stream_limits.streams).then_some(open + 1)
            })
            .map_err(|_| TOO_MANY_STREAMS)?;
        Ok(Self(shared.clone()))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.streams.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Disconnects the client once the stream has not been taken from for the
/// stall limit: the connection only asks for more once it has written what
/// it holds. Ends with the stream.
pub(super) fn watch(io: IoRef, polled: Rc<Cell<Instant>>, stall: Duration) -> flume::Sender<()> {
    let (watched, ended) = flume::bounded::<()>(1);
    ntex::rt::spawn(async move {
        loop {
            if ntex::time::timeout(stall / 4, ended.recv_async())
                .await
                .is_ok()
            {
                return;
            }
            if polled.get().elapsed() >= stall {
                io.terminate();
                return;
            }
        }
    });
    watched
}

/// The feed's next event, being read.
type Next = Pin<Box<dyn Future<Output = Option<(Bytes, Feed)>>>>;

/// The response body: each event the feed gives, read only when the
/// connection asks for the next, and given in pieces of at most [`PIECE`]
/// bytes. The feed, and its cursor, come back once the last piece is given.
struct Body {
    feed: Option<Feed>,
    next: Option<Next>,
    /// What is left to give of the event the feed gave last.
    rest: Option<(Bytes, Feed)>,
    /// When the connection last asked.
    polled: Rc<Cell<Instant>>,
    /// Ends the stall watch when the body is dropped.
    _watched: Option<flume::Sender<()>>,
}

impl Stream for Body {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled.set(Instant::now());
        if let Some((mut event, feed)) = self.rest.take() {
            return Poll::Ready(Some(Ok(self.piece(&mut event, feed))));
        }
        if self.next.is_none() {
            let Some(feed) = self.feed.take() else {
                return Poll::Ready(None);
            };
            self.next = Some(Box::pin(feed.next()));
        }
        let Some(next) = self.next.as_mut() else {
            return Poll::Ready(None);
        };
        match next.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(item) => {
                self.next = None;
                Poll::Ready(item.map(|(mut event, feed)| Ok(self.piece(&mut event, feed))))
            }
        }
    }
}

impl Body {
    /// The next piece of `event`, keeping what is left of it, or the feed
    /// once nothing is.
    fn piece(&mut self, event: &mut Bytes, feed: Feed) -> Bytes {
        if event.len() <= PIECE {
            self.feed = Some(feed);
            return std::mem::take(event);
        }
        let piece = event.split_to(PIECE);
        self.rest = Some((std::mem::take(event), feed));
        piece
    }
}

/// What the stream knows, and how far it has sent.
struct Feed {
    shared: Arc<Shared>,
    changes: crate::system::notification::Subscription,
    outputs: crate::system::notification::Subscription,
    job: JobId,
    /// The `seq` of the last output sent, or the client's cursor.
    cursor: Option<i64>,
    /// The Job as last read, before every output in `pending` was read.
    record: JobRecord,
    /// Outputs read but not yet sent, in `seq` order.
    pending: VecDeque<JobOutputRecord>,
    /// Whether a page read after `record` came back short: every output
    /// stored before `record` was read is then sent or in `pending`.
    drained: bool,
    /// The lifecycle last sent in a `job` event.
    announced: Option<Lifecycle>,
    ended: bool,
    opened: Instant,
    /// When an event was last given to the connection.
    written: Instant,
    _slot: Slot,
}

/// What a `job` event is sent again for.
type Lifecycle = [Option<Timestamp>; 6];

fn lifecycle(record: &JobRecord) -> Lifecycle {
    [
        record.started_at,
        record.completed_at,
        record.failed_at,
        record.cancelled_at,
        record.unknown_at,
        record.stop_confirmed_at,
    ]
}

/// Whether the record shows the Job concluded: an outcome that is final,
/// or an unknown one whose local execution was confirmed stopped.
fn concluded(record: &JobRecord) -> bool {
    record.completed_at.is_some()
        || record.failed_at.is_some()
        || record.cancelled_at.is_some()
        || (record.unknown_at.is_some() && record.stop_confirmed_at.is_some())
}

impl Feed {
    /// The next event, and the feed past it; `None` once the stream ends.
    /// The cursor moves past an output only with the event that carries it.
    async fn next(mut self) -> Option<(Bytes, Self)> {
        let database = self.shared.database.clone();
        let limits = self.shared.stream_limits;
        loop {
            if self.ended
                || self.shared.closing.load(Ordering::SeqCst)
                || self.opened.elapsed() >= limits.lifetime
            {
                return None;
            }
            if self.announced.is_none() {
                return self.announce();
            }
            if let Some(output) = self.pending.pop_front() {
                return match output_event(&self.shared, self.job, &output) {
                    Ok(event) => {
                        self.cursor = Some(output.seq);
                        self.send(event)
                    }
                    Err(refusal) => self.fail(refusal),
                };
            }
            if !self.drained {
                match database.job_outputs(self.job, self.cursor, PAGE).await {
                    Ok(page) => {
                        self.drained = page.len() < PAGE;
                        self.pending = page.into();
                        continue;
                    }
                    Err(error) => return self.fail(Refusal::from(error)),
                }
            } else if concluded(&self.record) {
                return self.settle();
            } else if self.announced != Some(lifecycle(&self.record)) {
                return self.announce();
            }
            if self.written.elapsed() >= limits.heartbeat {
                return self.send(Bytes::from_static(b": heartbeat\n\n"));
            }
            match ntex::time::timeout(
                limits.heartbeat,
                ntex::util::select(self.changes.changed(), self.outputs.changed()),
            )
            .await
            {
                Ok(ntex::util::Either::Left(Ok(())) | ntex::util::Either::Right(Ok(()))) => {}
                Ok(ntex::util::Either::Left(Err(_)) | ntex::util::Either::Right(Err(_))) => {
                    return None;
                }
                Err(_) => return self.send(Bytes::from_static(b": heartbeat\n\n")),
            }
            match database.job(self.job).await {
                Ok(Some(record)) => {
                    self.record = record;
                    self.drained = false;
                }
                Ok(None) => return self.fail(JOB_NOT_FOUND),
                Err(error) => return self.fail(Refusal::from(error)),
            }
        }
    }

    fn send(mut self, event: Bytes) -> Option<(Bytes, Self)> {
        self.written = Instant::now();
        Some((event, self))
    }

    fn announce(mut self) -> Option<(Bytes, Self)> {
        match job_view(&self.record) {
            Ok(described) => {
                self.announced = Some(lifecycle(&self.record));
                let data = envelope(serde_json::json!(JobEventView { job: described }));
                self.send(event("job", &data))
            }
            Err(refusal) => self.fail(refusal),
        }
    }

    fn settle(mut self) -> Option<(Bytes, Self)> {
        match settlement_view(&self.record) {
            Ok(settlement) => {
                self.ended = true;
                let data = envelope(serde_json::json!(SettlementEventView {
                    job: self.job.to_string(),
                    settlement,
                    stop_confirmed_at_us: self
                        .record
                        .stop_confirmed_at
                        .map(|at| at.as_unix_micros()),
                }));
                self.send(event("settlement", &data))
            }
            Err(refusal) => self.fail(refusal),
        }
    }

    fn fail(mut self, refusal: Refusal) -> Option<(Bytes, Self)> {
        self.ended = true;
        let data = envelope(serde_json::json!({
            "error": serde_json::json!({
                "code": refusal.code.to_owned(),
                "message": refusal.message.to_owned(),
            }),
        }));
        self.send(event("stream_error", &data))
    }
}

/// An event without an `id`.
fn event(name: &str, data: &Value) -> Bytes {
    Bytes::from(format!("event: {name}\ndata: {data}\n\n"))
}

/// The output's event, whole, or [`OUTPUT_TOO_LARGE`] when it is larger
/// than a response may be. Such an output is larger than a page of the
/// outputs route could carry too.
fn output_event(shared: &Shared, job: JobId, output: &JobOutputRecord) -> Result<Bytes, Refusal> {
    let data = envelope(serde_json::json!(OutputEventView {
        job: job.to_string(),
        output: output_view(output),
    }));
    let event = format!("id: {}\nevent: output\ndata: {data}\n\n", output.seq);
    if event.len() > shared.response_bytes {
        return Err(OUTPUT_TOO_LARGE);
    }
    Ok(Bytes::from(event))
}
