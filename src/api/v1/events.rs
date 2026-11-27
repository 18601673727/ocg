use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::contracts::envelope;
use super::jobs::{JobView, mutable_job_view};
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
use crate::db::entities::NativeJobItem;
use crate::db::entities::{JobId, JobOutputRecord};

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
    event_seq: String,
    settlement: super::jobs::SettlementView,
    stop_confirmed_at_us: Option<i64>,
}

const LAST_EVENT_ID: &str = "last-event-id";

const PAGE: usize = 16;

const PIECE: usize = 4 * 1024;

#[derive(Clone, Copy, Debug)]
pub(super) struct StreamLimits {
    pub(super) streams: usize,
    pub(super) heartbeat: Duration,
    pub(super) stall: Duration,
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
    let record = database.native_job(job).await?.ok_or(JOB_NOT_FOUND)?;
    let (page, pieces) = database
        .job_outputs_with_pieces(job, after_seq, PAGE, Default::default())
        .await?;

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
        pieces,
        record,
        announced: None,
        ended: false,
        opened: now,
        written: now,
        _slot: slot,
    };
    Ok(response(
        io,
        now,
        shared.stream_limits.stall,
        feed,
        Feed::next,
    ))
}

pub(super) fn response<F, N>(
    io: Option<IoRef>,
    now: Instant,
    stall: Duration,
    feed: F,
    advance: fn(F) -> N,
) -> HttpResponse
where
    F: Unpin + 'static,
    N: Future<Output = Option<(Bytes, F)>> + 'static,
{
    let polled = Rc::new(Cell::new(now));
    let watched = io.map(|io| watch(io, polled.clone(), stall));
    HttpResponse::builder(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .streaming(Body {
            feed: Some(feed),
            advance,
            next: None,
            rest: None,
            polled,
            _watched: watched,
        })
}

pub(super) struct Slot(Arc<Shared>);

impl Slot {
    pub(super) fn take(shared: &Arc<Shared>) -> Result<Self, Refusal> {
        shared
            .streams
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |open| {
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

fn watch(io: IoRef, polled: Rc<Cell<Instant>>, stall: Duration) -> flume::Sender<()> {
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

struct Body<F, N> {
    feed: Option<F>,
    advance: fn(F) -> N,
    next: Option<Pin<Box<N>>>,
    rest: Option<(Bytes, F)>, // Finish emitting this frame before polling the feed again.
    polled: Rc<Cell<Instant>>,
    _watched: Option<flume::Sender<()>>,
}

impl<F, N> Body<F, N> {
    fn piece(&mut self, mut bytes: Bytes, feed: F) -> Bytes {
        if bytes.len() <= PIECE {
            self.feed = Some(feed);
            return bytes;
        }
        let piece = bytes.split_to(PIECE);
        self.rest = Some((bytes, feed));
        piece
    }
}

impl<F: Unpin, N: Future<Output = Option<(Bytes, F)>>> Stream for Body<F, N> {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polled.set(Instant::now());
        if let Some((bytes, feed)) = self.rest.take() {
            return Poll::Ready(Some(Ok(self.piece(bytes, feed))));
        }
        if self.next.is_none() {
            let Some(feed) = self.feed.take() else {
                return Poll::Ready(None);
            };
            self.next = Some(Box::pin((self.advance)(feed)));
        }
        let Some(next) = self.next.as_mut() else {
            return Poll::Ready(None);
        };
        match next.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(value) => {
                self.next = None;
                Poll::Ready(value.map(|(bytes, feed)| Ok(self.piece(bytes, feed))))
            }
        }
    }
}

struct Feed {
    shared: Arc<Shared>,
    changes: crate::support::notification::Subscription,
    outputs: crate::support::notification::Subscription,
    job: JobId,
    cursor: Option<i64>,
    record: NativeJobItem,
    pending: VecDeque<JobOutputRecord>,
    pieces: crate::db::PieceReader,
    drained: bool,
    announced: Option<Lifecycle>,
    ended: bool,
    opened: Instant,
    written: Instant,
    _slot: Slot,
}

type Lifecycle = (String, [Option<i64>; 4]);

impl Feed {
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
                let pieces = std::mem::take(&mut self.pieces);
                match database
                    .job_outputs_with_pieces(self.job, self.cursor, PAGE, pieces)
                    .await
                {
                    Ok((page, pieces)) => {
                        self.pieces = pieces;
                        self.drained = page.len() < PAGE;
                        self.pending = page.into();
                        continue;
                    }
                    Err(error) => return self.fail(Refusal::from(error)),
                }
            } else if self.announced != Some(self.view().lifecycle()) {
                return self.announce();
            } else if self.view().concluded() {
                return self.settle();
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
            match database.native_job(self.job).await {
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

    fn view(&self) -> JobView {
        mutable_job_view(
            &self.record.record,
            self.record.allocation.as_ref(),
            self.record.updated_at_us,
        )
    }

    fn announce(mut self) -> Option<(Bytes, Self)> {
        let described = self.view();
        self.announced = Some(described.lifecycle());
        let data = envelope(serde_json::json!(JobEventView { job: described }));
        self.send(event("job", &data))
    }

    fn settle(mut self) -> Option<(Bytes, Self)> {
        let described = self.view();
        self.ended = true;
        let data = envelope(serde_json::json!(SettlementEventView {
            job: self.job.to_string(),
            event_seq: described.event_seq().to_owned(),
            settlement: described.settlement(),
            stop_confirmed_at_us: described.stop_confirmed_at_us(),
        }));
        self.send(event("settlement", &data))
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

fn event(name: &str, data: &Value) -> Bytes {
    Bytes::from(format!("event: {name}\ndata: {data}\n\n"))
}

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
