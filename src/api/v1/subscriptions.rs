use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Instant;

use ntex::http::header;
use ntex::io::IoRef;
use ntex::util::{Bytes, Stream};
use ntex::web::HttpResponse;
use serde_json::{Value, json};

use super::{
    INVALID_REQUEST, Refusal, SHUTTING_DOWN, Shared, catalog, dashboard, events, projects,
};
use crate::db::entities::{JobId, ProjectId};
use crate::system::notification::JournalCursor;
use crate::system::notification::Subscription;

pub(super) fn change_cursor(query: &str) -> Result<Option<JournalCursor>, Refusal> {
    if query.is_empty() {
        return Ok(None);
    }
    let value = query.strip_prefix("since=").ok_or(INVALID_REQUEST)?;
    let (generation, sequence) = value.split_once('.').ok_or(INVALID_REQUEST)?;
    Ok(Some(JournalCursor {
        generation: uuid::Uuid::parse_str(generation).map_err(|_| INVALID_REQUEST)?,
        sequence: super::decimal(sequence).ok_or(INVALID_REQUEST)?,
    }))
}

fn format_change_cursor(cursor: JournalCursor) -> String {
    format!("{}.{}", cursor.generation, cursor.sequence)
}

pub(super) struct SidebarQuery {
    children: Vec<(JobId, usize)>,
}

impl SidebarQuery {
    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut children = Vec::new();
        if !query.is_empty() {
            for pair in query.split('&') {
                let ("child", value) = pair.split_once('=').ok_or(INVALID_REQUEST)? else {
                    return Err(INVALID_REQUEST);
                };
                let (id, pages) = value.split_once(':').ok_or(INVALID_REQUEST)?;
                let id = super::routing::job_id(id)?;
                let pages = super::decimal::<usize>(pages)
                    .filter(|pages| (1..=100).contains(pages))
                    .ok_or(INVALID_REQUEST)?;
                if children.len() == 64 || children.iter().any(|(known, _)| *known == id) {
                    return Err(INVALID_REQUEST);
                }
                children.push((id, pages));
            }
        }
        Ok(Self { children })
    }
}

pub(super) struct JobsQuery {
    terms: Vec<String>,
    limit: usize,
}

impl JobsQuery {
    pub(super) fn parse(query: &str) -> Result<Self, Refusal> {
        let mut raw = None;
        let mut limit = None;
        if !query.is_empty() {
            for pair in query.split('&') {
                let (key, value) = pair.split_once('=').ok_or(INVALID_REQUEST)?;
                match key {
                    "query" if raw.is_none() => raw = Some(value),
                    "limit" if limit.is_none() => {
                        limit = Some(
                            super::decimal::<usize>(value)
                                .filter(|limit| (1..=100).contains(limit))
                                .ok_or(INVALID_REQUEST)?,
                        );
                    }
                    _ => return Err(INVALID_REQUEST),
                }
            }
        }
        let terms = match raw {
            Some(raw) => catalog::conversation_search_query(&format!("query={raw}"))?.0,
            None => Vec::new(),
        };
        Ok(Self {
            terms,
            limit: limit.unwrap_or(50),
        })
    }
}

pub(super) enum Topic {
    Sidebar(SidebarQuery),
    Jobs(JobsQuery),
    Usage(dashboard::ObservationQuery),
    Availability(dashboard::ObservationQuery),
    Logs(dashboard::LogQuery),
    Catalog,
    ObservationChanges(Option<JournalCursor>),
}

enum Page {
    Projects(Option<ProjectId>),
    Conversations(Option<JobId>),
    Children {
        index: usize,
        before: Option<JobId>,
        page: usize,
    },
    Jobs {
        before: Option<JobId>,
    },
    Usage {
        after: Option<String>,
    },
    Availability {
        after: Option<String>,
    },
    Logs,
    Catalog,
    ObservationChanges,
    Wait,
}

impl Topic {
    fn first(&self) -> Page {
        match self {
            Self::Sidebar(_) => Page::Projects(None),
            Self::Jobs(_) => Page::Jobs { before: None },
            Self::Usage(_) => Page::Usage { after: None },
            Self::Availability(_) => Page::Availability { after: None },
            Self::Logs(_) => Page::Logs,
            Self::Catalog => Page::Catalog,
            Self::ObservationChanges(_) => Page::ObservationChanges,
        }
    }
}

pub(super) fn open(
    shared: &Arc<Shared>,
    io: Option<IoRef>,
    topic: Topic,
) -> Result<HttpResponse, Refusal> {
    if shared.closing.load(Ordering::SeqCst) {
        return Err(SHUTTING_DOWN);
    }
    let slot = events::Slot::take(shared)?;
    // Subscribe before the first read so a commit racing that read cannot be lost.
    let observation_cursor = match &topic {
        Topic::ObservationChanges(since) => {
            Some(since.unwrap_or_else(|| shared.database.observation_cursor()))
        }
        Topic::Sidebar(_)
        | Topic::Jobs(_)
        | Topic::Usage(_)
        | Topic::Availability(_)
        | Topic::Logs(_)
        | Topic::Catalog => None,
    };
    let changes = match &topic {
        Topic::Sidebar(_)
        | Topic::Jobs(_)
        | Topic::Usage(_)
        | Topic::Availability(_)
        | Topic::Catalog => shared.database.subscribe_catalog(),
        Topic::Logs(_) => shared.database.subscribe_logs(),
        Topic::ObservationChanges(_) => shared.database.subscribe_observations(),
    };
    let now = Instant::now();
    let feed = Feed {
        shared: shared.clone(),
        changes,
        page: topic.first(),
        topic,
        opened: now,
        ended: false,
        observation_cursor,
        observation_ready: false,
        _slot: slot,
    };
    let polled = Rc::new(Cell::new(now));
    let watched = io.map(|io| events::watch(io, polled.clone(), shared.stream_limits.stall));
    Ok(HttpResponse::Ok()
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

struct Feed {
    shared: Arc<Shared>,
    changes: Subscription,
    topic: Topic,
    page: Page,
    opened: Instant,
    ended: bool,
    observation_cursor: Option<JournalCursor>,
    observation_ready: bool,
    _slot: events::Slot,
}

impl Feed {
    async fn snapshot(&mut self) -> Result<Value, Refusal> {
        let page = std::mem::replace(&mut self.page, Page::Wait);
        let (topic, key, first, last, value) = match page {
            Page::Projects(before) => {
                let reply = projects::list(&self.shared.database, before, 100).await?;
                let next = cursor(&reply.body)?;
                self.page = match next {
                    Some(id) => Page::Projects(Some(ProjectId::new(id.get()))),
                    None => Page::Conversations(None),
                };
                (
                    "projects",
                    None,
                    before.is_none(),
                    next.is_none(),
                    reply.body,
                )
            }
            Page::Conversations(before) => {
                let reply = catalog::conversations(&self.shared.database, before, 100).await?;
                let next = cursor(&reply.body)?;
                self.page = match next {
                    Some(id) => Page::Conversations(Some(id)),
                    None => Page::Children {
                        index: 0,
                        before: None,
                        page: 0,
                    },
                };
                (
                    "conversations",
                    None,
                    before.is_none(),
                    next.is_none(),
                    reply.body,
                )
            }
            Page::Children {
                index,
                before,
                page,
            } => {
                let Topic::Sidebar(query) = &self.topic else {
                    return Err(super::INTERNAL);
                };
                let Some(&(id, pages)) = query.children.get(index) else {
                    return Ok(Value::Null);
                };
                let reply =
                    catalog::conversation_children(&self.shared.database, id, before, 20).await?;
                let next = cursor(&reply.body)?;
                let visible = reply
                    .body
                    .get("children")
                    .and_then(Value::as_array)
                    .ok_or(super::INTERNAL)?
                    .iter()
                    .any(|child| child.get("origin").and_then(Value::as_str) != Some("system"));
                let page = page + usize::from(visible);
                let last = next.is_none() || page >= pages;
                self.page = if last {
                    Page::Children {
                        index: index + 1,
                        before: None,
                        page: 0,
                    }
                } else {
                    Page::Children {
                        index,
                        before: next,
                        page,
                    }
                };
                (
                    "children",
                    Some(id.to_string()),
                    before.is_none(),
                    last,
                    reply.body,
                )
            }
            Page::Jobs { before } => {
                let Topic::Jobs(query) = &self.topic else {
                    return Err(super::INTERNAL);
                };
                let page = self
                    .shared
                    .database
                    .search_jobs(query.terms.clone(), before, query.limit)
                    .await?;
                let next = page.next_before;
                let jobs = page
                    .records
                    .iter()
                    .map(super::jobs::job_view)
                    .collect::<Result<Vec<_>, _>>()?;
                let body = super::contracts::envelope(json!({
                    "jobs": jobs,
                    "next_before": next.map(|id| id.to_string()),
                    "has_more": page.has_more,
                }));
                self.page = match next {
                    Some(id) => Page::Jobs { before: Some(id) },
                    None => Page::Wait,
                };
                ("jobs", None, before.is_none(), next.is_none(), body)
            }
            Page::Usage { after } => {
                let Topic::Usage(query) = &self.topic else {
                    return Err(super::INTERNAL);
                };
                let reply = dashboard::usage(&self.shared, query.page(after.clone())?).await?;
                let next = reply
                    .body
                    .get("next_after")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                self.page = match next {
                    Some(ref cursor) => Page::Usage {
                        after: Some(cursor.clone()),
                    },
                    None => Page::Wait,
                };
                ("usage", None, after.is_none(), next.is_none(), reply.body)
            }
            Page::Availability { after } => {
                let Topic::Availability(query) = &self.topic else {
                    return Err(super::INTERNAL);
                };
                let reply =
                    dashboard::availability(&self.shared, query.page(after.clone())?).await?;
                let next = reply
                    .body
                    .get("next_after")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                self.page = match next {
                    Some(ref cursor) => Page::Availability {
                        after: Some(cursor.clone()),
                    },
                    None => Page::Wait,
                };
                (
                    "availability",
                    None,
                    after.is_none(),
                    next.is_none(),
                    reply.body,
                )
            }
            Page::Logs => {
                let Topic::Logs(query) = &self.topic else {
                    return Err(super::INTERNAL);
                };
                let reply = dashboard::logs(&self.shared, query.clone()).await?;
                ("logs", None, true, true, reply.body)
            }
            Page::Catalog => ("catalog", None, true, true, json!({ "changed": true })),
            Page::ObservationChanges => {
                let Some(mut cursor) = self.observation_cursor else {
                    return Err(super::INTERNAL);
                };
                if !self.observation_ready {
                    self.observation_ready = true;
                    self.page = Page::ObservationChanges;
                    (
                        "observation_changes",
                        Some(format_change_cursor(cursor)),
                        true,
                        true,
                        super::contracts::envelope(json!({"usage": [], "availability": []})),
                    )
                } else {
                    let changes =
                        self.shared
                            .database
                            .observation_changes(cursor)
                            .map_err(|()| {
                                Refusal::new(
                                    ntex::http::StatusCode::SERVICE_UNAVAILABLE,
                                    "observation_replay_lost",
                                    "observation changes need resynchronization",
                                )
                            })?;
                    if changes.is_empty() {
                        self.page = Page::Wait;
                        return Ok(Value::Null);
                    }
                    let mut usage = Vec::new();
                    let mut availability = Vec::new();
                    for (next, change) in changes {
                        if let Some(record) =
                            dashboard::changed_observation(&self.shared, change).await?
                        {
                            match change {
                                crate::db::ObservationChange::Metrics(_, _) => usage.push(record),
                                crate::db::ObservationChange::Availability(_, _) => {
                                    availability.push(record)
                                }
                            }
                        }
                        cursor = next;
                    }
                    self.observation_cursor = Some(cursor);
                    self.page = Page::ObservationChanges;
                    (
                        "observation_changes",
                        Some(format_change_cursor(cursor)),
                        false,
                        true,
                        super::contracts::envelope(
                            json!({"usage": usage, "availability": availability}),
                        ),
                    )
                }
            }
            Page::Wait => return Err(super::INTERNAL),
        };
        Ok(json!({ "topic": topic, "key": key, "first": first, "last": last, "value": value }))
    }

    async fn next(mut self) -> Option<(Bytes, Self)> {
        loop {
            if self.ended
                || self.shared.closing.load(Ordering::SeqCst)
                || self.opened.elapsed() >= self.shared.stream_limits.lifetime
            {
                return None;
            }
            if matches!(self.page, Page::Wait) {
                match ntex::time::timeout(
                    self.shared.stream_limits.heartbeat,
                    self.changes.changed(),
                )
                .await
                {
                    Ok(Ok(())) => self.page = self.topic.first(),
                    Ok(Err(_)) => return None,
                    Err(_) => return Some((Bytes::from_static(b": heartbeat\n\n"), self)),
                }
            }
            match self.snapshot().await {
                Ok(Value::Null) => continue,
                Ok(value) => {
                    let bytes = match super::wire::bounded_json(&value, self.shared.response_bytes)
                    {
                        Ok(bytes) => bytes,
                        Err(refusal) => return self.fail(refusal),
                    };
                    let mut frame = Vec::with_capacity(bytes.len() + 24);
                    frame.extend_from_slice(b"event: snapshot\ndata: ");
                    frame.extend_from_slice(&bytes);
                    frame.extend_from_slice(b"\n\n");
                    return Some((Bytes::from(frame), self));
                }
                Err(refusal) => return self.fail(refusal),
            }
        }
    }
    fn fail(mut self, refusal: Refusal) -> Option<(Bytes, Self)> {
        self.ended = true;
        let value = json!({ "error": { "code": refusal.code, "message": refusal.message } });
        Some((
            Bytes::from(format!("event: stream_error\ndata: {value}\n\n")),
            self,
        ))
    }
}

fn cursor(value: &Value) -> Result<Option<JobId>, Refusal> {
    if value.get("has_more").and_then(Value::as_bool) != Some(true) {
        return Ok(None);
    }
    let next = value
        .get("next_before")
        .and_then(Value::as_str)
        .ok_or(super::INTERNAL)?;
    super::routing::job_id(next).map(Some)
}

type Next = Pin<Box<dyn Future<Output = Option<(Bytes, Feed)>>>>;
struct Body {
    feed: Option<Feed>,
    next: Option<Next>,
    rest: Option<(Bytes, Feed)>,
    polled: Rc<Cell<Instant>>,
    _watched: Option<flume::Sender<()>>,
}

impl Body {
    fn piece(&mut self, mut bytes: Bytes, feed: Feed) -> Bytes {
        if bytes.len() <= events::PIECE {
            self.feed = Some(feed);
            return bytes;
        }
        let piece = bytes.split_to(events::PIECE);
        self.rest = Some((bytes, feed));
        piece
    }
}

impl Stream for Body {
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
            self.next = Some(Box::pin(feed.next()));
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
