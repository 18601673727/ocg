use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError, Weak};

#[derive(Default)]
pub(crate) struct Notifications {
    listeners: Mutex<Vec<Weak<flume::Sender<()>>>>,
}

pub(crate) struct Subscription {
    _sender: Arc<flume::Sender<()>>,
    receiver: flume::Receiver<()>,
}

impl Notifications {
    pub(crate) fn subscribe(&self) -> Subscription {
        let (sender, receiver) = flume::bounded(1);
        let sender = Arc::new(sender);
        let mut listeners = self
            .listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        listeners.retain(|listener| listener.strong_count() > 0);
        listeners.push(Arc::downgrade(&sender));
        Subscription {
            _sender: sender,
            receiver,
        }
    }

    pub(crate) fn publish(&self) {
        self.listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|listener| {
                let Some(sender) = listener.upgrade() else {
                    return false;
                };
                match sender.try_send(()) {
                    Ok(()) | Err(flume::TrySendError::Full(())) => true,
                    Err(flume::TrySendError::Disconnected(())) => false,
                }
            });
    }
}

#[derive(Default)]
pub(crate) struct KeyedNotifications<K> {
    listeners: Mutex<Vec<(K, Weak<flume::Sender<()>>)>>,
}

impl<K: Copy + Eq> KeyedNotifications<K> {
    pub(crate) fn subscribe(&self, key: K) -> Subscription {
        let (sender, receiver) = flume::bounded(1);
        let sender = Arc::new(sender);
        let mut listeners = self
            .listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        listeners.retain(|(_, listener)| listener.strong_count() > 0);
        listeners.push((key, Arc::downgrade(&sender)));
        Subscription {
            _sender: sender,
            receiver,
        }
    }

    pub(crate) fn publish(&self, key: K) {
        self.listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(listener_key, listener)| {
                let Some(sender) = listener.upgrade() else {
                    return false;
                };
                if *listener_key != key {
                    return true;
                }
                match sender.try_send(()) {
                    Ok(()) | Err(flume::TrySendError::Full(())) => true,
                    Err(flume::TrySendError::Disconnected(())) => false,
                }
            });
    }
}

impl Subscription {
    pub(crate) async fn changed(&self) -> Result<(), flume::RecvError> {
        self.receiver.recv_async().await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct JournalCursor {
    pub(crate) generation: uuid::Uuid,
    pub(crate) sequence: u64,
}

struct JournalState<T> {
    generation: uuid::Uuid,
    sequence: u64,
    entries: VecDeque<(u64, T)>,
}

pub(crate) struct Journal<T> {
    state: Mutex<JournalState<T>>,
    changes: Notifications,
}

impl<T: Copy> Default for Journal<T> {
    fn default() -> Self {
        Self {
            state: Mutex::new(JournalState {
                generation: uuid::Uuid::now_v7(),
                sequence: 0,
                entries: VecDeque::new(),
            }),
            changes: Notifications::default(),
        }
    }
}

impl<T: Copy> Journal<T> {
    const CAPACITY: usize = 4096;

    pub(crate) fn subscribe(&self) -> Subscription {
        self.changes.subscribe()
    }

    pub(crate) fn cursor(&self) -> JournalCursor {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        JournalCursor {
            generation: state.generation,
            sequence: state.sequence,
        }
    }

    pub(crate) fn reset(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.generation = uuid::Uuid::now_v7();
        state.sequence = 0;
        state.entries.clear();
        drop(state);
        self.changes.publish();
    }

    pub(crate) fn publish(&self, entry: T) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(sequence) = state.sequence.checked_add(1) else {
            drop(state);
            self.reset();
            self.publish(entry);
            return;
        };
        state.sequence = sequence;
        state.entries.push_back((sequence, entry));
        if state.entries.len() > Self::CAPACITY {
            state.entries.pop_front();
        }
        drop(state);
        self.changes.publish();
    }

    pub(crate) fn after(&self, cursor: JournalCursor) -> Result<Vec<(JournalCursor, T)>, ()> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if cursor.generation != state.generation
            || cursor.sequence > state.sequence
            || state
                .entries
                .front()
                .is_some_and(|(sequence, _)| cursor.sequence < sequence.saturating_sub(1))
        {
            return Err(());
        }
        Ok(state
            .entries
            .iter()
            .filter(|(sequence, _)| *sequence > cursor.sequence)
            .take(32)
            .map(|(sequence, entry)| {
                (
                    JournalCursor {
                        generation: state.generation,
                        sequence: *sequence,
                    },
                    *entry,
                )
            })
            .collect())
    }
}
