/// The id counters of `sequence`. Each allocates upward from 1, and a
/// committed id is never reused.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Sequence {
    Job,
    Config,
    Attachment,
    Project,
}

impl Sequence {
    pub(super) fn key(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Config => "config",
            Self::Attachment => "attachment",
            Self::Project => "project",
        }
    }
}
