use super::*;

impl DatabaseHandle {
    pub async fn tool_stats(
        &self,
    ) -> Result<Vec<crate::db::entities::ToolStatRecord>, DatabaseError> {
        self.read(redb::read_tool_stats).await
    }

    pub async fn usage_summary(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        after: Option<crate::db::entities::UsageCursor>,
    ) -> Result<crate::db::entities::UsageSummaryResponse, DatabaseError> {
        self.read(move |database| redb::read_usage_summary(database, &filter, after))
            .await
    }

    pub async fn usage_buckets(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        bucketing: crate::db::entities::UsageBucketing,
        after: Option<crate::db::entities::UsageCursor>,
    ) -> Result<crate::db::entities::UsageBucketResponse, DatabaseError> {
        self.read(move |database| redb::read_usage_buckets(database, &filter, &bucketing, after))
            .await
    }

    pub(crate) async fn usage_retries(
        &self,
        filter: crate::db::entities::UsageSummaryFilter,
        bucketing: crate::db::entities::UsageBucketing,
        after: Option<(u128, u128)>,
        limit: usize,
    ) -> Result<crate::db::entities::UsageRetriesPage, DatabaseError> {
        self.read(move |database| {
            redb::read_usage_retries(database, &filter, bucketing, after, limit)
        })
        .await
    }

    pub async fn job_metrics(
        &self,
        job: JobId,
        after: Option<MetricsId>,
        limit: usize,
    ) -> Result<Vec<MetricsRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_metrics(database, job, after, limit))
            .await
    }

    pub async fn job_availability(
        &self,
        job: JobId,
        after: Option<AvailabilityStatId>,
        limit: usize,
    ) -> Result<Vec<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| redb::read_job_availability(database, job, after, limit))
            .await
    }

    pub async fn metrics(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor>,
        limit: usize,
    ) -> Result<ObservationPage<MetricsRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_metrics(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }

    pub async fn availability_stats(
        &self,
        filter: ObservationFilter,
        after: Option<ObservationCursor>,
        limit: usize,
    ) -> Result<ObservationPage<AvailabilityStatRecord>, DatabaseError> {
        self.read(move |database| {
            redb::read_availability_stats(database, &filter, after, limit, MAX_OBSERVATION_SCAN)
        })
        .await
    }
}
