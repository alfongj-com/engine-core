//! Bounded timing labels. No request IDs, payloads, Redis URLs or wallet labels.
use prometheus::{HistogramOpts, HistogramTimer, HistogramVec, Registry};
use std::{sync::LazyLock, time::Duration};

static DURATIONS: LazyLock<HistogramVec> = LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "tw_engine_journal_duration_seconds",
            "Journal wait and service time; SQL execution includes commit time. Canceled waits are included.",
        )
        .buckets(vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0,
            300.0,
        ]),
        &["operation", "phase"],
    )
    .expect("static journal metric definition")
});

pub fn register(registry: &Registry) -> Result<(), prometheus::Error> {
    registry.register(Box::new(DURATIONS.clone()))
}

pub(super) fn timer(operation: &'static str, phase: &'static str) -> HistogramTimer {
    DURATIONS.with_label_values(&[operation, phase]).start_timer()
}

pub(super) fn observe(operation: &'static str, phase: &'static str, elapsed: Duration) {
    DURATIONS
        .with_label_values(&[operation, phase])
        .observe(elapsed.as_secs_f64());
}
