mod lib;
use crate::lib::{run_example, stop_example};
use tokio_cron_scheduler::{
    JobScheduler, SqliteMetadataStore, SqliteNotificationStore, SimpleJobCode,
    SimpleNotificationCode,
};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");

    unsafe {
        std::env::set_var("SQLITE_URL", "./sqlite-store-example.db");
        std::env::set_var("SQLITE_INIT_METADATA", "true");
        std::env::set_var("SQLITE_INIT_NOTIFICATIONS", "true");
    }

    let metadata_storage = Box::new(SqliteMetadataStore::default());
    let notification_storage = Box::new(SqliteNotificationStore::default());
    if std::env::var("SQLITE_INIT_METADATA").is_err() {
        info!("Set to not initialize the job metadata tables. SQLITE_INIT_METADATA=false");
    }
    if std::env::var("SQLITE_INIT_NOTIFICATIONS").is_err() {
        info!(
            "Set to not initialization of notification tables. SQLITE_INIT_NOTIFICATIONS=false"
        );
    }

    let simple_job_code = Box::new(SimpleJobCode::default());
    let simple_notification_code = Box::new(SimpleNotificationCode::default());

    let mut sched = JobScheduler::new_with_storage_and_code(
        metadata_storage,
        notification_storage,
        simple_job_code,
        simple_notification_code,
        1000
    )
    .await
    .unwrap();

    let jobs = run_example(&mut sched)
        .await
        .expect("Could not run example");
    stop_example(&mut sched, jobs)
        .await
        .expect("Could not stop example");
}
