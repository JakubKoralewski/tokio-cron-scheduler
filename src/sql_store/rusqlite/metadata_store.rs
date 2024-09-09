use crate::job::job_data_prost::{CronJob, JobType, NonCronJob};
use crate::sql_store::rusqlite::SqliteStore;
use crate::store::{DataStore, InitStore, MetaDataStorage};
use crate::{JobAndNextTick, JobSchedulerError, JobStoredData, JobUuid};
use chrono::{DateTime, Utc};
use std::convert::TryFrom;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

const TABLE: &str = "job";

#[derive(Clone)]
pub struct SqliteMetadataStore {
    pub store: Arc<RwLock<SqliteStore>>,
    pub init_tables: bool,
    pub table: String,
}

impl Default for SqliteMetadataStore {
    fn default() -> Self {
        let init_tables = std::env::var("SQLITE_INIT_METADATA")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or_default();
        let table =
            std::env::var("SQLITE_METADATA_TABLE").unwrap_or_else(|_| TABLE.to_lowercase());
        let store = Arc::new(RwLock::new(SqliteStore::default()));
        Self {
            init_tables,
            table,
            store,
        }
    }
}

impl DataStore<JobStoredData> for SqliteMetadataStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<JobStoredData>, JobSchedulerError>> + Send>>
    {
        let store = self.store.clone();
        let table = self.table.clone();
        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::GetJobData),
                SqliteStore::Inited(store) => {
                    let sql = "select \
                        id, last_updated, next_tick, last_tick, job_type, count, \
                        ran, stopped, schedule, repeating, repeated_every, \
                        extra, time_offset_seconds \
                     from "
                        .to_string()
                        + &*table
                        + " where id = $1 limit 1";
                    let row: Result<JobStoredData, _> = store.call(move |store| {
                        Ok(store.query_row(&sql, &[&id], |row| Ok(row.into()))?)
                    }).await;
                    match row {
                        Err(e) => {
                            error!("Error getting value {:?}", e);
                            return Err(JobSchedulerError::GetJobData);
                        },
                        Ok(data) => Ok(Some(data))
                    }
                }
            }
        })
    }

    fn add_or_update(
        &mut self,
        data: JobStoredData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        Box::pin(async move {
            use crate::job::job_data_prost::job_stored_data::Job::CronJob as CronJobType;
            use crate::job::job_data_prost::job_stored_data::Job::NonCronJob as NonCronJobType;

            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::UpdateJobData),
                SqliteStore::Inited(store) => {
                    let uuid: Uuid = data.id.as_ref().unwrap().into();
                    let sql = "INSERT INTO ".to_string()
                        + &*table
                        + " (\
                        id, last_updated, next_tick, job_type, count, \
                        ran, stopped, schedule, repeating, repeated_every, \
                        extra, last_tick, time_offset_seconds \
                    )\
                    VALUES (\
                        $1, $2, $3, $4, $5, \
                        $6, $7, $8, $9, $10,\
                        $11, $12, $13 \
                    )\
                    ON CONFLICT (id) \
                    DO \
                        UPDATE \
                        SET \
                            last_updated=$2, next_tick=$3, job_type=$4, count=$5, \
                            ran=$6, stopped=$7, schedule=$8, repeating=$9, repeated_every=$10, \
                            extra=$11, last_tick=$12, time_offset_seconds=$13
                    ";
                    let last_updated = data.last_updated.as_ref().map(|i| *i as i64);
                    let next_tick = data.next_tick as i64;
                    let job_type = data.job_type;
                    let count = data.count as i32;
                    let ran = data.ran;
                    let stopped = data.stopped;
                    let schedule = match data.job.as_ref() {
                        Some(CronJobType(ct)) => Some(ct.schedule.clone()),
                        _ => None,
                    };
                    let repeating = match data.job.as_ref() {
                        Some(NonCronJobType(ct)) => Some(ct.repeating),
                        _ => None,
                    };
                    let repeated_every = match data.job.as_ref() {
                        Some(NonCronJobType(ct)) => Some(ct.repeated_every as i64),
                        _ => None,
                    };
                    let extra = data.extra;
                    let last_tick = data.last_tick.as_ref().map(|i| *i as i64);
                    let time_offset_seconds = data.time_offset_seconds;

                    let val = store.call(move |store|
                        Ok(store.execute(
                            &sql,
                            tokio_rusqlite::params![
                                uuid,
                                last_updated,
                                next_tick,
                                job_type,
                                count,
                                ran,
                                stopped,
                                schedule,
                                repeating,
                                repeated_every,
                                extra,
                                last_tick,
                                time_offset_seconds,
                            ],
                        )?))
                        .await;
                    if let Err(e) = val {
                        error!("Error {:?}", e);
                        Err(JobSchedulerError::CantAdd)
                    } else {
                        Ok(())
                    }
                }
            }
        })
    }

    fn delete(
        &mut self,
        guid: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CantRemove),
                SqliteStore::Inited(store) => {

                    let sql = "DELETE FROM ".to_string() + &*table + " WHERE id = $1";
                    // let val = store.query(&*sql, &[&guid]).await;
                    let val = store.call(move |store|
                        Ok(store.execute(
                            &sql,
                            &[&guid]
                        )?)
                    ).await;
                    match val {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            error!("Error deleting job data {:?}", e);
                            Err(JobSchedulerError::CantRemove)
                        }
                    }
                }
            }
        })
    }
}

impl<'stmt> From<&tokio_rusqlite::Row<'stmt>> for JobStoredData {
    fn from(row: &tokio_rusqlite::Row<'stmt>) -> Self {
        /*
        id, last_updated, next_tick, last_tick, job_type, count, \
                        ran, stopped, schedule, repeating, repeated_every, \
                        extra, time_offset_seconds
         */
        let id: Uuid = row.get(0).unwrap();
        let last_updated = row.get(1).ok().map(|i: i64| i as u64);
        let next_tick = row
            .get(2)
            .ok()
            .map(|i: i64| i as u64)
            .unwrap_or_default();
        let last_tick = row.get(3).ok().map(|i: i64| i as u64);

        let job_type: i32 = row.get(4).unwrap_or_default();
        let count = row.get(5).unwrap_or_default();
        let ran = row.get(6).unwrap_or_default();
        let stopped = row.get(7).unwrap_or_default();
        let job = {
            use crate::job::job_data_prost::job_stored_data::Job::CronJob as CronJobType;
            use crate::job::job_data_prost::job_stored_data::Job::NonCronJob as NonCronJobType;

            let job_type = JobType::try_from(job_type);
            match job_type {
                Ok(JobType::Cron) => match row.get(8) {
                    Ok(schedule) => Some(CronJobType(CronJob { schedule })),
                    _ => None,
                },
                Ok(_) => {
                    let repeating = row.get(9).unwrap();
                    let repeated_every = row
                        .get(10)
                        .ok()
                        .map(|i: i64| i as u64)
                        .unwrap_or_default();
                    Some(NonCronJobType(NonCronJob {
                        repeating,
                        repeated_every,
                    }))
                }
                Err(_) => None,
            }
        };
        let extra = row.get(11).unwrap_or_default();
        let time_offset_seconds = row.get(12).unwrap_or_default();

        Self {
            id: Some(id.into()),
            last_updated,
            last_tick,
            next_tick,
            job_type,
            count,
            extra,
            ran,
            stopped,
            job,
            time_offset_seconds,
        }
    }
}

impl InitStore for SqliteMetadataStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let inited = self.inited();
        let store = self.store.clone();
        let init_tables = self.init_tables;
        let table = self.table.clone();
        Box::pin(async move {
            let inited = inited.await;
            if matches!(inited, Ok(false)) || matches!(inited, Err(_)) {
                let mut w = store.write().await;
                let val = w.clone();
                let val = val.init().await;
                match val {
                    Ok(v) => {
                        if init_tables {
                            if let SqliteStore::Inited(client) = &v {
                                // let v = client.read().await;
                                let sql = "CREATE TABLE IF NOT EXISTS ".to_string()
                                    + &*table
                                    + " (\
                                            id BLOB,\
                                            last_updated BIGINT,\
                                            next_tick BIGINT,\
                                            last_tick BIGINT,\
                                            job_type INTEGER NOT NULL,\
                                            count INTEGER,\
                                            ran BOOLEAN,\
                                            stopped BOOLEAN,\
                                            schedule TEXT,\
                                            repeating BOOLEAN,\
                                            repeated_every BIGINT,\
                                            time_offset_seconds INTEGER, \
                                            extra BLOB, \
                                            CONSTRAINT pk_metadata PRIMARY KEY (id) \
                                        )";
                                let create = client.call(move |store|
                                    Ok(store.execute(&sql, [])?)
                                ).await;
                                // let create = v.execute(&*sql, &[]).await;
                                if let Err(e) = create {
                                    error!("Error on init Sqlite Metadata store {:?}", e);
                                    return Err(JobSchedulerError::CantInit);
                                }
                            }
                        }
                        *w = v;
                        Ok(())
                    }
                    Err(e) => {
                        error!("Error initialising {:?}", e);
                        Err(e)
                    }
                }
            } else {
                Ok(())
            }
        })
    }

    fn inited(&mut self) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        Box::pin(async move {
            let store = store.read().await;
            Ok(store.inited())
        })
    }
}

impl MetaDataStorage for SqliteMetadataStore {
    fn list_next_ticks(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<JobAndNextTick>, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CantListNextTicks),
                SqliteStore::Inited(store) => {
                    // let store = store.read().await;
                    let now = Utc::now().timestamp();
                    let sql = "SELECT \
                            id, job_type, next_tick, last_tick \
                        FROM "
                        .to_string()
                        + &*table
                        + " \
                        WHERE \
                              next_tick > 0 \
                          AND next_tick < $1";
                    
                    let rows: Vec<JobAndNextTick> = store.call(
                        move |store| {
                            let mut stmt = store.prepare(&*sql)?;
                            let x = Ok(stmt.query_map([now], |row| {
                                let id: Uuid = row.get(0)?;
                                let id: JobUuid = id.into();
                                let job_type = row.get(1)?;
                                let next_tick = row
                                    .get(2)
                                    .ok()
                                    .map(|i: i64| i as u64)
                                    .unwrap_or_default();
                                let last_tick = row.get(3).map(|i: i64| i as u64).ok();

                                Ok(JobAndNextTick {
                                    id: Some(id),
                                    job_type,
                                    next_tick,
                                    last_tick,
                                })
                            })?.collect::<Result<Vec<_>, _>>()?); x
                        }
                    ).await.unwrap();
                    Ok(rows)
                    // match rows {
                    //     Ok(rows) => Ok(rows
                    //         .iter()
                    //         .map(|row| {
                    //         })
                    //         .collect::<Vec<_>>()),
                    //     Err(e) => {
                    //         error!("Error getting next ticks {:?}", e);
                    //         Err(JobSchedulerError::CantListNextTicks)
                    //     }
                    // }
                }
            }
        })
    }

    fn set_next_and_last_tick(
        &mut self,
        guid: Uuid,
        next_tick: Option<DateTime<Utc>>,
        last_tick: Option<DateTime<Utc>>,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::UpdateJobData),
                SqliteStore::Inited(store) => {
                    // let store = store.read().await;
                    let next_tick = next_tick.map(|b| b.timestamp()).unwrap_or(0);
                    let last_tick = last_tick.map(|b| b.timestamp());
                    let sql = "UPDATE ".to_string()
                        + &*table
                        + " \
                        SET \
                         next_tick=$1, last_tick=$2 \
                        WHERE \
                            id = $3";
                    let resp = store.call(move |store|
                        Ok(store.execute(&sql, tokio_rusqlite::params![&next_tick, &last_tick, &guid])?)
                    ).await;
                    if let Err(e) = resp {
                        error!("Error updating next and last tick {:?}", e);
                        Err(JobSchedulerError::UpdateJobData)
                    } else {
                        Ok(())
                    }
                }
            }
        })
    }

    fn time_till_next_job(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Duration>, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CouldNotGetTimeUntilNextTick),
                SqliteStore::Inited(store) => {
                    // let store = store.read().await;
                    let now = Utc::now().timestamp();
                    let sql = "SELECT \
                            next_tick \
                        FROM "
                        .to_string()
                        + &*table
                        + " \
                        WHERE \
                              next_tick > 0\
                          AND next_tick > $2 \
                        ORDER BY next_tick ASC \
                        LIMIT 1";
                    let row = store.call(move |store|
                        Ok(store.query_row(&sql, [&now], 
                            |row|
                                Ok(row
                                    .get::<_, i64>(0)
                                    .ok()
                                    // .map(|r| r.get::<_, i64>(0))
                                    .map(|ts| ts - now)
                                    .filter(|ts| *ts > 0)
                                    .map(|ts| ts as u64)
                                    .map(std::time::Duration::from_secs))
                        )?)
                    ).await;
                    match row {
                        Err(e) => {
                            error!("Error getting time until next job {:?}", e);
                            Err(JobSchedulerError::CouldNotGetTimeUntilNextTick)
                        },
                        Ok(row) => Ok(row)
                    }
                }
            }
        })
    }
}
