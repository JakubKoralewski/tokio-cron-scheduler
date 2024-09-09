use crate::job::job_data_prost::{JobIdAndNotification, JobState, NotificationData};
use crate::job::{JobId, NotificationId};
use crate::store::{DataStore, InitStore, NotificationStore};
use crate::{JobSchedulerError, SqliteStore};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;
use uuid::Uuid;

const MAIN_TABLE: &str = "notification";
const STATES_TABLE: &str = "notification_state";

#[derive(Clone)]
pub struct SqliteNotificationStore {
    pub store: Arc<RwLock<SqliteStore>>,
    pub init_tables: bool,
    pub table: String,
    pub states_table: String,
}

impl Default for SqliteNotificationStore {
    fn default() -> Self {
        let init_tables = std::env::var("SQLITE_INIT_NOTIFICATIONS")
            .map(|s| s.to_lowercase() == "true")
            .unwrap_or_default();
        let table = std::env::var("SQLITE_NOTIFICATION_TABLE")
            .unwrap_or_else(|_| MAIN_TABLE.to_lowercase());
        let states_table = std::env::var("SQLITE_NOTIFICATION_STATES_TABLE")
            .unwrap_or_else(|_| STATES_TABLE.to_lowercase());
        let store = Arc::new(RwLock::new(SqliteStore::default()));
        Self {
            init_tables,
            table,
            states_table,
            store,
        }
    }
}

impl DataStore<NotificationData> for SqliteNotificationStore {
    fn get(
        &mut self,
        id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Option<NotificationData>, JobSchedulerError>> + Send>>
    {
        let store = self.store.clone();
        let table = self.table.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::GetJobData),
                SqliteStore::Inited(store) => {
                    let sql =
                        "SELECT id, job_id, extra from ".to_string() + &*table + " where id = $1";
                    let data = store.call(move |store|
                        Ok(store.query_row(&*sql, &[&id], |row| {
                            let notification_id: Uuid = row.get(0).unwrap();
                            let job_id: Uuid = row.get(1).unwrap();

                            let job_id = JobIdAndNotification {
                                job_id: Some(job_id.into()),
                                notification_id: Some(notification_id.into()),
                            };

                            let extra = row.get(2).unwrap();
                            let job_id = Some(job_id);
                            Ok((notification_id, job_id, extra))
                        })?)
                    ).await;
                    let (notification_id, job_id, extra) = match data {
                        Err(tokio_rusqlite::Error::Rusqlite(tokio_rusqlite::rusqlite::Error::QueryReturnedNoRows)) => return Ok(None),
                        Err(e) => {
                            error!("Error fetching notification data {:?}", e);
                            return Err(JobSchedulerError::GetJobData);
                        },
                        Ok(data) => data
                    };

                    let job_states = {
                        let sql =
                            "SELECT state from ".to_string() + &*states_table + " where id = $1";
                        let rows = store.call(move |store|  {
                            let mut stmt = store.prepare(&sql).unwrap();
                            let x = Ok(stmt.query_map(&[&notification_id], |row| row.get(0))?.collect::<Result<Vec<i32>, _>>()?); x
                        }).await;
                        match rows {
                            Ok(rows) => rows,
                            Err(e) => {
                                error!("Error getting states {:?}", e);
                                vec![]
                            }
                        }
                    };

                    Ok(Some(NotificationData {
                        job_id,
                        job_states,
                        extra,
                    }))
                }
            }
        })
    }

    fn add_or_update(
        &mut self,
        data: NotificationData,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::UpdateJobData),
                SqliteStore::Inited(store) => {
                    let (job_id, notification_id) =
                        match data.job_id_and_notification_id_from_data() {
                            Some((job_id, notification_id)) => (job_id, notification_id),
                            None => return Err(JobSchedulerError::UpdateJobData),
                        };
                    let sql = "DELETE FROM ".to_string() + &*states_table + " WHERE id = $1";
                    let result = store.call(move |store| Ok(store.execute(&sql, &[&notification_id])?)).await;
                    if let Err(e) = result {
                        error!("Error deleting {:?}", e);
                    }

                    let sql = "INSERT INTO ".to_string()
                        + &*table
                        + " (id, job_id, extra) \
                    VALUES ($1, $2, $3) \
                    ON CONFLICT (id) \
                    DO \
                        UPDATE \
                        SET \
                            job_id = $2, extra = $3";
                    let extra = data.extra;
                    let result = store.call(
                        move |store|
                             Ok(
                                store.execute(
                                    &sql,
                                    tokio_rusqlite::params![notification_id, job_id, &extra]
                                )
                            ?)
                    ).await;

                    if let Err(e) = result {
                        error!("Error doing the upsert {:?}", e);
                    }

                    if !data.job_states.is_empty() {
                        let sql = "INSERT INTO ".to_string()
                            + &*states_table
                            + " (id, state) VALUES "
                            + &*data
                                .job_states
                                .iter()
                                .map(|s| format!("($1, {})", s))
                                .collect::<Vec<_>>()
                                .join(",");
                        let result = store.call(move |store| Ok(store.execute(&sql, &[&notification_id])?)).await;
                        if let Err(e) = result {
                            error!("Error inserting state vals {:?}", e);
                        }
                    }
                    Ok(())
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

                    store.call(move |store|
                        Ok(store.execute(&*sql, &[&guid])?)
                    ).await.map(|_| ()).map_err(|e| {
                        error!("Error deleting notification {:?}", e);
                        JobSchedulerError::CantRemove
                    })
                }
            }
        })
    }
}

impl InitStore for SqliteNotificationStore {
    fn init(&mut self) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let inited = self.inited();
        let store = self.store.clone();
        let init_tables = self.init_tables;
        let table = self.table.clone();
        let states_table = self.states_table.clone();

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
                                let sql = "CREATE TABLE IF NOT EXISTS ".to_string()
                                    + &*table
                                    + " ( \
                                    id BLOB, \
                                    job_id BLOB, \
                                    extra BLOB, \
                                    CONSTRAINT pk_notification_id PRIMARY KEY (id)
                                )";
                                let create = client.call(move |store|
                                    Ok(store.execute(&*sql, [])?)
                                ).await;
                                if let Err(e) = create {
                                    error!("Error creating notification table {:?}", e);
                                    return Err(JobSchedulerError::CantInit);
                                }
                                let sql = "CREATE TABLE IF NOT EXISTS ".to_string()
                                    + &*states_table
                                    + " (\
                                    id BLOB NOT NULL,
                                    state INTEGER NOT NULL,
                                    CONSTRAINT pk_notification_states PRIMARY KEY (id, state),
                                    CONSTRAINT fk_notification_id FOREIGN KEY (id) REFERENCES "
                                    + &*table
                                    + " (id) ON DELETE CASCADE
                                )";
                                let create = client.call(move |store| 
                                    Ok(store.execute(&sql, [])?)
                                ).await;
                                if let Err(e) = create {
                                    error!("Error creating notification states table {:?}", e);
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

impl NotificationStore for SqliteNotificationStore {
    fn list_notification_guids_for_job_and_state(
        &mut self,
        job: JobId,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<NotificationId>, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();
        let states_table = self.states_table.clone();
        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CantListGuids),
                SqliteStore::Inited(store) => {
                    let state = state as i32;
                    let sql = "SELECT DISTINCT states.id \
                    FROM \
                     "
                    .to_string()
                        + &*table
                        + " as states \
                     RIGHT JOIN "
                        + &*states_table
                        + " as st ON st.id = states.id \
                    WHERE \
                         job_id = $1 \
                     AND state = $2";
                    let result = store.call(move |store| {
                        let mut stmt = store.prepare(&sql)?;
                        let x = Ok(stmt.query_map(
                            tokio_rusqlite::params![&job, &state],
                            |row| Ok(row.get(0)?)
                        )?.collect::<Result<Vec<Uuid>,_>>()?); x
                    }).await;
                    match result {
                        Ok(rows) => Ok(rows),
                            // .iter()
                            // .map(|r| {
                            //     let uuid: Uuid = r.get(0);
                            //     uuid
                            // })
                            // .collect::<Vec<_>>()),
                        Err(e) => {
                            error!("Error listing notification guids for job and state {:?}", e);
                            Err(JobSchedulerError::CantListGuids)
                        }
                    }
                }
            }
        })
    }

    fn list_notification_guids_for_job_id(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Uuid>, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CantListGuids),
                SqliteStore::Inited(store) => {
                    let sql =
                        "SELECT DISTINCT id FROM ".to_string() + &*table + " WHERE job_id = $1";

                    let result = store.call(move |store| {
                        let mut stmt = store.prepare(&sql)?;
                        let x = Ok(stmt.query_map(
                            tokio_rusqlite::params![&job_id],
                            |row| Ok(row.get(0)?)
                        )?.collect::<Result<Vec<Uuid>,_>>()?); x
                    }).await;
                    match result {
                        Ok(rows) => Ok(rows),
                        Err(e) => {
                            error!(
                                "Error getting list of notifications guids for job id{:?}",
                                e
                            );
                            Err(JobSchedulerError::CantListGuids)
                        }
                    }
                }
            }
        })
    }

    fn delete_notification_for_state(
        &mut self,
        notification_id: Uuid,
        state: JobState,
    ) -> Pin<Box<dyn Future<Output = Result<bool, JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let states_table = self.states_table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CantRemove),
                SqliteStore::Inited(store) => {
                    let state = state as i32;
                    let sql = "DELETE FROM ".to_string()
                        + &*states_table
                        + " \
                    WHERE \
                            id = $1 \
                        AND state = $2 \
                    RETURNING state";
                    let result = store.call(
                        move |store| 
                            Ok(store.execute(&*sql, tokio_rusqlite::params![&notification_id, &state])?)
                    ).await;
                    match result {
                        Ok(row) => Ok(row != 0),
                        Err(e) => {
                            error!("Error deleting notification for state {:?}", e);
                            Err(JobSchedulerError::CantRemove)
                        }
                    }
                }
            }
        })
    }

    fn delete_for_job(
        &mut self,
        job_id: Uuid,
    ) -> Pin<Box<dyn Future<Output = Result<(), JobSchedulerError>> + Send>> {
        let store = self.store.clone();
        let table = self.table.clone();

        Box::pin(async move {
            let store = store.read().await;
            match &*store {
                SqliteStore::Created(_) => Err(JobSchedulerError::CantRemove),
                SqliteStore::Inited(store) => {
                    let sql = "DELETE FROM ".to_string() + &*table + " WHERE job_id = $1";
                    store.call(move |store| 
                        Ok(store.execute(&*sql, &[&job_id])?)
                    )
                        .await
                        .map(|_| ())
                        .map_err(|e| {
                            error!("Error deleting for job {:?}", e);
                            JobSchedulerError::CantRemove
                        })
                }
            }
        })
    }
}
