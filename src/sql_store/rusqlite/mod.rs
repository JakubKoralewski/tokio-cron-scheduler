mod metadata_store;
mod notification_store;

use crate::JobSchedulerError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

pub use metadata_store::SqliteMetadataStore;
pub use notification_store::SqliteNotificationStore;

#[derive(Clone)]
pub enum SqliteStore {
    Created(String),
    // Inited(Arc<RwLock<rusqlite::Connection>>), // the RwLock will be needed since not sync
    Inited(tokio_rusqlite::Connection),
}

impl SqliteStore {
    pub fn inited(&self) -> bool {
        matches!(self, SqliteStore::Inited(_))
    }
}

impl Default for SqliteStore {
    fn default() -> Self {
        let url = std::env::var("SQLITE_URL")
            .unwrap();
         
        Self::Created(url)
    }
}

impl SqliteStore {
    pub async fn init(
        self,
    ) -> Result<SqliteStore, JobSchedulerError> {
        match self {
            SqliteStore::Created(url) => {
                let connection = tokio_rusqlite::Connection::open(
                    url,
                ).await.map_err(|e| JobSchedulerError::SqliteCantInit(format!("{e:?}")))?;
                // Ok(SqliteStore::Inited(Arc::new(RwLock::new(connection))))
                Ok(SqliteStore::Inited(connection))
            }
            SqliteStore::Inited(client) => Ok(SqliteStore::Inited(client)),
        }
    }
}
