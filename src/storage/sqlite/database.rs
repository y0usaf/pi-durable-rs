use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, Params, Row, TransactionBehavior};

use crate::errors::{Error, Result};

#[derive(Debug, Clone, Copy, Default)]
pub struct OpenOptions {
    pub wal_auto_checkpoint_pages: Option<u64>,
    pub busy_timeout_ms: Option<u64>,
}

pub(crate) fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Invalid(error.to_string())
}

pub(crate) fn sql(error: rusqlite::Error) -> Error {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
        | rusqlite::Error::SqlInputError { msg: message, .. } => Error::Invalid(message),
        other => Error::Invalid(other.to_string()),
    }
}

pub(crate) fn open(path: &Path, options: &OpenOptions) -> Result<Connection> {
    let checkpoint_pages = options.wal_auto_checkpoint_pages.unwrap_or(1_000);
    let timeout = options.busy_timeout_ms.unwrap_or(5_000);
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(invalid)?;
    }
    let connection = Connection::open(path).map_err(sql)?;
    connection.set_prepared_statement_cache_capacity(256);
    connection
        .busy_timeout(Duration::from_millis(timeout))
        .map_err(sql)?;
    connection
        .execute_batch(&format!(
            "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA wal_autocheckpoint = {checkpoint_pages};"
        ))
        .map_err(sql)?;
    Ok(connection)
}

pub(crate) fn close(connection: Connection) -> Result<()> {
    let checkpoint = connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)");
    let closed = connection.close();
    checkpoint.map_err(sql)?;
    closed.map_err(|(_, error)| sql(error))
}

pub(crate) fn run(connection: &Connection, statement: &str, params: impl Params) -> Result<()> {
    connection
        .prepare_cached(statement)
        .and_then(|mut prepared| prepared.execute(params))
        .map(|_| ())
        .map_err(sql)
}

pub(crate) fn get<T>(
    connection: &Connection,
    statement: &str,
    params: impl Params,
    map: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Option<T>> {
    let mut prepared = connection.prepare_cached(statement).map_err(sql)?;
    let mut rows = prepared.query(params).map_err(sql)?;
    match rows.next().map_err(sql)? {
        Some(row) => map(row).map(Some).map_err(sql),
        None => Ok(None),
    }
}

pub(crate) fn all<T>(
    connection: &Connection,
    statement: &str,
    params: impl Params,
    map: impl FnMut(&Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>> {
    let mut prepared = connection.prepare_cached(statement).map_err(sql)?;
    let rows = prepared.query_map(params, map).map_err(sql)?;
    rows.collect::<rusqlite::Result<Vec<T>>>().map_err(sql)
}

pub(crate) fn transaction<T>(
    connection: &mut Connection,
    body: impl FnOnce(&Connection) -> Result<T>,
) -> Result<T> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sql)?;
    match body(&transaction) {
        Ok(value) => {
            transaction.commit().map_err(sql)?;
            Ok(value)
        }
        Err(error) => match transaction.rollback() {
            Ok(()) => Err(error),
            Err(rollback) => Err(Error::StorageRejected {
                message: format!("SQLite transaction failed and rollback failed: {rollback}"),
                cause: Some(Box::new(error)),
            }),
        },
    }
}
