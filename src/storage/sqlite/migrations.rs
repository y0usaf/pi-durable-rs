use rusqlite::{Connection, params};

use super::database::{get, invalid, run, transaction};
use crate::errors::Result;

pub struct Migration<'a> {
    pub version: u64,
    pub statements: &'a [&'a str],
}

const SCHEMA_TABLE: &str = r#"CREATE TABLE IF NOT EXISTS durable_schema (
			singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
			version INTEGER NOT NULL CHECK (version >= 0)
		) STRICT"#;

pub const SQLITE_MIGRATIONS: &[Migration<'static>] = &[Migration {
    version: 1,
    statements: &[
        r#"CREATE TABLE durable_metadata (
		singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
		next_id TEXT NOT NULL,
		next_seq INTEGER NOT NULL
	) STRICT"#,
        r#"INSERT INTO durable_metadata (singleton, next_id, next_seq) VALUES (1, '2', 1)"#,
        r#"CREATE TABLE record_ids (
		id INTEGER PRIMARY KEY,
		record_type TEXT NOT NULL CHECK (record_type IN ('conversation', 'entry', 'task', 'submission', 'document'))
	) STRICT"#,
        r#"CREATE TABLE conversations (
		id INTEGER PRIMARY KEY,
		owner_conversation_id INTEGER,
		owner_task_id INTEGER,
		record TEXT NOT NULL CHECK (json_valid(record))
	) STRICT"#,
        r#"CREATE INDEX conversations_by_owner_conversation ON conversations (owner_conversation_id, id)"#,
        r#"CREATE INDEX conversations_by_owner_task ON conversations (owner_task_id, id)"#,
        r#"CREATE TABLE entries (
		id INTEGER PRIMARY KEY,
		conversation_id INTEGER NOT NULL,
		head INTEGER,
		commit_seq INTEGER NOT NULL,
		record TEXT NOT NULL CHECK (json_valid(record))
	) STRICT"#,
        r#"CREATE INDEX entries_by_conversation ON entries (conversation_id, id DESC)"#,
        r#"CREATE INDEX entry_heads_by_conversation ON entries (conversation_id, id DESC) WHERE head IS NOT NULL"#,
        r#"CREATE TABLE tasks (
		id INTEGER PRIMARY KEY,
		conversation_id INTEGER NOT NULL,
		kind TEXT NOT NULL,
		status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'waiting', 'completing', 'terminal')),
		abort_requested INTEGER NOT NULL CHECK (abort_requested IN (0, 1)),
		background INTEGER NOT NULL CHECK (background IN (0, 1)),
		record TEXT NOT NULL CHECK (json_valid(record))
	) STRICT"#,
        r#"CREATE INDEX tasks_by_status ON tasks (status, id)"#,
        r#"CREATE INDEX tasks_by_conversation ON tasks (conversation_id, id)"#,
        r#"CREATE INDEX tasks_by_kind ON tasks (kind, id)"#,
        r#"CREATE INDEX tasks_by_abort_requested ON tasks (abort_requested, id)"#,
        r#"CREATE INDEX tasks_by_background ON tasks (background, id)"#,
        r#"CREATE TABLE submissions (
		id INTEGER PRIMARY KEY,
		conversation_id INTEGER NOT NULL,
		request_id TEXT,
		status TEXT NOT NULL CHECK (status IN ('queued', 'placed', 'done', 'unanswered')),
		record TEXT NOT NULL CHECK (json_valid(record))
	) STRICT"#,
        r#"CREATE INDEX submissions_by_request ON submissions (conversation_id, request_id)"#,
        r#"CREATE INDEX submissions_by_conversation ON submissions (conversation_id, id)"#,
        r#"CREATE INDEX submissions_by_status ON submissions (status, id)"#,
        r#"CREATE TABLE documents (
		id INTEGER PRIMARY KEY,
		kind TEXT NOT NULL,
		family INTEGER NOT NULL CHECK (family IN (0, 1)),
		key_value TEXT NOT NULL,
		scope_kind TEXT NOT NULL CHECK (scope_kind IN ('session', 'conversation', 'task')),
		owner_id INTEGER NOT NULL,
		created_at INTEGER NOT NULL,
		retired_at INTEGER,
		record TEXT NOT NULL CHECK (json_valid(record))
	) STRICT"#,
        r#"CREATE INDEX documents_by_address
		ON documents (kind, scope_kind, owner_id, family, key_value, created_at DESC, retired_at)"#,
        r#"CREATE INDEX documents_by_scope ON documents (scope_kind, owner_id, id)"#,
        r#"CREATE INDEX documents_by_scope_kind ON documents (scope_kind, owner_id, kind, id)"#,
        r#"CREATE TABLE document_revisions (
		document_id INTEGER NOT NULL,
		seq INTEGER NOT NULL,
		kind TEXT NOT NULL CHECK (kind IN ('base', 'delta')),
		version INTEGER NOT NULL,
		content TEXT NOT NULL CHECK (json_valid(content)),
		PRIMARY KEY (document_id, seq)
	) STRICT"#,
        r#"CREATE INDEX document_revisions_by_kind ON document_revisions (document_id, kind, seq DESC)"#,
    ],
}];

pub fn apply_migrations(connection: &mut Connection, migrations: &[Migration]) -> Result<()> {
    if migrations
        .iter()
        .enumerate()
        .any(|(index, migration)| migration.version != index as u64 + 1)
    {
        return Err(invalid(
            "Durable SQLite migrations must have contiguous versions starting at 1",
        ));
    }
    transaction(connection, |transaction| {
        transaction.execute_batch(SCHEMA_TABLE).map_err(invalid)?;
        run(
            transaction,
            "INSERT OR IGNORE INTO durable_schema (singleton, version) VALUES (1, 0)",
            params![],
        )?;
        let version: u64 = get(
            transaction,
            "SELECT version FROM durable_schema WHERE singleton = 1",
            params![],
            |row| row.get(0),
        )?
        .ok_or_else(|| invalid("Durable SQLite schema metadata is missing"))?;
        let current = migrations.last().map_or(0, |migration| migration.version);
        if version > current {
            return Err(invalid(format!(
                "Durable SQLite schema version {version} is newer than supported version {current}"
            )));
        }
        for migration in migrations {
            if migration.version <= version {
                continue;
            }
            for statement in migration.statements {
                transaction.execute_batch(statement).map_err(invalid)?;
            }
            run(
                transaction,
                "UPDATE durable_schema SET version = ? WHERE singleton = 1",
                params![migration.version],
            )?;
        }
        Ok(())
    })
}
