use std::collections::HashSet;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, SqlitePool};
use uuid::Uuid;

use crate::models::generate_token;

const ALLOWED_COLLECTION_TYPES: [&str; 12] = [
    "movies",
    "tvshows",
    "music",
    "musicvideos",
    "trailers",
    "homevideos",
    "boxsets",
    "books",
    "photos",
    "livetv",
    "playlists",
    "folders",
];

#[derive(Debug, Clone, FromRow)]
pub struct LibraryGroup {
    pub id: i64,
    pub virtual_library_id: String,
    pub name: String,
    pub collection_type: String,
    pub preview_server_id: i64,
    pub preview_library_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow)]
pub struct LibraryGroupSource {
    pub id: i64,
    pub library_group_id: i64,
    pub server_id: i64,
    pub source_library_id: String,
    pub source_library_name: String,
    pub source_collection_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct LibraryGroupWithSources {
    pub group: LibraryGroup,
    pub sources: Vec<LibraryGroupSource>,
}

#[derive(Debug, Clone)]
pub struct NewLibraryGroupSource {
    pub server_id: i64,
    pub source_library_id: String,
    pub source_library_name: String,
    pub source_collection_type: String,
}

#[derive(Debug, Clone, FromRow)]
pub struct PreviewSourceResolution {
    pub group_id: i64,
    pub virtual_library_id: String,
    pub group_name: String,
    pub collection_type: String,
    pub preview_server_id: i64,
    pub preview_library_id: String,
}

#[derive(Debug, Clone)]
pub struct LibraryManagementService {
    pool: SqlitePool,
}

impl LibraryManagementService {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub fn allowed_collection_types() -> &'static [&'static str] {
        &ALLOWED_COLLECTION_TYPES
    }

    pub fn normalize_collection_type(value: &str) -> String {
        value.trim().to_ascii_lowercase()
    }

    pub fn is_valid_collection_type(value: &str) -> bool {
        let normalized = Self::normalize_collection_type(value);
        ALLOWED_COLLECTION_TYPES.contains(&normalized.as_str())
    }

    pub async fn list_groups_with_sources(&self) -> Result<Vec<LibraryGroupWithSources>, sqlx::Error> {
        let groups = sqlx::query_as::<_, LibraryGroup>(
            r#"
            SELECT id, virtual_library_id, name, collection_type, preview_server_id, preview_library_id, created_at, updated_at
            FROM library_groups
            ORDER BY name COLLATE NOCASE ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(groups.len());
        for group in groups {
            let sources = self.list_sources_for_group(group.id).await?;
            out.push(LibraryGroupWithSources { group, sources });
        }

        Ok(out)
    }

    pub async fn get_group_by_id_with_sources(
        &self,
        id: i64,
    ) -> Result<Option<LibraryGroupWithSources>, sqlx::Error> {
        let group = sqlx::query_as::<_, LibraryGroup>(
            r#"
            SELECT id, virtual_library_id, name, collection_type, preview_server_id, preview_library_id, created_at, updated_at
            FROM library_groups
            WHERE id = ?
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(group) = group else {
            return Ok(None);
        };

        let sources = self.list_sources_for_group(group.id).await?;
        Ok(Some(LibraryGroupWithSources { group, sources }))
    }

    pub async fn get_group_by_virtual_id_with_sources(
        &self,
        virtual_id: &str,
    ) -> Result<Option<LibraryGroupWithSources>, sqlx::Error> {
        let normalized_virtual_id = Self::normalize_virtual_id(virtual_id);
        let group = sqlx::query_as::<_, LibraryGroup>(
            r#"
            SELECT id, virtual_library_id, name, collection_type, preview_server_id, preview_library_id, created_at, updated_at
            FROM library_groups
            WHERE virtual_library_id = ?
            "#,
        )
        .bind(normalized_virtual_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(group) = group else {
            return Ok(None);
        };

        let sources = self.list_sources_for_group(group.id).await?;
        Ok(Some(LibraryGroupWithSources { group, sources }))
    }

    pub async fn create_group(
        &self,
        name: &str,
        collection_type: &str,
        sources: Vec<NewLibraryGroupSource>,
    ) -> Result<LibraryGroupWithSources, sqlx::Error> {
        let normalized_name = name.trim();
        if normalized_name.is_empty() {
            return Err(validation_error("Library name cannot be empty"));
        }

        let normalized_collection_type = Self::normalize_collection_type(collection_type);
        if !Self::is_valid_collection_type(&normalized_collection_type) {
            return Err(validation_error("Invalid collection type"));
        }

        let deduped_sources = normalize_sources(&normalized_collection_type, sources)?;
        let preview = deduped_sources
            .first()
            .ok_or_else(|| validation_error("At least one source library is required"))?;

        let now = Utc::now();
        let virtual_library_id = generate_token();

        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            INSERT INTO library_groups
                (virtual_library_id, name, collection_type, preview_server_id, preview_library_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&virtual_library_id)
        .bind(normalized_name)
        .bind(&normalized_collection_type)
        .bind(preview.server_id)
        .bind(&preview.source_library_id)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await?;

        let group_id = result.last_insert_rowid();
        for source in &deduped_sources {
            sqlx::query(
                r#"
                INSERT INTO library_group_sources
                    (library_group_id, server_id, source_library_id, source_library_name, source_collection_type, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(group_id)
            .bind(source.server_id)
            .bind(&source.source_library_id)
            .bind(&source.source_library_name)
            .bind(&source.source_collection_type)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        self.get_group_by_id_with_sources(group_id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    pub async fn update_group(
        &self,
        id: i64,
        name: &str,
        collection_type: &str,
        sources: Vec<NewLibraryGroupSource>,
    ) -> Result<Option<LibraryGroupWithSources>, sqlx::Error> {
        let normalized_name = name.trim();
        if normalized_name.is_empty() {
            return Err(validation_error("Library name cannot be empty"));
        }

        let normalized_collection_type = Self::normalize_collection_type(collection_type);
        if !Self::is_valid_collection_type(&normalized_collection_type) {
            return Err(validation_error("Invalid collection type"));
        }

        let deduped_sources = normalize_sources(&normalized_collection_type, sources)?;
        let preview = deduped_sources
            .first()
            .ok_or_else(|| validation_error("At least one source library is required"))?;

        let now = Utc::now();
        let mut tx = self.pool.begin().await?;

        let update_result = sqlx::query(
            r#"
            UPDATE library_groups
            SET name = ?, collection_type = ?, preview_server_id = ?, preview_library_id = ?, updated_at = ?
            WHERE id = ?
            "#,
        )
        .bind(normalized_name)
        .bind(&normalized_collection_type)
        .bind(preview.server_id)
        .bind(&preview.source_library_id)
        .bind(now)
        .bind(id)
        .execute(&mut *tx)
        .await?;

        if update_result.rows_affected() == 0 {
            tx.rollback().await?;
            return Ok(None);
        }

        sqlx::query(
            r#"
            DELETE FROM library_group_sources
            WHERE library_group_id = ?
            "#,
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;

        for source in &deduped_sources {
            sqlx::query(
                r#"
                INSERT INTO library_group_sources
                    (library_group_id, server_id, source_library_id, source_library_name, source_collection_type, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(id)
            .bind(source.server_id)
            .bind(&source.source_library_id)
            .bind(&source.source_library_name)
            .bind(&source.source_collection_type)
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        self.get_group_by_id_with_sources(id).await
    }

    pub async fn delete_group(&self, id: i64) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM library_groups
            WHERE id = ?
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn resolve_preview_source_by_virtual_id(
        &self,
        virtual_id: &str,
    ) -> Result<Option<PreviewSourceResolution>, sqlx::Error> {
        let normalized_virtual_id = Self::normalize_virtual_id(virtual_id);
        sqlx::query_as::<_, PreviewSourceResolution>(
            r#"
            SELECT
                id as group_id,
                virtual_library_id,
                name as group_name,
                collection_type,
                preview_server_id,
                preview_library_id
            FROM library_groups
            WHERE virtual_library_id = ?
            "#,
        )
        .bind(normalized_virtual_id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn resolve_source_by_virtual_id_for_server(
        &self,
        virtual_id: &str,
        server_id: i64,
    ) -> Result<Option<LibraryGroupSource>, sqlx::Error> {
        let normalized_virtual_id = Self::normalize_virtual_id(virtual_id);
        sqlx::query_as::<_, LibraryGroupSource>(
            r#"
            SELECT
                s.id,
                s.library_group_id,
                s.server_id,
                s.source_library_id,
                s.source_library_name,
                s.source_collection_type,
                s.created_at,
                s.updated_at
            FROM library_group_sources s
            JOIN library_groups g ON g.id = s.library_group_id
            WHERE g.virtual_library_id = ? AND s.server_id = ?
            LIMIT 1
            "#,
        )
        .bind(normalized_virtual_id)
        .bind(server_id)
        .fetch_optional(&self.pool)
        .await
    }

    fn normalize_virtual_id(value: &str) -> String {
        match Uuid::parse_str(value.trim()) {
            Ok(uuid) => uuid.simple().to_string(),
            Err(_) => value.trim().to_string(),
        }
    }

    async fn list_sources_for_group(
        &self,
        group_id: i64,
    ) -> Result<Vec<LibraryGroupSource>, sqlx::Error> {
        sqlx::query_as::<_, LibraryGroupSource>(
            r#"
            SELECT id, library_group_id, server_id, source_library_id, source_library_name, source_collection_type, created_at, updated_at
            FROM library_group_sources
            WHERE library_group_id = ?
            ORDER BY id ASC
            "#,
        )
        .bind(group_id)
        .fetch_all(&self.pool)
        .await
    }
}

fn normalize_sources(
    normalized_collection_type: &str,
    sources: Vec<NewLibraryGroupSource>,
) -> Result<Vec<NewLibraryGroupSource>, sqlx::Error> {
    if sources.is_empty() {
        return Err(validation_error("At least one source library is required"));
    }

    let mut dedup = HashSet::new();
    let mut out = Vec::new();

    for source in sources {
        let source_library_id = source.source_library_id.trim().to_string();
        let source_library_name = source.source_library_name.trim().to_string();
        let source_collection_type =
            LibraryManagementService::normalize_collection_type(&source.source_collection_type);

        if source_library_id.is_empty() {
            return Err(validation_error("Source library ID cannot be empty"));
        }
        if source_library_name.is_empty() {
            return Err(validation_error("Source library name cannot be empty"));
        }
        if source_collection_type.is_empty() {
            return Err(validation_error("Source collection type cannot be empty"));
        }
        if source_collection_type != normalized_collection_type {
            return Err(validation_error(
                "Source collection type must match group collection type",
            ));
        }

        let key = (source.server_id, source_library_id.clone());
        if dedup.insert(key) {
            out.push(NewLibraryGroupSource {
                server_id: source.server_id,
                source_library_id,
                source_library_name,
                source_collection_type,
            });
        }
    }

    if out.is_empty() {
        return Err(validation_error("At least one source library is required"));
    }

    Ok(out)
}

fn validation_error(message: impl Into<String>) -> sqlx::Error {
    sqlx::Error::Protocol(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::MIGRATOR;

    async fn setup_service() -> LibraryManagementService {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        sqlx::query("PRAGMA foreign_keys = ON;")
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query(
            r#"
            INSERT INTO servers (name, url, priority, created_at, updated_at)
            VALUES
                ('Server A', 'http://a.local:8096', 100, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
                ('Server B', 'http://b.local:8096', 90, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        LibraryManagementService::new(pool)
    }

    #[tokio::test]
    async fn test_library_group_crud() {
        let service = setup_service().await;

        let created = service
            .create_group(
                "My Movies",
                "movies",
                vec![
                    NewLibraryGroupSource {
                        server_id: 1,
                        source_library_id: "movies_a".to_string(),
                        source_library_name: "Movies A".to_string(),
                        source_collection_type: "movies".to_string(),
                    },
                    NewLibraryGroupSource {
                        server_id: 2,
                        source_library_id: "movies_b".to_string(),
                        source_library_name: "Movies B".to_string(),
                        source_collection_type: "movies".to_string(),
                    },
                ],
            )
            .await
            .unwrap();

        assert_eq!(created.group.name, "My Movies");
        assert_eq!(created.group.collection_type, "movies");
        assert_eq!(created.sources.len(), 2);

        let all = service.list_groups_with_sources().await.unwrap();
        assert_eq!(all.len(), 1);

        let by_virtual = service
            .get_group_by_virtual_id_with_sources(&created.group.virtual_library_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_virtual.group.id, created.group.id);
        assert_eq!(by_virtual.sources.len(), 2);

        let updated = service
            .update_group(
                created.group.id,
                "My Movies Updated",
                "movies",
                vec![NewLibraryGroupSource {
                    server_id: 2,
                    source_library_id: "movies_b".to_string(),
                    source_library_name: "Movies B".to_string(),
                    source_collection_type: "movies".to_string(),
                }],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.group.name, "My Movies Updated");
        assert_eq!(updated.sources.len(), 1);

        let deleted = service.delete_group(created.group.id).await.unwrap();
        assert!(deleted);

        let none = service
            .get_group_by_id_with_sources(created.group.id)
            .await
            .unwrap();
        assert!(none.is_none());
    }

    #[tokio::test]
    async fn test_validation_rejects_invalid_inputs() {
        let service = setup_service().await;

        let err = service
            .create_group(" ", "movies", Vec::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Library name cannot be empty"));

        let err = service
            .create_group(
                "Bad Group",
                "movies",
                vec![NewLibraryGroupSource {
                    server_id: 1,
                    source_library_id: "tv_a".to_string(),
                    source_library_name: "TV A".to_string(),
                    source_collection_type: "tvshows".to_string(),
                }],
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("must match group collection type"));
    }
}
