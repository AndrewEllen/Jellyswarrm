use std::{collections::HashMap, sync::Arc, time::Duration};

use sqlx::{FromRow, Row, SqlitePool};
use tracing::{debug, error, info, trace};
use uuid::Uuid;
use tokio::sync::RwLock;

use crate::models::generate_token;
use crate::server_storage::Server;
use moka::future::Cache;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDedupeMember {
    pub server_id: i64,
    pub virtual_media_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaDedupeGroup {
    pub canonical_virtual_media_id: String,
    pub members: Vec<MediaDedupeMember>,
}

#[derive(Debug, Clone, FromRow)]
pub struct MediaMapping {
    pub id: i64,
    pub virtual_media_id: String,
    pub original_media_id: String,
    pub server_url: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct MediaStorageService {
    pool: SqlitePool,
    original_mapping_cache: Cache<String, MediaMapping>,
    mapping_with_server_cache: Cache<String, (MediaMapping, Server)>,
    dedupe_group_cache: Cache<String, MediaDedupeGroup>,
    dedupe_member_index_cache: Cache<String, String>,
    dedupe_group_index: Arc<RwLock<HashMap<String, MediaDedupeGroup>>>,
}

impl MediaStorageService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            original_mapping_cache: Cache::builder()
                .time_to_live(Duration::from_secs(60 * 30))
                .max_capacity(100_000)
                .build(),
            mapping_with_server_cache: Cache::builder()
                .time_to_live(Duration::from_secs(60 * 30))
                .max_capacity(10_000)
                .build(),
            dedupe_group_cache: Cache::builder()
                .time_to_live(Duration::from_secs(60 * 60 * 6))
                .max_capacity(100_000)
                .build(),
            dedupe_member_index_cache: Cache::builder()
                .time_to_live(Duration::from_secs(60 * 60 * 6))
                .max_capacity(500_000)
                .build(),
            dedupe_group_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_media_dedupe_group(
        &self,
        canonical_virtual_media_id: &str,
        members: Vec<MediaDedupeMember>,
    ) {
        if members.is_empty() {
            return;
        }

        let canonical_virtual_media_id = Self::normalize_uuid(canonical_virtual_media_id);

        let mut deduped_members = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for member in members {
            let normalized_virtual_id = Self::normalize_uuid(&member.virtual_media_id);
            let key = (member.server_id, normalized_virtual_id.clone());
            if !seen.insert(key) {
                continue;
            }

            deduped_members.push(MediaDedupeMember {
                server_id: member.server_id,
                virtual_media_id: normalized_virtual_id,
            });
        }

        if deduped_members.is_empty() {
            return;
        }

        if let Some(existing) = self.dedupe_group_cache.get(&canonical_virtual_media_id).await {
            for member in existing.members {
                self.dedupe_member_index_cache
                    .invalidate(&member.virtual_media_id)
                    .await;
            }
        }

        for member in &deduped_members {
            self.dedupe_member_index_cache
                .insert(
                    member.virtual_media_id.clone(),
                    canonical_virtual_media_id.clone(),
                )
                .await;
        }

        self.dedupe_member_index_cache
            .insert(
                canonical_virtual_media_id.clone(),
                canonical_virtual_media_id.clone(),
            )
            .await;

        let group = MediaDedupeGroup {
            canonical_virtual_media_id: canonical_virtual_media_id.clone(),
            members: deduped_members,
        };

        self.dedupe_group_cache
            .insert(canonical_virtual_media_id.clone(), group.clone())
            .await;
        self.dedupe_group_index
            .write()
            .await
            .insert(canonical_virtual_media_id, group);
    }

    pub async fn get_media_dedupe_group(
        &self,
        media_virtual_id: &str,
    ) -> Option<MediaDedupeGroup> {
        let media_virtual_id = Self::normalize_uuid(media_virtual_id);

        if let Some(canonical_virtual_id) = self.dedupe_member_index_cache.get(&media_virtual_id).await
        {
            return self.dedupe_group_cache.get(&canonical_virtual_id).await;
        }

        self.dedupe_group_cache.get(&media_virtual_id).await
    }

    pub async fn list_media_dedupe_groups(&self) -> Vec<MediaDedupeGroup> {
        self.dedupe_group_index
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn get_media_dedupe_member_for_server(
        &self,
        media_virtual_id: &str,
        server_id: i64,
    ) -> Option<String> {
        let group = self.get_media_dedupe_group(media_virtual_id).await?;
        group
            .members
            .iter()
            .find(|member| member.server_id == server_id)
            .map(|member| member.virtual_media_id.clone())
    }

    /// Create or get a media mapping
    pub async fn get_or_create_media_mapping(
        &self,
        original_media_id: &str,
        server_url: &str,
    ) -> Result<MediaMapping, sqlx::Error> {
        let key = format!("{}|{}", original_media_id, server_url);
        if let Some(cached) = self.original_mapping_cache.get(&key).await {
            trace!("Cache hit for media mapping: {}", key);
            return Ok(cached);
        }
        let mapping = self
            ._get_or_create_media_mapping(original_media_id, server_url)
            .await?;
        self.original_mapping_cache
            .insert(key, mapping.clone())
            .await;
        Ok(mapping)
    }

    async fn _get_or_create_media_mapping(
        &self,
        original_media_id: &str,
        server_url: &str,
    ) -> Result<MediaMapping, sqlx::Error> {
        let original_media_id = Self::normalize_uuid(original_media_id);

        // Try to find existing mapping
        if let Some(mapping) = self
            .get_media_mapping_by_original(&original_media_id, server_url)
            .await?
        {
            return Ok(mapping);
        }

        // Create new mapping
        let virtual_media_id = generate_token();
        let now = chrono::Utc::now();

        let inserted = sqlx::query_as::<_, MediaMapping>(
            r#"
            INSERT INTO media_mappings (virtual_media_id, original_media_id, server_url, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(original_media_id, server_url) DO NOTHING
            RETURNING id, virtual_media_id, original_media_id, server_url, created_at
            "#,
        )
        .bind(&virtual_media_id)
        .bind(&original_media_id)
        .bind(server_url)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = inserted {
            debug!(
                "Created new media mapping: {} -> {} ({})",
                &original_media_id, row.virtual_media_id, server_url
            );
            return Ok(row);
        }

        // Conflict path: fetch existing row. Happens if another process created it concurrently
        if let Some(existing) = self
            .get_media_mapping_by_original(&original_media_id, server_url)
            .await?
        {
            return Ok(existing);
        }

        // If we reach here, something went very wrong
        Err(sqlx::Error::RowNotFound)
    }

    pub fn normalize_uuid(s: &str) -> String {
        match Uuid::parse_str(s) {
            Ok(uuid) => uuid.simple().to_string(),
            Err(_) => s.to_string(),
        }
    }

    /// Get media mapping by virtual media ID
    pub async fn get_media_mapping_by_virtual(
        &self,
        virtual_media_id: &str,
    ) -> Result<Option<MediaMapping>, sqlx::Error> {
        let virtual_media_id = Self::normalize_uuid(virtual_media_id);

        let mapping = sqlx::query_as::<_, MediaMapping>(
            r#"
            SELECT id, virtual_media_id, original_media_id, server_url, created_at
            FROM media_mappings 
            WHERE virtual_media_id = ?
            "#,
        )
        .bind(virtual_media_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(mapping)
    }

    /// Get media mapping by original media ID and server
    pub async fn get_media_mapping_by_original(
        &self,
        original_media_id: &str,
        server_url: &str,
    ) -> Result<Option<MediaMapping>, sqlx::Error> {
        let original_media_id = Self::normalize_uuid(original_media_id);

        let mapping = sqlx::query_as::<_, MediaMapping>(
            r#"
            SELECT id, virtual_media_id, original_media_id, server_url, created_at
            FROM media_mappings 
            WHERE original_media_id = ? AND server_url = ?
            "#,
        )
        .bind(original_media_id)
        .bind(server_url)
        .fetch_optional(&self.pool)
        .await?;

        Ok(mapping)
    }

    /// Get media mapping with server information by virtual media ID
    pub async fn get_media_mapping_with_server(
        &self,
        virtual_media_id: &str,
    ) -> Result<Option<(MediaMapping, Server)>, sqlx::Error> {
        let virtual_media_id = Self::normalize_uuid(virtual_media_id);

        if let Some(cached) = self.mapping_with_server_cache.get(&virtual_media_id).await {
            trace!(
                "Cache hit for media mapping with server: {}",
                virtual_media_id
            );
            return Ok(Some(cached));
        }

        let row = sqlx::query(
            r#"
            SELECT 
                m.id as media_id,
                m.virtual_media_id,
                m.original_media_id,
                m.server_url as media_server_url,
                m.created_at as media_created_at,
                
                s.id as server_id,
                s.name as server_name,
                s.url as server_url_full,
                s.priority,
                s.created_at as server_created_at,
                s.updated_at as server_updated_at
            FROM media_mappings m
            JOIN servers s ON RTRIM(m.server_url, '/') = RTRIM(s.url, '/')
            WHERE m.virtual_media_id = ?
            "#,
        )
        .bind(&virtual_media_id)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let mapping = MediaMapping {
                id: row.get("media_id"),
                virtual_media_id: row.get("virtual_media_id"),
                original_media_id: row.get("original_media_id"),
                server_url: row.get("media_server_url"),
                created_at: row.get("media_created_at"),
            };

            let server = Server {
                id: row.get("server_id"),
                name: row.get("server_name"),
                url: url::Url::parse(row.get::<String, _>("server_url_full").as_str()).unwrap(),
                priority: row.get("priority"),
                created_at: row.get("server_created_at"),
                updated_at: row.get("server_updated_at"),
            };

            self.mapping_with_server_cache
                .insert(virtual_media_id, (mapping.clone(), server.clone()))
                .await;
            Ok(Some((mapping, server)))
        } else {
            Ok(None)
        }
    }

    /// Delete a media mapping
    pub async fn delete_media_mapping(&self, virtual_media_id: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM media_mappings WHERE virtual_media_id = ?
            "#,
        )
        .bind(virtual_media_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            {
                let id_to_invalidate = virtual_media_id.to_string();
                if let Err(e) =
                    self.original_mapping_cache
                        .invalidate_entries_if(move |_, value| {
                            value.virtual_media_id == id_to_invalidate
                        })
                {
                    error!("Failed to invalidate cache entry: {}", e);
                    self.original_mapping_cache.invalidate_all();
                }
            }
            // Also invalidate the mapping_with_server_cache
            self.mapping_with_server_cache
                .invalidate(virtual_media_id)
                .await;
            self.dedupe_group_cache.invalidate_all();
            self.dedupe_member_index_cache.invalidate_all();
            self.dedupe_group_index.write().await.clear();
            info!("Deleted media mapping: {}", virtual_media_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete all media mappings for a specific server
    pub async fn delete_media_mappings_by_server(
        &self,
        server_url: &str,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query(
            r#"
            DELETE FROM media_mappings WHERE server_url = ?
            "#,
        )
        .bind(server_url)
        .execute(&self.pool)
        .await?;

        let deleted_count = result.rows_affected();
        if deleted_count > 0 {
            info!(
                "Deleted {} media mappings for server: {}",
                deleted_count, server_url
            );
        }
        self.original_mapping_cache.invalidate_all();
        self.mapping_with_server_cache.invalidate_all();
        self.dedupe_group_cache.invalidate_all();
        self.dedupe_member_index_cache.invalidate_all();
        self.dedupe_group_index.write().await.clear();
        Ok(deleted_count)
    }
}

#[cfg(test)]
mod tests {
    use crate::config::MIGRATOR;

    use super::*;

    #[tokio::test]
    async fn test_media_storage_service() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let service = MediaStorageService::new(pool.clone());

        // Create media mapping
        let mapping = service
            .get_or_create_media_mapping("original-movie-123", "http://localhost:8096")
            .await
            .unwrap();

        assert_eq!(mapping.original_media_id, "original-movie-123");
        assert_eq!(mapping.server_url, "http://localhost:8096");

        // Get mapping by virtual ID
        let retrieved_mapping = service
            .get_media_mapping_by_virtual(&mapping.virtual_media_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retrieved_mapping.virtual_media_id, mapping.virtual_media_id);
        assert_eq!(retrieved_mapping.original_media_id, "original-movie-123");
    }

    #[tokio::test]
    async fn test_get_media_mapping_with_server() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let service = MediaStorageService::new(pool.clone());

        // Create the servers table (normally done by ServerStorageService)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS servers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                url TEXT NOT NULL,
                priority INTEGER NOT NULL DEFAULT 100,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&pool)
        .await
        .unwrap();

        // Create a server in the servers table
        sqlx::query(
            r#"
            INSERT INTO servers (name, url, priority, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            "#,
        )
        .bind("Test Server")
        .bind("http://localhost:8096")
        .bind(100)
        .bind(chrono::Utc::now())
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .unwrap();

        // Create media mapping
        let mapping = service
            .get_or_create_media_mapping("original-movie-123", "http://localhost:8096")
            .await
            .unwrap();

        // Get mapping with server info
        let (retrieved_mapping, server) = service
            .get_media_mapping_with_server(&mapping.virtual_media_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(retrieved_mapping.virtual_media_id, mapping.virtual_media_id);
        assert_eq!(retrieved_mapping.original_media_id, "original-movie-123");
        assert_eq!(server.name, "Test Server");
        assert_eq!(
            server.url.as_str().trim_end_matches('/'),
            "http://localhost:8096"
        );
    }

    #[tokio::test]
    async fn test_delete_operations() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let service = MediaStorageService::new(pool.clone());

        // Create media mapping
        let mapping = service
            .get_or_create_media_mapping("movie-123", "http://localhost:8096")
            .await
            .unwrap();

        // Verify mapping exists
        assert!(service
            .get_media_mapping_by_virtual(&mapping.virtual_media_id)
            .await
            .unwrap()
            .is_some());

        // Delete mapping
        let deleted = service
            .delete_media_mapping(&mapping.virtual_media_id)
            .await
            .unwrap();

        assert!(deleted);

        // Verify mapping is gone
        assert!(service
            .get_media_mapping_by_virtual(&mapping.virtual_media_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn test_media_dedupe_group_cache() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        MIGRATOR.run(&pool).await.unwrap();
        let service = MediaStorageService::new(pool);

        service
            .register_media_dedupe_group(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                vec![
                    MediaDedupeMember {
                        server_id: 1,
                        virtual_media_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                    },
                    MediaDedupeMember {
                        server_id: 2,
                        virtual_media_id: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
                    },
                ],
            )
            .await;

        let by_canonical = service
            .get_media_dedupe_group("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .await
            .unwrap();
        assert_eq!(by_canonical.members.len(), 2);

        let by_member = service
            .get_media_dedupe_group("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
            .await
            .unwrap();
        assert_eq!(
            by_member.canonical_virtual_media_id,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let member_server_2 = service
            .get_media_dedupe_member_for_server("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 2)
            .await
            .unwrap();
        assert_eq!(member_server_2, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    }
}
