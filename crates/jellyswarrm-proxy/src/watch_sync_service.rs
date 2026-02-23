use std::collections::HashMap;

use chrono::Utc;
use jellyfin_api::JellyfinClient;
use reqwest::{header::AUTHORIZATION, StatusCode};
use serde::Deserialize;
use tracing::{debug, error, warn};

use crate::{
    config::CLIENT_INFO,
    encryption::{HashedPassword, Password},
    media_storage_service::{MediaDedupeGroup, MediaDedupeMember},
    models::Authorization,
    server_storage::Server,
    url_helper::join_server_url,
    user_authorization_service::{AuthorizationSession, Device, ServerMapping, User},
    AppState,
};

#[derive(Debug, Default, Clone)]
pub struct WatchSyncReport {
    pub dedupe_groups_total: usize,
    pub dedupe_groups_eligible: usize,
    pub users_total: usize,
    pub users_with_mappings: usize,
    pub users_authenticated: usize,
    pub sync_pairs_checked: usize,
    pub updates_applied: usize,
    pub already_in_sync: usize,
    pub skipped_missing_items: usize,
    pub skipped_missing_sessions: usize,
    pub mapping_resolution_misses: usize,
    pub authentication_failures: usize,
    pub request_failures: usize,
}

#[derive(Debug, Clone)]
struct ResolvedGroupMember {
    server_id: i64,
    original_media_id: String,
}

#[derive(Debug, Clone)]
struct ResolvedGroup {
    members: Vec<ResolvedGroupMember>,
}

#[derive(Debug, Clone)]
struct AuthenticatedServerContext {
    server: Server,
    session: AuthorizationSession,
}

#[derive(Debug, Clone)]
struct CandidateMember {
    server: Server,
    session: AuthorizationSession,
    original_media_id: String,
}

pub async fn sync_watch_data_from_priority_servers(state: &AppState) -> WatchSyncReport {
    let mut report = WatchSyncReport::default();

    let servers = match state.server_storage.list_servers().await {
        Ok(servers) => servers,
        Err(e) => {
            error!("Failed to list servers before watch sync: {}", e);
            report.request_failures += 1;
            return report;
        }
    };

    let mut servers_by_url = HashMap::new();
    for server in servers {
        servers_by_url.insert(normalize_server_url(server.url.as_str()), server);
    }

    let dedupe_groups = state.media_storage.list_media_dedupe_groups().await;
    report.dedupe_groups_total = dedupe_groups.len();

    let resolved_groups = resolve_groups(state, dedupe_groups, &mut report).await;
    report.dedupe_groups_eligible = resolved_groups.len();

    let users = match state.user_authorization.list_users().await {
        Ok(users) => users,
        Err(e) => {
            error!("Failed to list users for watch sync: {}", e);
            report.request_failures += 1;
            return report;
        }
    };

    report.users_total = users.len();

    let admin_password = state.get_admin_password().await;
    let admin_password_hash: HashedPassword = admin_password.clone().into();

    for user in users {
        let mappings = match state.user_authorization.list_server_mappings(&user.id).await {
            Ok(mappings) => mappings,
            Err(e) => {
                error!(
                    "Failed to list server mappings for user '{}': {}",
                    user.original_username, e
                );
                report.request_failures += 1;
                continue;
            }
        };

        if mappings.is_empty() {
            continue;
        }

        report.users_with_mappings += 1;

        let mut sessions_by_server_id: HashMap<i64, AuthenticatedServerContext> = HashMap::new();
        for mapping in &mappings {
            let Some(server) = servers_by_url
                .get(&normalize_server_url(&mapping.server_url))
                .cloned()
            else {
                continue;
            };

            match authenticate_mapping(
                state,
                &user,
                mapping,
                &server,
                &admin_password,
                &admin_password_hash,
            )
            .await
            {
                Ok(session) => {
                    sessions_by_server_id
                        .insert(server.id, AuthenticatedServerContext { server, session });
                }
                Err(e) => {
                    warn!(
                        "Watch sync authentication failed for user '{}' on server '{}': {}",
                        user.original_username, server.name, e
                    );
                    report.authentication_failures += 1;
                }
            }
        }

        if sessions_by_server_id.is_empty() {
            continue;
        }

        report.users_authenticated += 1;

        for group in &resolved_groups {
            let mut candidates = group
                .members
                .iter()
                .filter_map(|member| {
                    sessions_by_server_id
                        .get(&member.server_id)
                        .map(|ctx| CandidateMember {
                            server: ctx.server.clone(),
                            session: ctx.session.clone(),
                            original_media_id: member.original_media_id.clone(),
                        })
                })
                .collect::<Vec<_>>();

            if candidates.len() < 2 {
                report.skipped_missing_sessions += 1;
                continue;
            }

            candidates.sort_by(|left, right| {
                right
                    .server
                    .priority
                    .cmp(&left.server.priority)
                    .then_with(|| left.server.id.cmp(&right.server.id))
            });

            let source = &candidates[0];
            let source_played = match fetch_item_played_state(state, source).await {
                Ok(Some(played)) => played,
                Ok(None) => {
                    report.skipped_missing_items += 1;
                    continue;
                }
                Err(e) => {
                    debug!(
                        "Failed to fetch source watched state from server '{}': {}",
                        source.server.name, e
                    );
                    report.request_failures += 1;
                    continue;
                }
            };

            for target in candidates.iter().skip(1) {
                report.sync_pairs_checked += 1;

                let target_played = match fetch_item_played_state(state, target).await {
                    Ok(Some(played)) => played,
                    Ok(None) => {
                        report.skipped_missing_items += 1;
                        continue;
                    }
                    Err(e) => {
                        debug!(
                            "Failed to fetch target watched state from server '{}': {}",
                            target.server.name, e
                        );
                        report.request_failures += 1;
                        continue;
                    }
                };

                if target_played == source_played {
                    report.already_in_sync += 1;
                    continue;
                }

                match apply_item_played_state(state, target, source_played).await {
                    Ok(true) => report.updates_applied += 1,
                    Ok(false) => report.skipped_missing_items += 1,
                    Err(e) => {
                        debug!(
                            "Failed to apply watched state on server '{}': {}",
                            target.server.name, e
                        );
                        report.request_failures += 1;
                    }
                }
            }
        }
    }

    report
}

async fn resolve_groups(
    state: &AppState,
    groups: Vec<MediaDedupeGroup>,
    report: &mut WatchSyncReport,
) -> Vec<ResolvedGroup> {
    let mut resolved = Vec::new();

    for group in groups {
        if group.members.len() < 2 {
            continue;
        }

        let mut resolved_members = Vec::new();

        for member in group.members {
            if let Some(resolved_member) = resolve_group_member(state, &member).await {
                resolved_members.push(resolved_member);
            } else {
                report.mapping_resolution_misses += 1;
            }
        }

        if resolved_members.len() >= 2 {
            resolved.push(ResolvedGroup {
                members: resolved_members,
            });
        }
    }

    resolved
}

async fn resolve_group_member(
    state: &AppState,
    member: &MediaDedupeMember,
) -> Option<ResolvedGroupMember> {
    let mapping = state
        .media_storage
        .get_media_mapping_by_virtual(&member.virtual_media_id)
        .await
        .ok()
        .flatten()?;

    Some(ResolvedGroupMember {
        server_id: member.server_id,
        original_media_id: mapping.original_media_id,
    })
}

async fn authenticate_mapping(
    state: &AppState,
    user: &User,
    mapping: &ServerMapping,
    server: &Server,
    admin_password: &Password,
    admin_password_hash: &HashedPassword,
) -> Result<AuthorizationSession, String> {
    let mapped_password = state.user_authorization.decrypt_server_mapping_password(
        mapping,
        &user.original_password_hash,
        admin_password_hash,
        None,
        Some(admin_password),
    );

    let client = JellyfinClient::new(server.url.as_str(), CLIENT_INFO.clone())
        .map_err(|e| format!("client init error: {e}"))?;

    let remote_user = client
        .authenticate_by_name(&mapping.mapped_username, mapped_password.as_str())
        .await
        .map_err(|e| format!("authentication error: {e}"))?;

    let token = client
        .get_token()
        .await
        .ok_or_else(|| "missing token after successful authentication".to_string())?;

    let sync_auth = Authorization {
        client: CLIENT_INFO.client.clone(),
        device: CLIENT_INFO.device.clone(),
        device_id: format!("{}-watch-sync", CLIENT_INFO.device_id),
        version: CLIENT_INFO.version.clone(),
        token: Some(token.clone()),
    };

    let session_id = state
        .user_authorization
        .store_authorization_session(
            &user.id,
            &mapping.server_url,
            &sync_auth,
            token.clone(),
            remote_user.id.clone(),
            None,
        )
        .await
        .map_err(|e| format!("failed to store authorization session: {e}"))?;

    let now = Utc::now();
    Ok(AuthorizationSession {
        id: session_id,
        user_id: user.id.clone(),
        mapping_id: mapping.id,
        server_url: mapping.server_url.clone(),
        device: Device {
            client: sync_auth.client,
            device: sync_auth.device,
            device_id: sync_auth.device_id,
            version: sync_auth.version,
        },
        jellyfin_token: token,
        original_user_id: remote_user.id,
        expires_at: None,
        created_at: now,
        updated_at: now,
    })
}

#[derive(Debug, Deserialize)]
struct ItemUserDataEnvelope {
    #[serde(rename = "UserData")]
    user_data: Option<ItemUserData>,
}

#[derive(Debug, Deserialize)]
struct ItemUserData {
    #[serde(rename = "Played")]
    played: Option<bool>,
}

async fn fetch_item_played_state(
    state: &AppState,
    candidate: &CandidateMember,
) -> Result<Option<bool>, String> {
    let mut url = join_server_url(
        &candidate.server.url,
        &format!(
            "/Users/{}/Items/{}",
            candidate.session.original_user_id, candidate.original_media_id
        ),
    );
    url.query_pairs_mut().append_pair("Fields", "UserData");

    let response = state
        .reqwest_client
        .get(url)
        .header(
            AUTHORIZATION,
            candidate.session.to_authorization().to_header_value(),
        )
        .send()
        .await
        .map_err(|e| format!("request error: {e}"))?;

    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("unexpected status {} ({})", status, body));
    }

    let payload = response
        .json::<ItemUserDataEnvelope>()
        .await
        .map_err(|e| format!("invalid item payload: {e}"))?;

    Ok(Some(
        payload
            .user_data
            .and_then(|user_data| user_data.played)
            .unwrap_or(false),
    ))
}

async fn apply_item_played_state(
    state: &AppState,
    candidate: &CandidateMember,
    played: bool,
) -> Result<bool, String> {
    let url = join_server_url(
        &candidate.server.url,
        &format!(
            "/Users/{}/PlayedItems/{}",
            candidate.session.original_user_id, candidate.original_media_id
        ),
    );

    let request = if played {
        state.reqwest_client.post(url)
    } else {
        state.reqwest_client.delete(url)
    }
    .header(
        AUTHORIZATION,
        candidate.session.to_authorization().to_header_value(),
    );

    let response = request
        .send()
        .await
        .map_err(|e| format!("request error: {e}"))?;

    let status = response.status();
    if status == StatusCode::NOT_FOUND {
        return Ok(false);
    }

    if status.is_success() {
        return Ok(true);
    }

    let body = response.text().await.unwrap_or_default();
    Err(format!("unexpected status {} ({})", status, body))
}

fn normalize_server_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}
