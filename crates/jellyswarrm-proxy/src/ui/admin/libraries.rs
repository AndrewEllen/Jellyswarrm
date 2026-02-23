use std::collections::{HashMap, HashSet};

use askama::Template;
use axum::{
    extract::{Path, RawForm, State},
    http::{HeaderMap, StatusCode},
    response::{Html, IntoResponse, Response},
};
use jellyfin_api::JellyfinClient;
use tracing::{error, info, warn};

use crate::{
    config::CLIENT_INFO,
    encryption::{decrypt_password, HashedPassword},
    library_management_service::{LibraryGroupWithSources, LibraryManagementService, NewLibraryGroupSource},
    AppState,
};

#[derive(Template)]
#[template(path = "admin/libraries.html")]
pub struct LibrariesPageTemplate {
    pub ui_route: String,
}

pub struct CollectionTypeOption {
    pub value: String,
    pub label: String,
}

pub struct SourceOption {
    pub key: String,
    pub server_id: i64,
    pub server_name: String,
    pub library_id: String,
    pub library_name: String,
    pub collection_type: String,
    pub auth_source: String,
}

pub struct GroupView {
    pub id: i64,
    pub name: String,
    pub virtual_library_id: String,
    pub collection_type: String,
    pub source_descriptions: Vec<String>,
    pub source_choices: Vec<GroupSourceChoice>,
}

pub struct GroupSourceChoice {
    pub key: String,
    pub server_name: String,
    pub library_name: String,
    pub collection_type: String,
    pub auth_source: String,
    pub checked: bool,
}

pub struct ServerDiscoveryStatus {
    pub server_name: String,
    pub auth_source: Option<String>,
    pub message: Option<String>,
    pub library_count: usize,
}

#[derive(Template)]
#[template(path = "admin/library_list.html")]
pub struct LibrariesListTemplate {
    pub ui_route: String,
    pub collection_types: Vec<CollectionTypeOption>,
    pub source_options: Vec<SourceOption>,
    pub groups: Vec<GroupView>,
    pub server_statuses: Vec<ServerDiscoveryStatus>,
    pub flash_message: Option<String>,
    pub flash_is_error: bool,
}

pub struct LibraryForm {
    pub name: String,
    pub collection_type: String,
    pub sources: Vec<String>,
}

pub async fn libraries_page(State(state): State<AppState>) -> impl IntoResponse {
    let template = LibrariesPageTemplate {
        ui_route: state.get_ui_route().await,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            error!("Failed to render libraries page template: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Template error").into_response()
        }
    }
}

pub async fn get_library_list(State(state): State<AppState>) -> impl IntoResponse {
    match render_library_list(&state, None, false).await {
        Ok(html) => Html(html).into_response(),
        Err(e) => {
            error!("Failed to render library list: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Template error").into_response()
        }
    }
}

pub async fn add_library(
    State(state): State<AppState>,
    headers: HeaderMap,
    RawForm(raw_form): RawForm,
) -> Response {
    let form = match parse_library_form(raw_form.as_ref()) {
        Ok(form) => form,
        Err(message) => {
            return render_library_list_with_status(
                &state,
                Some(message),
                true,
                StatusCode::BAD_REQUEST,
                is_htmx_request(&headers),
            )
            .await;
        }
    };

    handle_save_library(&state, None, form, is_htmx_request(&headers)).await
}

pub async fn update_library(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(group_id): Path<i64>,
    RawForm(raw_form): RawForm,
) -> Response {
    let form = match parse_library_form(raw_form.as_ref()) {
        Ok(form) => form,
        Err(message) => {
            return render_library_list_with_status(
                &state,
                Some(message),
                true,
                StatusCode::BAD_REQUEST,
                is_htmx_request(&headers),
            )
            .await;
        }
    };

    handle_save_library(&state, Some(group_id), form, is_htmx_request(&headers)).await
}

pub async fn delete_library(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(group_id): Path<i64>,
) -> Response {
    let is_htmx = is_htmx_request(&headers);
    match state.library_management.delete_group(group_id).await {
        Ok(true) => {
            info!("Deleted grouped library {}", group_id);
            match render_library_list(
                &state,
                Some("Library group deleted".to_string()),
                false,
            )
            .await
            {
                Ok(html) => Html(html).into_response(),
                Err(e) => {
                    error!("Failed to render updated library list after delete: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Template error").into_response()
                }
            }
        }
        Ok(false) => {
            match render_library_list(&state, Some("Library group not found".to_string()), true).await
            {
                Ok(html) => {
                    (normalize_htmx_status(StatusCode::NOT_FOUND, is_htmx), Html(html))
                        .into_response()
                }
                Err(e) => {
                    error!("Failed to render library list after missing delete: {}", e);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Template error").into_response()
                }
            }
        }
        Err(e) => {
            error!("Failed to delete grouped library {}: {}", group_id, e);
            match render_library_list(
                &state,
                Some("Failed to delete library group".to_string()),
                true,
            )
            .await
            {
                Ok(html) => (
                    normalize_htmx_status(StatusCode::INTERNAL_SERVER_ERROR, is_htmx),
                    Html(html),
                )
                    .into_response(),
                Err(render_err) => {
                    error!("Failed to render library list after delete error: {}", render_err);
                    (StatusCode::INTERNAL_SERVER_ERROR, "Template error").into_response()
                }
            }
        }
    }
}

async fn handle_save_library(
    state: &AppState,
    group_id: Option<i64>,
    form: LibraryForm,
    is_htmx: bool,
) -> Response {
    let (source_options, _) = discover_source_libraries(state).await;
    let mut source_map = HashMap::with_capacity(source_options.len());
    for source in source_options {
        source_map.insert(source.key.clone(), source);
    }

    let normalized_collection_type =
        LibraryManagementService::normalize_collection_type(&form.collection_type);
    if !LibraryManagementService::is_valid_collection_type(&normalized_collection_type) {
        return render_library_list_with_status(
            state,
            Some("Invalid collection type".to_string()),
            true,
            StatusCode::BAD_REQUEST,
            is_htmx,
        )
        .await;
    }

    let sources = match sources_from_form(&form, &source_map, &normalized_collection_type) {
        Ok(s) => s,
        Err(msg) => {
            return render_library_list_with_status(
                state,
                Some(msg),
                true,
                StatusCode::BAD_REQUEST,
                is_htmx,
            )
            .await;
        }
    };

    let result = if let Some(id) = group_id {
        state
            .library_management
            .update_group(id, &form.name, &normalized_collection_type, sources)
            .await
            .map(|res| res.map(|_| "Library group updated".to_string()))
    } else {
        state
            .library_management
            .create_group(&form.name, &normalized_collection_type, sources)
            .await
            .map(|_| Some("Library group created".to_string()))
    };

    match result {
        Ok(Some(message)) => {
            render_library_list_with_status(state, Some(message), false, StatusCode::OK, is_htmx)
                .await
        }
        Ok(None) => {
            render_library_list_with_status(
                state,
                Some("Library group not found".to_string()),
                true,
                StatusCode::NOT_FOUND,
                is_htmx,
            )
            .await
        }
        Err(e) => {
            error!("Failed to save grouped library: {}", e);
            render_library_list_with_status(
                state,
                Some(format!("Failed to save library group: {}", e)),
                true,
                StatusCode::BAD_REQUEST,
                is_htmx,
            )
            .await
        }
    }
}

async fn render_library_list_with_status(
    state: &AppState,
    message: Option<String>,
    is_error: bool,
    status: StatusCode,
    is_htmx: bool,
) -> Response {
    match render_library_list(state, message, is_error).await {
        Ok(html) => (normalize_htmx_status(status, is_htmx), Html(html)).into_response(),
        Err(e) => {
            error!("Failed to render library list with status: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, "Template error").into_response()
        }
    }
}

fn is_htmx_request(headers: &HeaderMap) -> bool {
    headers
        .get("HX-Request")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn normalize_htmx_status(status: StatusCode, is_htmx: bool) -> StatusCode {
    if is_htmx && !status.is_success() {
        StatusCode::OK
    } else {
        status
    }
}

fn parse_library_form(raw_form: &[u8]) -> Result<LibraryForm, String> {
    let mut name = None;
    let mut collection_type = None;
    let mut sources = Vec::new();

    for (key, value) in url::form_urlencoded::parse(raw_form) {
        match key.as_ref() {
            "name" => name = Some(value.into_owned()),
            "collection_type" => collection_type = Some(value.into_owned()),
            "sources" | "sources[]" => sources.push(value.into_owned()),
            _ => {}
        }
    }

    let name = name.unwrap_or_default();
    let collection_type = collection_type.unwrap_or_default();

    Ok(LibraryForm {
        name,
        collection_type,
        sources,
    })
}

async fn render_library_list(
    state: &AppState,
    flash_message: Option<String>,
    flash_is_error: bool,
) -> Result<String, String> {
    let (source_options, server_statuses) = discover_source_libraries(state).await;
    let groups = state
        .library_management
        .list_groups_with_sources()
        .await
        .map_err(|e| format!("Failed to list grouped libraries: {}", e))?;

    let servers = state
        .server_storage
        .list_servers()
        .await
        .map_err(|e| format!("Failed to list servers: {}", e))?;
    let server_name_by_id: HashMap<i64, String> =
        servers.into_iter().map(|s| (s.id, s.name)).collect();

    let group_views = groups_to_view(groups, &server_name_by_id, &source_options);
    let collection_types = collection_type_options();

    let template = LibrariesListTemplate {
        ui_route: state.get_ui_route().await,
        collection_types,
        source_options,
        groups: group_views,
        server_statuses,
        flash_message,
        flash_is_error,
    };

    template.render().map_err(|e| e.to_string())
}

fn groups_to_view(
    groups: Vec<LibraryGroupWithSources>,
    server_name_by_id: &HashMap<i64, String>,
    source_options: &[SourceOption],
) -> Vec<GroupView> {
    groups
        .into_iter()
        .map(|group| {
            let selected_source_keys = group
                .sources
                .iter()
                .map(|source| source_key(source.server_id, &source.source_library_id))
                .collect::<Vec<_>>();

            let source_descriptions = group
                .sources
                .iter()
                .map(|source| {
                    let server_name = server_name_by_id
                        .get(&source.server_id)
                        .cloned()
                        .unwrap_or_else(|| format!("Server {}", source.server_id));
                    format!(
                        "{} - {} ({})",
                        server_name, source.source_library_name, source.source_collection_type
                    )
                })
                .collect::<Vec<_>>();

            let source_choices = source_options
                .iter()
                .map(|source| GroupSourceChoice {
                    key: source.key.clone(),
                    server_name: source.server_name.clone(),
                    library_name: source.library_name.clone(),
                    collection_type: source.collection_type.clone(),
                    auth_source: source.auth_source.clone(),
                    checked: selected_source_keys.contains(&source.key),
                })
                .collect::<Vec<_>>();

            GroupView {
                id: group.group.id,
                name: group.group.name,
                virtual_library_id: group.group.virtual_library_id,
                collection_type: group.group.collection_type,
                source_descriptions,
                source_choices,
            }
        })
        .collect()
}

fn collection_type_options() -> Vec<CollectionTypeOption> {
    LibraryManagementService::allowed_collection_types()
        .iter()
        .map(|t| CollectionTypeOption {
            value: t.to_string(),
            label: collection_type_label(t).to_string(),
        })
        .collect()
}

fn collection_type_label(value: &str) -> &'static str {
    match value {
        "movies" => "Movies",
        "tvshows" => "TV Shows",
        "music" => "Music",
        "musicvideos" => "Music Videos",
        "trailers" => "Trailers",
        "homevideos" => "Home Videos",
        "boxsets" => "Box Sets",
        "books" => "Books",
        "photos" => "Photos",
        "livetv" => "Live TV",
        "playlists" => "Playlists",
        "folders" => "Folders",
        _ => "Unknown",
    }
}

fn source_key(server_id: i64, library_id: &str) -> String {
    format!("{}::{}", server_id, library_id)
}

fn sources_from_form(
    form: &LibraryForm,
    source_map: &HashMap<String, SourceOption>,
    normalized_collection_type: &str,
) -> Result<Vec<NewLibraryGroupSource>, String> {
    if form.name.trim().is_empty() {
        return Err("Library name cannot be empty".to_string());
    }

    if form.sources.is_empty() {
        return Err("Select at least one source library".to_string());
    }

    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for source_key in &form.sources {
        if !seen.insert(source_key.clone()) {
            continue;
        }

        let source = source_map
            .get(source_key)
            .ok_or_else(|| format!("Unknown source selection: {}", source_key))?;

        let source_collection_type =
            LibraryManagementService::normalize_collection_type(&source.collection_type);
        if source_collection_type != normalized_collection_type {
            return Err(format!(
                "Source '{}' has type '{}' but group type is '{}'",
                source.library_name, source_collection_type, normalized_collection_type
            ));
        }

        out.push(NewLibraryGroupSource {
            server_id: source.server_id,
            source_library_id: source.library_id.clone(),
            source_library_name: source.library_name.clone(),
            source_collection_type: source.collection_type.clone(),
        });
    }

    if out.is_empty() {
        return Err("Select at least one source library".to_string());
    }

    Ok(out)
}

async fn discover_source_libraries(
    state: &AppState,
) -> (Vec<SourceOption>, Vec<ServerDiscoveryStatus>) {
    let servers = match state.server_storage.list_servers().await {
        Ok(servers) => servers,
        Err(e) => {
            error!("Failed to list servers for library discovery: {}", e);
            return (
                Vec::new(),
                vec![ServerDiscoveryStatus {
                    server_name: "Server list unavailable".to_string(),
                    auth_source: None,
                    message: Some("Failed to list configured servers".to_string()),
                    library_count: 0,
                }],
            );
        }
    };

    let mut discovered = Vec::new();
    let mut statuses = Vec::new();
    for server in servers {
        match discover_libraries_for_server(state, &server).await {
            Ok((auth_source, mut options)) => {
                let count = options.len();
                discovered.append(&mut options);
                statuses.push(ServerDiscoveryStatus {
                    server_name: server.name.clone(),
                    auth_source: Some(auth_source),
                    message: None,
                    library_count: count,
                });
            }
            Err(msg) => statuses.push(ServerDiscoveryStatus {
                server_name: server.name.clone(),
                auth_source: None,
                message: Some(msg),
                library_count: 0,
            }),
        }
    }

    discovered.sort_by(|a, b| {
        a.server_name
            .cmp(&b.server_name)
            .then_with(|| a.collection_type.cmp(&b.collection_type))
            .then_with(|| a.library_name.cmp(&b.library_name))
    });
    statuses.sort_by(|a, b| a.server_name.cmp(&b.server_name));
    (discovered, statuses)
}

async fn discover_libraries_for_server(
    state: &AppState,
    server: &crate::server_storage::Server,
) -> Result<(String, Vec<SourceOption>), String> {
    let client_info = CLIENT_INFO.clone();
    let client = JellyfinClient::new(server.url.as_str(), client_info)
        .map_err(|e| format!("Client setup failed: {}", e))?;

    // Prefer configured server admin credentials when available.
    if let Ok(Some(admin)) = state.server_storage.get_server_admin(server.id).await {
        let admin_password = state.get_admin_password().await;
        let admin_password_hash: HashedPassword = (&admin_password).into();
        match decrypt_password(&admin.password, &admin_password_hash) {
            Ok(password) => {
                if client
                    .authenticate_by_name(&admin.username, password.as_str())
                    .await
                    .is_ok()
                {
                    match client.get_media_folders(None).await {
                        Ok(folders) => {
                            let options = folders_to_source_options(
                                server.id,
                                &server.name,
                                "admin",
                                folders,
                            );
                            return Ok(("admin".to_string(), options));
                        }
                        Err(e) => {
                            warn!(
                                "Failed to fetch admin library folders for server {}: {}",
                                server.name, e
                            );
                        }
                    }
                } else {
                    warn!(
                        "Admin authentication failed for library discovery on server {}",
                        server.name
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Failed to decrypt admin credentials for server {}: {}",
                    server.name, e
                );
            }
        }
    }

    let latest_session = state
        .user_authorization
        .get_latest_active_session_for_server(server.url.as_str())
        .await
        .map_err(|e| format!("Failed to fetch latest active session: {}", e))?;

    if let Some(session) = latest_session {
        client.with_token(session.jellyfin_token.clone()).await;
        let folders = client
            .get_media_folders(Some(&session.original_user_id))
            .await
            .map_err(|e| format!("Failed to fetch libraries with mapped user session: {}", e))?;

        let options = folders_to_source_options(server.id, &server.name, "session", folders);
        return Ok(("session".to_string(), options));
    }

    Err("No usable admin credentials or active mapped-user session".to_string())
}

fn folders_to_source_options(
    server_id: i64,
    server_name: &str,
    auth_source: &str,
    folders: Vec<jellyfin_api::models::MediaFolder>,
) -> Vec<SourceOption> {
    folders
        .into_iter()
        .filter_map(|folder| {
            let collection_type = folder
                .collection_type
                .as_deref()
                .map(LibraryManagementService::normalize_collection_type)?;

            if collection_type.is_empty()
                || !LibraryManagementService::is_valid_collection_type(&collection_type)
            {
                return None;
            }

            let key = source_key(server_id, &folder.id);
            Some(SourceOption {
                key,
                server_id,
                server_name: server_name.to_string(),
                library_id: folder.id,
                library_name: folder.name,
                collection_type,
                auth_source: auth_source.to_string(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_library_form;

    #[test]
    fn parse_library_form_accepts_repeated_sources_keys() {
        let raw = b"name=Movies&collection_type=movies&sources=1%3A%3Aabc&sources=2%3A%3Adef";
        let form = parse_library_form(raw).expect("should parse");
        assert_eq!(form.name, "Movies");
        assert_eq!(form.collection_type, "movies");
        assert_eq!(
            form.sources,
            vec!["1::abc".to_string(), "2::def".to_string()]
        );
    }

    #[test]
    fn parse_library_form_accepts_bracketed_sources_keys() {
        let raw =
            b"name=Movies&collection_type=movies&sources%5B%5D=1%3A%3Aabc&sources%5B%5D=2%3A%3Adef";
        let form = parse_library_form(raw).expect("should parse");
        assert_eq!(
            form.sources,
            vec!["1::abc".to_string(), "2::def".to_string()]
        );
    }
}
