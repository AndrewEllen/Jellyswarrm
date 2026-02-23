use std::collections::HashMap;

use axum::{
    extract::{Request, State},
    Json,
};
use hyper::StatusCode;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, TRANSFER_ENCODING};
use reqwest::Body;
use tokio::task::JoinSet;
use tracing::{debug, error};

use crate::{
    handlers::{
        common::{
            execute_json_request, payload_from_request, process_media_item, process_media_source,
            track_play_session,
        },
        federated::{merge_server_item_batches, sort_options_from_url, ServerItemsBatch},
    },
    models::{
        enums::{BaseItemKind, CollectionType},
        ItemsResponseVariants, ItemsResponseWithCount, MediaItem, MediaSource, PlaybackRequest,
        PlaybackResponse,
    },
    request_preprocessing::{
        apply_to_request, extract_request_infos, preprocess_request, JellyfinAuthorization,
    },
    server_storage::Server,
    url_helper::{contains_id, replace_id},
    user_authorization_service::AuthorizationSession,
    AppState,
};

//http://localhost:3000/Users/7bc57a386ab84999ad7262210a9cd253/Items/5f7e146c44d84b479cafecd3280be4ea
//http://localhost:3000/Items/430c368c5eb34534bf98363d5adbb92f?userId=520ea298ed8044338a28d912523d715f
pub async fn get_item(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<MediaItem>, StatusCode> {
    let preprocessed = preprocess_request(req, &state).await.map_err(|e| {
        error!("Failed to preprocess request: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    let server = preprocessed.server.clone();
    let requested_item_id = preprocessed
        .original_request
        .as_ref()
        .and_then(|request| extract_item_id_from_path(request.url().path()));

    let grouped_library_override = if let Some(requested_item_id) = requested_item_id.clone() {
        match state
            .library_management
            .get_group_by_virtual_id_with_sources(&requested_item_id)
            .await
        {
            Ok(Some(group)) => Some((
                requested_item_id,
                group.group.name,
                parse_collection_type(&group.group.collection_type),
            )),
            Ok(None) => None,
            Err(e) => {
                error!(
                    "Failed to load grouped library metadata for item override: {}",
                    e
                );
                None
            }
        }
    } else {
        None
    };

    if let Some(requested_item_id) = requested_item_id.as_deref() {
        if let Some(mut grouped_item) =
            try_get_grouped_item_with_sources(&state, &preprocessed, requested_item_id).await?
        {
            if let Some((group_virtual_id, group_name, group_collection_type)) = grouped_library_override {
                grouped_item.id = group_virtual_id.clone();
                grouped_item.name = Some(group_name);
                grouped_item.collection_type = Some(group_collection_type);
                grouped_item.item_type = BaseItemKind::CollectionFolder;
                grouped_item.is_folder = Some(true);
                grouped_item.display_preferences_id = Some(group_virtual_id);
            }

            return Ok(Json(grouped_item));
        }
    }

    match execute_json_request::<MediaItem>(&state.reqwest_client, preprocessed.request).await {
        Ok(media_item) => {
            let server_id = { state.config.read().await.server_id.clone() };
            let mut processed_item =
                process_media_item(media_item, &state, &server, false, &server_id).await?;

            if let Some((group_virtual_id, group_name, group_collection_type)) =
                grouped_library_override
            {
                processed_item.id = group_virtual_id.clone();
                processed_item.name = Some(group_name);
                processed_item.collection_type = Some(group_collection_type);
                processed_item.item_type = BaseItemKind::CollectionFolder;
                processed_item.is_folder = Some(true);
                processed_item.display_preferences_id = Some(group_virtual_id);
            }

            Ok(Json(processed_item))
        }
        Err(e) => {
            error!("Failed to get MediaItem: {:?}", e);
            Err(e)
        }
    }
}

fn parse_collection_type(value: &str) -> CollectionType {
    serde_json::from_value::<CollectionType>(serde_json::Value::String(value.to_string()))
        .unwrap_or_else(|_| CollectionType::UnknownVariant(value.to_string()))
}

fn extract_item_id_from_path(path: &str) -> Option<String> {
    let segments: Vec<&str> = path.split('/').filter(|segment| !segment.is_empty()).collect();

    for index in 0..segments.len() {
        if segments[index].eq_ignore_ascii_case("items") && (index + 1) < segments.len() {
            return Some(segments[index + 1].to_string());
        }
    }

    None
}

//http://localhost:3000/Users/7bc57a386ab84999ad7262210a9cd253/Items?SortBy=SortName%2CProductionYear&SortOrder=Ascending&IncludeItemTypes=Movie&Recursive=true&Fields=PrimaryImageAspectRatio%2CMediaSourceCount&ImageTypeLimit=1&EnableImageTypes=Primary%2CBackdrop%2CBanner%2CThumb&StartIndex=0&ParentId=5f7e146c44d84b479cafecd3280be4ea&Limit=100
//http://localhost:3000/Items/430c368c5eb34534bf98363d5adbb92f/Similar?userId=520ea298ed8044338a28d912523d715f&limit=12&fields=PrimaryImageAspectRatio%2CCanDelete
pub async fn get_items(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<crate::models::ItemsResponseVariants>, StatusCode> {
    if let Some(series_virtual_id) = extract_show_series_id_from_path(req.uri().path()) {
        return get_show_items_with_optional_dedupe(State(state), req, series_virtual_id).await;
    }

    get_items_single_server(State(state), req).await
}

async fn get_items_single_server(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<crate::models::ItemsResponseVariants>, StatusCode> {
    let preprocessed = preprocess_request(req, &state).await.map_err(|e| {
        error!("Failed to preprocess request: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    let server = preprocessed.server;

    match execute_json_request::<crate::models::ItemsResponseVariants>(
        &state.reqwest_client,
        preprocessed.request,
    )
    .await
    {
        Ok(mut response) => {
            let server_id = { state.config.read().await.server_id.clone() };
            for item in &mut response.iter_mut_items() {
                *item =
                    process_media_item(item.clone(), &state, &server, false, &server_id).await?;
            }

            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get ItemsResponse: {:?}", e);
            Err(e)
        }
    }
}

async fn get_show_items_with_optional_dedupe(
    State(state): State<AppState>,
    req: Request,
    series_virtual_id: String,
) -> Result<Json<crate::models::ItemsResponseVariants>, StatusCode> {
    let Some(series_group) = state.media_storage.get_media_dedupe_group(&series_virtual_id).await else {
        return get_items_single_server(State(state), req).await;
    };

    if series_group.members.len() < 2 {
        return get_items_single_server(State(state), req).await;
    }

    let (original_request, _, _, sessions, _) =
        extract_request_infos(req, &state).await.map_err(|e| {
            error!("Failed to preprocess deduped show items request: {}", e);
            StatusCode::BAD_REQUEST
        })?;

    let sessions = sessions.ok_or(StatusCode::UNAUTHORIZED)?;
    if sessions.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let mut sessions_by_server: HashMap<i64, (AuthorizationSession, Server)> = HashMap::new();
    for (session, server) in sessions {
        sessions_by_server.entry(server.id).or_insert((session, server));
    }

    let mut targets = series_group
        .members
        .iter()
        .filter_map(|member| sessions_by_server.get(&member.server_id).cloned())
        .collect::<Vec<_>>();

    if let Some(season_virtual_id) = extract_query_parameter(original_request.url(), "SeasonId") {
        if let Some(season_group) = state.media_storage.get_media_dedupe_group(&season_virtual_id).await {
            targets.retain(|(_, server)| {
                season_group
                    .members
                    .iter()
                    .any(|member| member.server_id == server.id)
            });
        } else if let Some((_, season_server)) = state
            .media_storage
            .get_media_mapping_with_server(&season_virtual_id)
            .await
            .map_err(|e| {
                error!("Failed to resolve season server mapping: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
        {
            targets.retain(|(_, server)| server.id == season_server.id);
        }
    }

    if targets.is_empty() {
        return Ok(Json(ItemsResponseVariants::WithCount(ItemsResponseWithCount {
            items: Vec::new(),
            total_record_count: 0,
            start_index: 0,
        })));
    }

    let mut join_set = JoinSet::new();
    for (index, (session, server)) in targets.into_iter().enumerate() {
        let Some(request) = original_request.try_clone() else {
            continue;
        };
        let state_clone = state.clone();
        join_set.spawn(async move {
            let result =
                fetch_show_items_for_target(state_clone, request, session, server.clone()).await;
            (index, server, result)
        });
    }

    let mut indexed_batches = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok((index, server, Ok(items))) => {
                indexed_batches.push((index, ServerItemsBatch { server, items }))
            }
            Ok((_index, server, Err(status))) => {
                error!(
                    "Failed to fetch deduped show items from server '{}': {}",
                    server.name, status
                );
            }
            Err(e) => error!("Join error while fetching deduped show items: {}", e),
        }
    }

    indexed_batches.sort_by_key(|(index, _)| *index);
    let batches = indexed_batches
        .into_iter()
        .map(|(_, batch)| batch)
        .collect::<Vec<_>>();

    let sort_options = sort_options_from_url(original_request.url());
    let merged = merge_server_item_batches(&state, batches, sort_options.as_ref()).await;
    Ok(Json(merged))
}

async fn fetch_show_items_for_target(
    state: AppState,
    mut request: reqwest::Request,
    session: AuthorizationSession,
    server: Server,
) -> Result<ItemsResponseVariants, StatusCode> {
    let auth = Some(JellyfinAuthorization::Authorization(session.to_authorization()));
    apply_to_request(
        &mut request,
        &server,
        &Some(session),
        &auth,
        &state,
    )
    .await;

    let mut response =
        execute_json_request::<ItemsResponseVariants>(&state.reqwest_client, request).await?;
    let server_id = { state.config.read().await.server_id.clone() };
    for item in &mut response.iter_mut_items() {
        *item = process_media_item(item.clone(), &state, &server, false, &server_id).await?;
    }

    Ok(response)
}

// can be used for special features etc.
pub async fn get_items_list(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<Vec<MediaItem>>, StatusCode> {
    let preprocessed = preprocess_request(req, &state).await.map_err(|e| {
        error!("Failed to preprocess request: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    let server = preprocessed.server;

    match execute_json_request::<Vec<MediaItem>>(&state.reqwest_client, preprocessed.request).await
    {
        Ok(mut response) => {
            let server_id = { state.config.read().await.server_id.clone() };
            for item in &mut response {
                *item =
                    process_media_item(item.clone(), &state, &server, false, &server_id).await?;
            }

            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get Vec<MediaItem>: {:?}", e);
            Err(e)
        }
    }
}

//http://192.168.188.142:30013/Items/165a66aa5bd2e62c0df0f8da332ae47d/PlaybackInfo
#[axum::debug_handler]
pub async fn post_playback_info(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<PlaybackResponse>, StatusCode> {
    let preprocessed = preprocess_request(req, &state).await.map_err(|e| {
        error!("Failed to preprocess request: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    let original_request = preprocessed
        .original_request
        .as_ref()
        .ok_or(StatusCode::BAD_REQUEST)?;
    let payload: PlaybackRequest = payload_from_request(original_request)?;

    if let Some(grouped_playback_response) =
        try_get_grouped_playback_info(&state, &preprocessed, original_request, &payload).await?
    {
        return Ok(Json(grouped_playback_response));
    }

    let server = preprocessed.server;

    let session = preprocessed.session.ok_or(StatusCode::UNAUTHORIZED)?;

    let mut payload = payload;
    if payload.user_id.is_some() {
        payload.user_id = Some(session.original_user_id.clone());
    }

    if let Some(media_source_id) = &payload.media_source_id {
        if let Some(media_mapping) = state
            .media_storage
            .get_media_mapping_by_virtual(media_source_id)
            .await
            .unwrap_or_default()
        {
            payload.media_source_id = Some(media_mapping.original_media_id);
        }
    }

    debug!("Forwarding PlaybackRequest JSON: {:?}", &payload);

    let json = serde_json::to_vec(&payload).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Set body as a full buffer and provide Content-Length so upstream servers
    // don't wait for chunked data (which can trigger MinRequestBodyDataRate errors).
    let len = json.len();
    let mut request = preprocessed.request;
    *request.body_mut() = Some(Body::from(json));
    // Ensure Content-Length is set and remove Transfer-Encoding if present.
    request.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string()).unwrap(),
    );
    request.headers_mut().remove(TRANSFER_ENCODING);

    match execute_json_request::<PlaybackResponse>(&state.reqwest_client, request).await {
        Ok(mut response) => {
            for item in &mut response.media_sources {
                *item = process_media_source(item.clone(), &state.media_storage, &server).await?;
                track_play_session(item, &response.play_session_id, &server, &state).await?;
            }

            debug!("Requested Playback: {:?}", response);

            Ok(Json(response))
        }
        Err(e) => {
            error!("Failed to get playback info: {:?}", e);
            Err(e)
        }
    }
}

async fn try_get_grouped_playback_info(
    state: &AppState,
    preprocessed: &crate::request_preprocessing::PreprocessedRequest,
    original_request: &reqwest::Request,
    payload: &PlaybackRequest,
) -> Result<Option<PlaybackResponse>, StatusCode> {
    let Some(requested_item_id) = extract_item_id_from_path(original_request.url().path()) else {
        return Ok(None);
    };

    let Some(dedupe_group) = state.media_storage.get_media_dedupe_group(&requested_item_id).await else {
        return Ok(None);
    };

    if dedupe_group.members.len() < 2 {
        return Ok(None);
    }

    let Some(sessions) = preprocessed.sessions.clone() else {
        return Ok(None);
    };

    let mut sessions_by_server: HashMap<i64, (AuthorizationSession, Server)> = HashMap::new();
    for (session, server) in sessions {
        sessions_by_server.entry(server.id).or_insert((session, server));
    }

    let selected_server_id = if let Some(media_source_id) = payload.media_source_id.as_ref() {
        state
            .media_storage
            .get_media_mapping_with_server(media_source_id)
            .await
            .map_err(|e| {
                error!("Failed to resolve media source mapping: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .map(|(_, server)| server.id)
    } else {
        None
    };

    let mut candidates = dedupe_group
        .members
        .iter()
        .filter_map(|member| {
            sessions_by_server
                .get(&member.server_id)
                .map(|(session, server)| (member.virtual_media_id.clone(), session.clone(), server.clone()))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|a, b| {
        b.2.priority
            .cmp(&a.2.priority)
            .then_with(|| a.2.name.cmp(&b.2.name))
    });

    if let Some(selected_server_id) = selected_server_id {
        candidates.retain(|(_, _, server)| server.id == selected_server_id);
    }

    if candidates.is_empty() {
        return Ok(None);
    }

    let mut merged_sources = Vec::new();
    let mut play_session_id = None;

    for (item_virtual_id, session, server) in candidates {
        let Some(mut request) = original_request.try_clone() else {
            continue;
        };

        replace_item_id_for_request(&mut request, &item_virtual_id);

        let mut payload_for_server = payload.clone();
        if payload_for_server.user_id.is_some() {
            payload_for_server.user_id = Some(session.original_user_id.clone());
        }

        if let Some(selected_server_id) = selected_server_id {
            if server.id == selected_server_id {
                if let Some(media_source_id) = payload_for_server.media_source_id.as_ref() {
                    if let Some(mapping) = state
                        .media_storage
                        .get_media_mapping_by_virtual(media_source_id)
                        .await
                        .map_err(|e| {
                            error!("Failed to map selected media source: {}", e);
                            StatusCode::INTERNAL_SERVER_ERROR
                        })?
                    {
                        payload_for_server.media_source_id = Some(mapping.original_media_id);
                    }
                }
            } else {
                payload_for_server.media_source_id = None;
            }
        } else {
            payload_for_server.media_source_id = None;
        }

        let auth = Some(JellyfinAuthorization::Authorization(session.to_authorization()));
        apply_to_request(
            &mut request,
            &server,
            &Some(session),
            &auth,
            state,
        )
        .await;

        apply_json_body_to_request(&mut request, &payload_for_server)?;

        let mut response =
            execute_json_request::<PlaybackResponse>(&state.reqwest_client, request).await?;

        if play_session_id.is_none() {
            play_session_id = Some(response.play_session_id.clone());
        }

        for source in &mut response.media_sources {
            *source = process_media_source(source.clone(), &state.media_storage, &server).await?;

            source.name = Some(match source.name.as_deref() {
                Some(existing) if !existing.trim().is_empty() => {
                    format!("{existing} [{}]", server.name)
                }
                _ => format!("{} source", server.name),
            });

            track_play_session(source, &response.play_session_id, &server, state).await?;
            merged_sources.push(source.clone());
        }
    }

    if merged_sources.is_empty() {
        return Ok(None);
    }

    Ok(Some(PlaybackResponse {
        media_sources: merged_sources,
        play_session_id: play_session_id.unwrap_or_default(),
    }))
}

fn apply_json_body_to_request(
    request: &mut reqwest::Request,
    payload: &PlaybackRequest,
) -> Result<(), StatusCode> {
    let json = serde_json::to_vec(payload).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let len = json.len();
    *request.body_mut() = Some(Body::from(json));
    request.headers_mut().insert(
        CONTENT_LENGTH,
        HeaderValue::from_str(&len.to_string()).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    request.headers_mut().remove(TRANSFER_ENCODING);
    Ok(())
}

fn replace_item_id_for_request(request: &mut reqwest::Request, item_virtual_id: &str) {
    let mut url = request.url().clone();
    if let Some(current_item_id) = contains_id(&url, "Items") {
        url = replace_id(url, &current_item_id, item_virtual_id);
    }
    *request.url_mut() = url;
}

fn extract_show_series_id_from_path(path: &str) -> Option<String> {
    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    for index in 0..segments.len() {
        if segments[index].eq_ignore_ascii_case("shows")
            && (index + 2) < segments.len()
            && (segments[index + 2].eq_ignore_ascii_case("seasons")
                || segments[index + 2].eq_ignore_ascii_case("episodes"))
        {
            return Some(segments[index + 1].to_string());
        }
    }

    None
}

fn extract_query_parameter(url: &url::Url, parameter_name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key.eq_ignore_ascii_case(parameter_name))
        .and_then(|(_, value)| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
}

async fn try_get_grouped_item_with_sources(
    state: &AppState,
    preprocessed: &crate::request_preprocessing::PreprocessedRequest,
    requested_item_id: &str,
) -> Result<Option<MediaItem>, StatusCode> {
    let Some(group) = state.media_storage.get_media_dedupe_group(requested_item_id).await else {
        return Ok(None);
    };
    if group.members.len() < 2 {
        return Ok(None);
    }

    let Some(original_request) = preprocessed.original_request.as_ref() else {
        return Ok(None);
    };
    let Some(sessions) = preprocessed.sessions.clone() else {
        return Ok(None);
    };

    let mut sessions_by_server: HashMap<i64, (AuthorizationSession, Server)> = HashMap::new();
    for (session, server) in sessions {
        sessions_by_server.entry(server.id).or_insert((session, server));
    }

    let mut candidates = group
        .members
        .iter()
        .filter_map(|member| {
            sessions_by_server
                .get(&member.server_id)
                .map(|(session, server)| (member.virtual_media_id.clone(), session.clone(), server.clone()))
        })
        .collect::<Vec<_>>();

    candidates.sort_by(|a, b| {
        b.2.priority
            .cmp(&a.2.priority)
            .then_with(|| a.2.name.cmp(&b.2.name))
    });

    let server_id = { state.config.read().await.server_id.clone() };
    let mut canonical_item: Option<(i32, String, MediaItem)> = None;
    let mut merged_sources: Vec<MediaSource> = Vec::new();
    let mut seen_source_ids = std::collections::HashSet::new();

    for (item_virtual_id, session, server) in candidates {
        let Some(mut request) = original_request.try_clone() else {
            continue;
        };
        replace_item_id_for_request(&mut request, &item_virtual_id);

        let auth = Some(JellyfinAuthorization::Authorization(session.to_authorization()));
        apply_to_request(
            &mut request,
            &server,
            &Some(session),
            &auth,
            state,
        )
        .await;

        let response_item =
            match execute_json_request::<MediaItem>(&state.reqwest_client, request).await {
                Ok(item) => item,
                Err(status) => {
                    error!(
                        "Failed to fetch grouped item details from server '{}': {}",
                        server.name, status
                    );
                    continue;
                }
            };

        let mut processed_item =
            process_media_item(response_item, state, &server, false, &server_id).await?;

        if let Some(media_sources) = processed_item.media_sources.take() {
            for mut source in media_sources {
                if !seen_source_ids.insert(source.id.clone()) {
                    continue;
                }
                source.name = Some(match source.name.as_deref() {
                    Some(existing) if !existing.trim().is_empty() => {
                        format!("{existing} [{}]", server.name)
                    }
                    _ => format!("{} source", server.name),
                });
                merged_sources.push(source);
            }
        }

        let should_replace_canonical = canonical_item
            .as_ref()
            .map(|(priority, name, _)| {
                (server.priority > *priority)
                    || (server.priority == *priority && server.name.to_ascii_lowercase() < name.to_ascii_lowercase())
            })
            .unwrap_or(true);

        if should_replace_canonical {
            canonical_item = Some((server.priority, server.name.clone(), processed_item));
        }
    }

    let Some((_, _, mut canonical_item)) = canonical_item else {
        return Ok(None);
    };

    if !merged_sources.is_empty() {
        canonical_item.media_sources = Some(merged_sources);
    }
    canonical_item.id = requested_item_id.to_string();

    Ok(Some(canonical_item))
}
