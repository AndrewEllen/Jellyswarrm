use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

use axum::{
    extract::{Request, State},
    Json,
};
use hyper::StatusCode;
use regex::Regex;
use tokio::task::JoinSet;
use tracing::{debug, error, trace};

use crate::{
    handlers::{
        common::{execute_json_request, process_media_item},
        items::get_items,
    },
    media_storage_service::MediaDedupeMember,
    models::{
        enums::{BaseItemKind, CollectionType},
        ItemsResponseVariants, ItemsResponseWithCount, MediaItem,
    },
    request_preprocessing::{apply_to_request, extract_request_infos, JellyfinAuthorization},
    server_storage::Server,
    user_authorization_service::AuthorizationSession,
    AppState,
};

static SERIES_OR_PARENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new("(?i)(seriesid|parentid)").unwrap());

pub async fn get_items_from_all_servers_if_not_restricted(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<ItemsResponseVariants>, StatusCode> {
    if let Some(query) = req.uri().query() {
        if let Some(parent_id) = extract_parent_id_from_query(query) {
            let is_grouped_parent = state
                .library_management
                .get_group_by_virtual_id_with_sources(&parent_id)
                .await
                .map_err(|e| {
                    error!("Failed to lookup grouped parent library '{}': {}", parent_id, e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?
                .is_some();

            if is_grouped_parent {
                return get_items_from_grouped_parent(State(state), req, parent_id).await;
            }
        }

        // Keep existing behavior for normal constrained folder/series browsing.
        if SERIES_OR_PARENT_RE.is_match(query) {
            return get_items(State(state), req).await;
        }
    }

    get_items_from_all_servers(State(state), req).await
}

pub async fn get_items_from_all_servers(
    State(state): State<AppState>,
    req: Request,
) -> Result<Json<ItemsResponseVariants>, StatusCode> {
    let request_path = req.uri().path().to_string();
    let (original_request, _, _, sessions, _) =
        extract_request_infos(req, &state).await.map_err(|e| {
            error!("Failed to preprocess request: {}", e);
            StatusCode::BAD_REQUEST
        })?;

    let sessions = sessions.ok_or(StatusCode::UNAUTHORIZED)?;
    if sessions.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    if is_user_views_request(&request_path) {
        return get_grouped_user_views_response(&state, &sessions).await;
    }

    let targets: Vec<FederatedTarget> = sessions
        .into_iter()
        .enumerate()
        .map(|(index, (session, server))| FederatedTarget {
            index,
            session,
            server,
            parent_override: None,
        })
        .collect();

    let server_items = fetch_items_parallel(&state, &original_request, targets).await;
    let merged = merge_server_item_batches(&state, server_items).await;
    Ok(Json(merged))
}

async fn get_items_from_grouped_parent(
    State(state): State<AppState>,
    req: Request,
    parent_virtual_id: String,
) -> Result<Json<ItemsResponseVariants>, StatusCode> {
    let group = state
        .library_management
        .get_group_by_virtual_id_with_sources(&parent_virtual_id)
        .await
        .map_err(|e| {
            error!(
                "Failed to load grouped library by virtual id '{}': {}",
                parent_virtual_id, e
            );
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let (original_request, _, _, sessions, _) =
        extract_request_infos(req, &state).await.map_err(|e| {
            error!("Failed to preprocess grouped parent request: {}", e);
            StatusCode::BAD_REQUEST
        })?;

    let sessions = sessions.ok_or(StatusCode::UNAUTHORIZED)?;
    if sessions.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let mut session_by_server: HashMap<i64, (AuthorizationSession, Server)> = HashMap::new();
    for (session, server) in sessions {
        session_by_server.entry(server.id).or_insert((session, server));
    }

    let mut targets = Vec::new();
    for (index, source) in group.sources.into_iter().enumerate() {
        if let Some((session, server)) = session_by_server.get(&source.server_id) {
            targets.push(FederatedTarget {
                index,
                session: session.clone(),
                server: server.clone(),
                parent_override: Some(source.source_library_id),
            });
        }
    }

    if targets.is_empty() {
        return Ok(Json(ItemsResponseVariants::WithCount(ItemsResponseWithCount {
            items: Vec::new(),
            total_record_count: 0,
            start_index: 0,
        })));
    }

    let server_items = fetch_items_parallel(&state, &original_request, targets).await;
    let merged = merge_server_item_batches(&state, server_items).await;
    Ok(Json(merged))
}

async fn get_grouped_user_views_response(
    state: &AppState,
    sessions: &[(AuthorizationSession, Server)],
) -> Result<Json<ItemsResponseVariants>, StatusCode> {
    let groups = state
        .library_management
        .list_groups_with_sources()
        .await
        .map_err(|e| {
            error!("Failed to list grouped libraries for user views: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let accessible_server_ids: HashSet<i64> = sessions.iter().map(|(_, server)| server.id).collect();
    let server_id = state.config.read().await.server_id.clone();

    let mut items = Vec::new();
    for group in groups {
        let accessible_source_count = group
            .sources
            .iter()
            .filter(|source| accessible_server_ids.contains(&source.server_id))
            .count();

        // Hide empty groups per user.
        if accessible_source_count == 0 {
            continue;
        }

        items.push(build_grouped_user_view_item(
            &group.group.virtual_library_id,
            &group.group.name,
            &group.group.collection_type,
            &server_id,
            accessible_source_count as i32,
        ));
    }

    Ok(Json(ItemsResponseVariants::WithCount(ItemsResponseWithCount {
        total_record_count: items.len() as i32,
        start_index: 0,
        items,
    })))
}

#[derive(Debug, Clone)]
struct FederatedTarget {
    index: usize,
    session: AuthorizationSession,
    server: Server,
    parent_override: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ServerItemsBatch {
    pub server: Server,
    pub items: ItemsResponseVariants,
}

async fn fetch_items_parallel(
    state: &AppState,
    original_request: &reqwest::Request,
    targets: Vec<FederatedTarget>,
) -> Vec<ServerItemsBatch> {
    let mut join_set = JoinSet::new();

    for target in targets {
        let request = match original_request.try_clone() {
            Some(req) => req,
            None => {
                error!(
                    "Failed to clone request for server '{}'",
                    target.server.name
                );
                continue;
            }
        };

        let state_clone = state.clone();
        join_set.spawn(async move {
            let result = fetch_items_for_target(state_clone, request, target.clone()).await;
            (target.index, result)
        });
    }

    let mut indexed_results: Vec<(usize, Option<ServerItemsBatch>)> = Vec::new();
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok((index, items)) => indexed_results.push((index, items)),
            Err(e) => error!("Federated fetch task failed: {}", e),
        }
    }

    indexed_results.sort_by_key(|(index, _)| *index);
    indexed_results
        .into_iter()
        .filter_map(|(_, items)| items)
        .collect()
}

async fn fetch_items_for_target(
    state: AppState,
    mut request: reqwest::Request,
    target: FederatedTarget,
) -> Option<ServerItemsBatch> {
    let auth = JellyfinAuthorization::Authorization(target.session.to_authorization());
    apply_to_request(
        &mut request,
        &target.server,
        &Some(target.session.clone()),
        &Some(auth),
        &state,
    )
    .await;

    if let Some(parent_override) = &target.parent_override {
        replace_parent_id_in_query(request.url_mut(), parent_override);
    }

    let response =
        match execute_json_request::<ItemsResponseVariants>(&state.reqwest_client, request).await {
            Ok(response) => response,
            Err(e) => {
                error!(
                    "Failed to get items from server '{}': {:?}",
                    target.server.name, e
                );
                return None;
            }
        };

    let server_id = { state.config.read().await.server_id.clone() };
    let mut response = response;
    for item in response.iter_mut_items() {
        match process_media_item(item.clone(), &state, &target.server, true, &server_id).await {
            Ok(processed_item) => *item = processed_item,
            Err(e) => {
                error!(
                    "Failed to process media item from server '{}': {:?}",
                    target.server.name, e
                );
                return None;
            }
        }
    }

    debug!(
        "Successfully retrieved {} items from server '{}'",
        response.len(),
        target.server.name
    );
    trace!(
        "Items from server '{}': {}",
        target.server.name,
        serde_json::to_string(&response).unwrap_or_default()
    );

    Some(ServerItemsBatch {
        server: target.server,
        items: response,
    })
}

pub(crate) async fn merge_server_item_batches(
    state: &AppState,
    server_items: Vec<ServerItemsBatch>,
) -> ItemsResponseVariants {
    let with_count = server_items
        .iter()
        .any(|items| matches!(&items.items, ItemsResponseVariants::WithCount(_)));

    let total_record_count: i32 = server_items
        .iter()
        .map(|items| match &items.items {
            ItemsResponseVariants::WithCount(response) => response.total_record_count,
            ItemsResponseVariants::Bare(items) => items.len() as i32,
        })
        .sum();

    let interleaved_items = interleave_items_with_servers(server_items);
    let deduped_items = dedupe_interleaved_items(state, interleaved_items).await;

    if with_count {
        ItemsResponseVariants::WithCount(ItemsResponseWithCount {
            items: deduped_items,
            total_record_count,
            start_index: 0,
        })
    } else {
        ItemsResponseVariants::Bare(deduped_items)
    }
}

fn merge_server_items_interleaved(server_items: Vec<ItemsResponseVariants>) -> ItemsResponseVariants {
    let mut interleaved_items = Vec::new();
    let mut live_tv_count = 0;
    let max_items = server_items.iter().map(|items| items.len()).max().unwrap_or(0);

    for i in 0..max_items {
        for server_item_list in &server_items {
            if let Some(item) = server_item_list.get(i) {
                if let Some(collectiontype) = &item.collection_type {
                    if *collectiontype == CollectionType::LiveTv
                        && item.item_type == BaseItemKind::UserView
                    {
                        live_tv_count += 1;
                        if live_tv_count > 1 {
                            continue;
                        }
                    }
                }
                interleaved_items.push(item.clone());
            }
        }
    }

    let count = interleaved_items.len();
    debug!(
        "Returning {} interleaved items from {} servers",
        count,
        server_items.len()
    );
    trace!(
        "Interleaved items payload: {}",
        serde_json::to_string(&interleaved_items).unwrap_or_default()
    );

    if server_items
        .iter()
        .any(|items| matches!(items, ItemsResponseVariants::WithCount(_)))
    {
        let total_record_count: i32 = server_items
            .iter()
            .map(|items| match items {
                ItemsResponseVariants::WithCount(response) => response.total_record_count,
                ItemsResponseVariants::Bare(items) => items.len() as i32,
            })
            .sum();

        ItemsResponseVariants::WithCount(ItemsResponseWithCount {
            items: interleaved_items,
            total_record_count,
            start_index: 0,
        })
    } else {
        ItemsResponseVariants::Bare(interleaved_items)
    }
}

#[derive(Debug, Clone)]
struct InterleavedItemWithServer {
    item: MediaItem,
    server: Server,
}

#[derive(Debug, Clone)]
struct DedupedItemEntry {
    output_index: usize,
    canonical_id: String,
    canonical_priority: i32,
    members: HashMap<i64, String>,
}

fn interleave_items_with_servers(server_items: Vec<ServerItemsBatch>) -> Vec<InterleavedItemWithServer> {
    let mut interleaved_items = Vec::new();
    let mut live_tv_count = 0;
    let max_items = server_items
        .iter()
        .map(|server_items| server_items.items.len())
        .max()
        .unwrap_or(0);

    for i in 0..max_items {
        for server_item_list in &server_items {
            if let Some(item) = server_item_list.items.get(i) {
                if let Some(collectiontype) = &item.collection_type {
                    if *collectiontype == CollectionType::LiveTv
                        && item.item_type == BaseItemKind::UserView
                    {
                        live_tv_count += 1;
                        if live_tv_count > 1 {
                            continue;
                        }
                    }
                }

                interleaved_items.push(InterleavedItemWithServer {
                    item: item.clone(),
                    server: server_item_list.server.clone(),
                });
            }
        }
    }

    interleaved_items
}

async fn dedupe_interleaved_items(
    state: &AppState,
    interleaved_items: Vec<InterleavedItemWithServer>,
) -> Vec<MediaItem> {
    let mut deduped_items: Vec<MediaItem> = Vec::new();
    let mut entries_by_key: HashMap<String, DedupedItemEntry> = HashMap::new();

    for interleaved in interleaved_items {
        let item = interleaved.item;
        let server = interleaved.server;

        let Some(dedupe_key) = build_media_dedupe_key(&item) else {
            deduped_items.push(item);
            continue;
        };

        if let Some(existing) = entries_by_key.get_mut(&dedupe_key) {
            existing
                .members
                .entry(server.id)
                .or_insert_with(|| item.id.clone());

            if server.priority > existing.canonical_priority {
                existing.canonical_priority = server.priority;
                existing.canonical_id = item.id.clone();
                deduped_items[existing.output_index] = item;
            }
            continue;
        }

        let output_index = deduped_items.len();
        let mut members = HashMap::new();
        members.insert(server.id, item.id.clone());
        entries_by_key.insert(
            dedupe_key,
            DedupedItemEntry {
                output_index,
                canonical_id: item.id.clone(),
                canonical_priority: server.priority,
                members,
            },
        );

        deduped_items.push(item);
    }

    for entry in entries_by_key.into_values() {
        if entry.members.len() <= 1 {
            continue;
        }

        let members = entry
            .members
            .into_iter()
            .map(|(server_id, virtual_media_id)| MediaDedupeMember {
                server_id,
                virtual_media_id,
            })
            .collect::<Vec<_>>();

        state
            .media_storage
            .register_media_dedupe_group(&entry.canonical_id, members)
            .await;
    }

    deduped_items
}

fn build_media_dedupe_key(item: &MediaItem) -> Option<String> {
    let kind = normalize_item_kind(&item.item_type);
    if !matches!(
        item.item_type,
        BaseItemKind::Movie
            | BaseItemKind::Video
            | BaseItemKind::Series
            | BaseItemKind::Season
            | BaseItemKind::Episode
            | BaseItemKind::BoxSet
            | BaseItemKind::CollectionFolder
            | BaseItemKind::Trailer
    ) {
        return None;
    }

    if let Some(provider_key) = provider_ids_key(item) {
        return Some(format!("provider|{kind}|{provider_key}"));
    }

    match item.item_type {
        BaseItemKind::Episode => {
            let series = normalize_opt_string(item.series_name.as_deref());
            let season_number = extract_i64_field(item, "ParentIndexNumber");
            let episode_number = extract_i64_field(item, "IndexNumber");
            if let (Some(series), Some(season), Some(episode)) = (series, season_number, episode_number)
            {
                return Some(format!("episode|{series}|{season}|{episode}"));
            }
        }
        BaseItemKind::Season => {
            let series = normalize_opt_string(item.series_name.as_deref());
            let season_number = extract_i64_field(item, "IndexNumber");
            if let (Some(series), Some(season)) = (series, season_number) {
                return Some(format!("season|{series}|{season}"));
            }
        }
        _ => {}
    }

    let name = normalize_opt_string(item.name.as_deref())
        .or_else(|| normalize_opt_string(item.original_title.as_deref()))?;
    let year = extract_i64_field(item, "ProductionYear")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "na".to_string());
    Some(format!("nameyear|{kind}|{name}|{year}"))
}

fn provider_ids_key(item: &MediaItem) -> Option<String> {
    let serde_json::Value::Object(provider_obj) = item.provider_ids.as_ref()? else {
        return None;
    };

    let mut pairs = provider_obj
        .iter()
        .filter_map(|(raw_key, raw_value)| {
            let key = normalize_opt_string(Some(raw_key.as_str()))?;
            let value = match raw_value {
                serde_json::Value::String(s) => normalize_opt_string(Some(s.as_str()))?,
                serde_json::Value::Number(n) => n.to_string(),
                _ => return None,
            };
            if value.is_empty() {
                return None;
            }
            Some(format!("{key}={value}"))
        })
        .collect::<Vec<_>>();

    if pairs.is_empty() {
        return None;
    }

    pairs.sort();
    Some(pairs.join("|"))
}

fn extract_i64_field(item: &MediaItem, field_name: &str) -> Option<i64> {
    let value = item
        .extra
        .iter()
        .find_map(|(key, value)| key.eq_ignore_ascii_case(field_name).then_some(value))?;

    match value {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn normalize_item_kind(kind: &BaseItemKind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|value| value.as_str().map(|s| s.to_ascii_lowercase()))
        .unwrap_or_else(|| "unknown".to_string())
}

fn normalize_opt_string(input: Option<&str>) -> Option<String> {
    let input = input?.trim();
    if input.is_empty() {
        return None;
    }

    Some(input.to_ascii_lowercase())
}

fn is_user_views_request(path: &str) -> bool {
    let segments: Vec<&str> = path.split('/').filter(|segment| !segment.is_empty()).collect();
    if let Some(last) = segments.last() {
        if last.eq_ignore_ascii_case("userviews") {
            return true;
        }
    }

    if segments.len() >= 3 {
        let a = segments[segments.len() - 3];
        let c = segments[segments.len() - 1];
        if a.eq_ignore_ascii_case("users") && c.eq_ignore_ascii_case("views") {
            return true;
        }
    }

    false
}

fn extract_parent_id_from_query(query: &str) -> Option<String> {
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        if k.eq_ignore_ascii_case("parentid") {
            let value = v.trim();
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn replace_parent_id_in_query(url: &mut url::Url, parent_id: &str) {
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let mut replaced = false;
    for (key, value) in &mut pairs {
        if key.eq_ignore_ascii_case("parentid") {
            *value = parent_id.to_string();
            replaced = true;
        }
    }

    if !replaced {
        pairs.push(("ParentId".to_string(), parent_id.to_string()));
    }

    {
        let mut query = url.query_pairs_mut();
        query.clear();
        for (key, value) in &pairs {
            query.append_pair(key, value);
        }
    }
}

fn build_grouped_user_view_item(
    virtual_library_id: &str,
    name: &str,
    collection_type: &str,
    server_id: &str,
    source_count: i32,
) -> MediaItem {
    let mut image_tags = HashMap::new();
    image_tags.insert("Primary".to_string(), virtual_library_id.to_string());

    MediaItem {
        name: Some(name.to_string()),
        server_id: Some(server_id.to_string()),
        id: virtual_library_id.to_string(),
        item_id: None,
        series_id: None,
        series_name: None,
        season_id: None,
        etag: None,
        date_created: None,
        can_delete: Some(false),
        can_download: Some(false),
        sort_name: Some(name.to_ascii_lowercase()),
        external_urls: Some(Vec::new()),
        path: None,
        enable_media_source_display: Some(true),
        channel_id: None,
        provider_ids: None,
        is_folder: Some(true),
        parent_id: None,
        parent_logo_item_id: None,
        parent_backdrop_item_id: None,
        parent_backdrop_image_tags: Some(Vec::new()),
        parent_logo_image_tag: None,
        parent_thumb_item_id: None,
        parent_thumb_image_tag: None,
        item_type: BaseItemKind::CollectionFolder,
        collection_type: Some(parse_collection_type(collection_type)),
        user_data: None,
        child_count: Some(source_count),
        display_preferences_id: Some(virtual_library_id.to_string()),
        tags: Some(Vec::new()),
        series_primary_image_tag: None,
        image_tags: Some(image_tags),
        backdrop_image_tags: Some(Vec::new()),
        image_blur_hashes: None,
        original_title: None,
        media_sources: None,
        media_streams: None,
        chapters: None,
        trickplay: None,
        extra: HashMap::new(),
    }
}

fn parse_collection_type(value: &str) -> CollectionType {
    serde_json::from_value::<CollectionType>(serde_json::Value::String(value.to_string()))
        .unwrap_or_else(|_| CollectionType::UnknownVariant(value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_user_views_request() {
        assert!(is_user_views_request("/UserViews"));
        assert!(is_user_views_request("/Users/abc/Views"));
        assert!(is_user_views_request("/prefix/Users/abc/Views"));
        assert!(!is_user_views_request("/Users/abc/Items"));
        assert!(!is_user_views_request("/Items"));
    }

    #[test]
    fn test_extract_parent_id_from_query() {
        assert_eq!(
            extract_parent_id_from_query("ParentId=abc123&Limit=50"),
            Some("abc123".to_string())
        );
        assert_eq!(
            extract_parent_id_from_query("limit=50&parentid=abcDEF"),
            Some("abcDEF".to_string())
        );
        assert_eq!(extract_parent_id_from_query("limit=50"), None);
    }

    #[test]
    fn test_merge_server_items_interleaved_order() {
        let a1 = build_grouped_user_view_item("a1", "A1", "movies", "srv", 1);
        let a2 = build_grouped_user_view_item("a2", "A2", "movies", "srv", 1);
        let b1 = build_grouped_user_view_item("b1", "B1", "movies", "srv", 1);

        let merged = merge_server_items_interleaved(vec![
            ItemsResponseVariants::WithCount(ItemsResponseWithCount {
                items: vec![a1.clone(), a2.clone()],
                total_record_count: 2,
                start_index: 0,
            }),
            ItemsResponseVariants::WithCount(ItemsResponseWithCount {
                items: vec![b1.clone()],
                total_record_count: 1,
                start_index: 0,
            }),
        ]);

        match merged {
            ItemsResponseVariants::WithCount(with_count) => {
                let ids = with_count
                    .items
                    .iter()
                    .map(|item| item.id.clone())
                    .collect::<Vec<_>>();
                assert_eq!(ids, vec!["a1", "b1", "a2"]);
                assert_eq!(with_count.total_record_count, 3);
            }
            ItemsResponseVariants::Bare(_) => panic!("Expected WithCount response"),
        }
    }

    #[test]
    fn test_merge_server_items_uses_federated_total_count() {
        let a1 = build_grouped_user_view_item("a1", "A1", "movies", "srv", 1);
        let b1 = build_grouped_user_view_item("b1", "B1", "movies", "srv", 1);

        let merged = merge_server_items_interleaved(vec![
            ItemsResponseVariants::WithCount(ItemsResponseWithCount {
                items: vec![a1],
                total_record_count: 1500,
                start_index: 0,
            }),
            ItemsResponseVariants::WithCount(ItemsResponseWithCount {
                items: vec![b1],
                total_record_count: 700,
                start_index: 0,
            }),
        ]);

        match merged {
            ItemsResponseVariants::WithCount(with_count) => {
                assert_eq!(with_count.items.len(), 2);
                assert_eq!(with_count.total_record_count, 2200);
            }
            ItemsResponseVariants::Bare(_) => panic!("Expected WithCount response"),
        }
    }

    #[test]
    fn test_build_media_dedupe_key_prefers_provider_ids() {
        let mut item = build_grouped_user_view_item("a1", "Test Movie", "movies", "srv", 1);
        item.item_type = BaseItemKind::Movie;
        item.provider_ids = Some(serde_json::json!({
            "Tmdb": "123",
            "Imdb": "tt123"
        }));

        let key = build_media_dedupe_key(&item).unwrap();
        assert!(key.contains("provider|movie|"));
        assert!(key.contains("imdb=tt123"));
        assert!(key.contains("tmdb=123"));
    }

    #[test]
    fn test_build_media_dedupe_key_episode_fallback() {
        let mut item = build_grouped_user_view_item("e1", "Pilot", "tvshows", "srv", 1);
        item.item_type = BaseItemKind::Episode;
        item.series_name = Some("Example Show".to_string());
        item.extra
            .insert("ParentIndexNumber".to_string(), serde_json::json!(1));
        item.extra
            .insert("IndexNumber".to_string(), serde_json::json!(2));

        let key = build_media_dedupe_key(&item).unwrap();
        assert_eq!(key, "episode|example show|1|2");
    }
}
