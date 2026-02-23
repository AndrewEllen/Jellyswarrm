use axum::{extract::State, Json};
use hyper::StatusCode;
use jellyfin_api::JellyfinClient;
use tracing::debug;

use crate::{models::BrandingConfig, AppState};

pub async fn handle_branding(
    State(state): State<AppState>,
) -> Result<Json<BrandingConfig>, StatusCode> {
    let servers = state
        .server_storage
        .list_servers()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut message = "Jellyswarrm proxying to the following servers: ".to_string();
    let mut custom_css = String::new();

    if !servers.is_empty() {
        let server_links: Vec<String> = servers
            .iter()
            .map(|s| {
                format!(
                    "<a href=\"{}\" target=\"_blank\" rel=\"noopener noreferrer\">{}</a>",
                    s.url, s.name
                )
            })
            .collect();
        message.push_str(&server_links.join(", "));

        // Use the first reachable server in priority order as branding source-of-truth.
        // `list_servers` is already ordered by priority DESC.
        for server in servers {
            let Ok(client) = JellyfinClient::new_with_client(
                server.url.as_ref(),
                state.server_storage.client_info.clone(),
                state.server_storage.http_client.clone(),
            ) else {
                continue;
            };

            match client.get_branding_configuration().await {
                Ok(branding) => {
                    custom_css = branding.custom_css.unwrap_or_default();
                    debug!(
                        "Using branding configuration from priority server '{}'",
                        server.name
                    );
                    break;
                }
                Err(e) => {
                    debug!(
                        "Failed to fetch branding from server '{}': {}",
                        server.name, e
                    );
                }
            }
        }
    } else {
        message.push_str("No servers configured.");
    }

    let config = BrandingConfig {
        login_disclaimer: message,
        custom_css,
        splashscreen_enabled: false,
    };
    Ok(Json(config))
}
