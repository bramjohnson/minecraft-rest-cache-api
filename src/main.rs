use axum::extract::Request;
use axum::{Json, Router, extract::State, routing::get};
use dotenv::dotenv;
use futures_util::{SinkExt, StreamExt};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

struct AppState {
    cache: Cache<String, Value>,
}

macro_rules! minecraft_endpoint {
    ($endpoint:expr) => {
        get(|state| get_endpoint(state, $endpoint))
    };
}

#[tokio::main]
async fn main() {
    // Load environment variables from .env
    let _ = dotenv();

    let shared_state = Arc::new(AppState {
        // TTL of 60 seconds
        cache: Cache::builder()
            .max_capacity(100)
            .time_to_live(Duration::from_secs(60))
            .build(),
    });

    let app = Router::new()
        .route(
            "/isAllowlistEnforced",
            minecraft_endpoint!("minecraft:serversettings/enforce_allowlist"),
        )
        .route("/allowlist", minecraft_endpoint!("minecraft:allowlist"))
        .route(
            "/difficulty",
            minecraft_endpoint!("minecraft:serversettings/difficulty"),
        )
        .route("/players", minecraft_endpoint!("minecraft:players"))
        .route(
            "/max_players",
            minecraft_endpoint!("minecraft:serversettings/max_players"),
        )
        .route("/server", minecraft_endpoint!("minecraft:server/status"))
        .route(
            "/motd",
            minecraft_endpoint!("minecraft:serversettings/motd"),
        )
        .with_state(shared_state);

    let port = std::env::var("SERVER_PORT").unwrap_or("3000".to_string());

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn get_endpoint(
    State(state): State<Arc<AppState>>,
    endpoint: &str,
) -> Result<Json<Value>, String> {
    Ok(Json(handle_rpc_call(&state, endpoint).await?))
}

#[derive(Serialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    method: String,
    id: u64,
}

#[derive(Deserialize, Debug)]
struct JsonRpcResponse {
    jsonrpc: String,
    id: u64,
    // Using Value here allows this struct to work for ANY result type
    result: Value,
    // You could also add an 'error' field here if needed
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("HTTP handshaking error: {0}")]
    Http(#[from] axum::http::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tungstenite::Error),

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("RPC message error: {0}")]
    Rpc(String),
}

async fn fetch_from_minecraft(method: &str) -> Result<Value, String> {
    let secret =
        std::env::var("MINECRAFT_SERVER_MANAGEMENT_SECRET").expect("Could not find server secret"); // Load this from env/config
    let url = std::env::var("MINECRAFT_SERVER_MANAGEMENT_URL").expect("Could not find server URL"); // Update with actual host/port

    // 1. Manually build the handshake request to include the auth protocol
    let request = Request::builder()
        .uri(url)
        .header("Sec-WebSocket-Protocol", format!("minecraft-v1,{}", secret))
        .header(
            "Origin",
            std::env::var("MINECRAFT_SERVER_MANAGEMENT_ORIGIN")
                .expect("Could not find server origin"),
        )
        .header(
            "Host",
            std::env::var("MINECRAFT_SERVER_MANAGEMENT_HOST").expect("Could not find server host"),
        )
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tungstenite::handshake::client::generate_key(),
        )
        .body(())
        .map_err(|err| err.to_string())?;

    // 2. Connect
    let (ws_stream, _) = connect_async(request)
        .await
        .map_err(|err| err.to_string())?;
    let (mut write, mut read) = ws_stream.split();

    // 3. Send JSON-RPC 2.0 request
    let payload = serde_json::to_string(&JsonRpcRequest {
        jsonrpc: "2.0".into(),
        method: method.to_string(),
        id: 1,
    })
    .unwrap_or_else(|err| panic!("Could not unmarshall payload: {err}"));
    println!("{}", payload.clone());
    write
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| err.to_string())?;
    write.flush().await.map_err(|err| err.to_string())?;

    // 4. Wait for response
    if let Some(msg_result) = read.next().await {
        let msg = msg_result.map_err(|err| err.to_string())?;
        if let Message::Text(text) = msg {
            let response: JsonRpcResponse =
                serde_json::from_str(&text).map_err(|err| err.to_string())?;
            return Ok(response.result);
        }
    }

    Err("Failed to receive valid RPC response".to_string())
}

async fn handle_rpc_call(state: &AppState, method: &str) -> Result<Value, String> {
    // Replace with your actual RPC call logic
    println!("--- Fetching fresh data from RPC ---");
    let result = state
        .cache
        .try_get_with(method.to_string(), async {
            fetch_from_minecraft(method).await
        })
        .await
        .map_err(|err| (*err).clone())?;

    Ok(result)
}
