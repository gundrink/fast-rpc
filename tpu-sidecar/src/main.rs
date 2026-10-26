//! tpu-sidecar: loopback HTTP API that opens QUIC connections to Solana leaders' TPU (ALPN "solana-tpu"),
//! using Agave's solana-tls-utils client cert/verifier and Anza's quinn fork (same as tpu-client-next).
//!   GET  /health
//!   POST /handshake {"addrs":["ip:port",..]}            -> connect only, no stream opened, nothing sent
//!   POST /send      {"wire_b64":"..","addrs":[..]}      -> one uni stream per leader (requires TPU_SIDECAR_ALLOW_SEND=1)
use axum::{extract::State, http::StatusCode, routing::{get, post}, Json, Router};
use base64::Engine;
use quinn::{crypto::rustls::QuicClientConfig, ClientConfig, Connection, Endpoint, IdleTimeout, TransportConfig};
use serde::{Deserialize, Serialize};
use solana_keypair::Keypair;
use solana_signer::Signer;
use solana_tls_utils::{tls_client_config_builder, QuicClientCertificate};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::{Duration, Instant}};
use tokio::sync::Mutex;

const ALPN_TPU: &[u8] = b"solana-tpu";

struct App { ep: Endpoint, conns: Mutex<HashMap<SocketAddr, Connection>>, allow_send: bool, timeout: Duration, identity: String }

#[derive(Deserialize)] struct HsReq { addrs: Vec<String> }
#[derive(Deserialize)] struct SendReq { wire_b64: String, addrs: Vec<String> }
#[derive(Serialize)] struct R { addr: String, ok: bool, ms: f64, reused: bool, #[serde(skip_serializing_if = "Option::is_none")] error: Option<String> }

fn load_keypair() -> Keypair {
    if let Ok(p) = std::env::var("TPU_IDENTITY_KEYPAIR") {
        let s = std::fs::read_to_string(&p).expect("read TPU_IDENTITY_KEYPAIR");
        let b: Vec<u8> = serde_json::from_str(&s).expect("keypair json array");
        return Keypair::try_from(b.as_slice()).expect("valid 64-byte keypair");
    }
    Keypair::new() // ephemeral, unstaked identity
}

fn client_config(kp: &Keypair) -> ClientConfig {
    let cert = QuicClientCertificate::new(Some(kp));
    let mut crypto = tls_client_config_builder().with_client_auth_cert(vec![cert.certificate.clone()], cert.key.clone_key()).expect("client cert");
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_TPU.to_vec()];
    let mut t = TransportConfig::default();
    t.max_idle_timeout(Some(IdleTimeout::try_from(Duration::from_secs(10)).unwrap()));
    t.keep_alive_interval(Some(Duration::from_secs(1)));
    let mut c = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto).unwrap()));
    c.transport_config(Arc::new(t));
    c
}

async fn connect(app: &App, addr: SocketAddr) -> Result<(Connection, bool), String> {
    if let Some(c) = app.conns.lock().await.get(&addr) { if c.close_reason().is_none() { return Ok((c.clone(), true)); } }
    let fut = app.ep.connect(addr, "connect").map_err(|e| e.to_string())?;
    let c = tokio::time::timeout(app.timeout, fut).await.map_err(|_| "timeout".to_string())?.map_err(|e| e.to_string())?;
    app.conns.lock().await.insert(addr, c.clone());
    Ok((c, false))
}

async fn handshake(State(app): State<Arc<App>>, Json(r): Json<HsReq>) -> Json<Vec<R>> {
    let jobs = r.addrs.into_iter().take(16).map(|a| { let app = app.clone(); async move {
        let t0 = Instant::now();
        let res = match a.parse::<SocketAddr>() { Ok(sa) => connect(&app, sa).await, Err(e) => Err(e.to_string()) };
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        match res { Ok((_, reused)) => R { addr: a, ok: true, ms, reused, error: None }, Err(e) => R { addr: a, ok: false, ms, reused: false, error: Some(e) } }
    }});
    Json(futures::future::join_all(jobs).await)
}

async fn send(State(app): State<Arc<App>>, Json(r): Json<SendReq>) -> Result<Json<Vec<R>>, (StatusCode, String)> {
    if !app.allow_send { return Err((StatusCode::FORBIDDEN, "send disabled: set TPU_SIDECAR_ALLOW_SEND=1".into())); }
    let wire = base64::engine::general_purpose::STANDARD.decode(&r.wire_b64).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    if wire.is_empty() || wire.len() > 1232 { return Err((StatusCode::BAD_REQUEST, "wire tx must be 1..1232 bytes".into())); }
    let wire = Arc::new(wire);
    let jobs = r.addrs.into_iter().take(16).map(|a| { let app = app.clone(); let wire = wire.clone(); async move {
        let t0 = Instant::now();
        let res: Result<bool, String> = async {
            let sa: SocketAddr = a.parse().map_err(|e: std::net::AddrParseError| e.to_string())?;
            let (c, reused) = connect(&app, sa).await?;
            let mut s = c.open_uni().await.map_err(|e| e.to_string())?;
            s.write_all(&wire).await.map_err(|e| e.to_string())?;
            s.finish().map_err(|e| e.to_string())?;
            Ok(reused)
        }.await;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        match res { Ok(reused) => R { addr: a, ok: true, ms, reused, error: None }, Err(e) => { app.conns.lock().await.remove(&a.parse().unwrap_or(SocketAddr::from(([0,0,0,0],0)))); R { addr: a, ok: false, ms, reused: false, error: Some(e) } } }
    }});
    Ok(Json(futures::future::join_all(jobs).await))
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::CryptoProvider::install_default(solana_tls_utils::crypto_provider());
    let kp = load_keypair();
    let identity = kp.pubkey().to_string();
    let mut ep = Endpoint::client("0.0.0.0:0".parse().unwrap()).expect("udp bind");
    ep.set_default_client_config(client_config(&kp));
    let app = Arc::new(App {
        ep, conns: Mutex::new(HashMap::new()), identity: identity.clone(),
        allow_send: std::env::var("TPU_SIDECAR_ALLOW_SEND").as_deref() == Ok("1"),
        timeout: Duration::from_millis(std::env::var("TPU_CONNECT_TIMEOUT_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000)),
    });
    let listen = std::env::var("TPU_SIDECAR_LISTEN").unwrap_or_else(|_| "127.0.0.1:7799".into());
    let a2 = app.clone();
    let router = Router::new()
        .route("/health", get(move || { let a = a2.clone(); async move { Json(serde_json::json!({"ok": true, "identity": a.identity, "allowSend": a.allow_send})) } }))
        .route("/handshake", post(handshake)).route("/send", post(send)).with_state(app);
    let l = tokio::net::TcpListener::bind(&listen).await.expect("bind");
    eprintln!("tpu-sidecar on {listen} identity={identity} (unstaked unless TPU_IDENTITY_KEYPAIR is a staked validator identity)");
    axum::serve(l, router).await.unwrap();
}
