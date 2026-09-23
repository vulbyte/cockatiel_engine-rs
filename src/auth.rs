use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct AuthSession {
    pub module_name: String,
    pub instance_uuid7: String,
    pub auth_token: String,
    pub position: String,
    pub priority: i32,
    pub authenticated: bool,
    pub connected_at: Option<i64>,
    pub shutdown_at: Option<i64>,
    /// Last time (ms epoch) any valid container arrived from this session.
    /// Proves liveness without a dedicated heartbeat.
    pub last_activity_ms: i64,
    /// Last time (ms epoch) an AuthVerify probe was sent.
    pub last_probe_at_ms: i64,
    /// Probe response deadline (ms epoch); 0 = no probe outstanding. Any
    /// inbound activity clears it — the module is alive.
    pub probe_deadline_ms: i64,
    /// Set when a probe window expired: the module is unresponsive.
    pub unresponsive: bool,
    /// Whether the peer connected from loopback (127.0.0.1/::1). Loopback
    /// TUI control-surface connections may exercise privileged userdb
    /// mutations with owner perms; remote connections must supply a verified
    /// actor (the remote TUI-login flow is deferred).
    pub peer_loopback: bool,
    /// Unique id of the socket currently bound to this session. A reconnect
    /// claims the session under a NEW socket; the old socket's disconnect
    /// cleanup then sees a mismatched token and must NOT evict the session.
    pub socket_token: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenClaims {
    pub sub: String,
    pub name: String,
    pub iat: i64,
    pub exp: i64,
}

const TOKEN_TTL_SECS: i64 = 30 * 24 * 60 * 60; // 30 days

#[derive(Clone)]
pub struct AuthStore {
    sessions: Arc<Mutex<HashMap<String, AuthSession>>>,
    secret: String,
}

impl AuthStore {
    pub fn new(secret: String) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            secret,
        }
    }

    pub fn insert(&self, session: AuthSession) {
        let mut sessions = self.sessions.lock().unwrap();
        sessions.insert(session.instance_uuid7.clone(), session);
    }

    pub fn get(&self, instance_uuid7: &str) -> Option<AuthSession> {
        let sessions = self.sessions.lock().unwrap();
        sessions.get(instance_uuid7).cloned()
    }

    /// A session is only authorized to send payloads once it exists AND is
    /// marked authenticated (i.e. it has been approved and issued a token).
    pub fn is_authenticated(&self, instance_uuid7: &str) -> bool {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .get(instance_uuid7)
            .map(|s| s.authenticated)
            .unwrap_or(false)
    }

    pub fn verify_token(&self, instance_uuid7: &str, token: &str, expected_name: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let claims = decode::<TokenClaims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &Validation::default(),
        );

        match claims {
            // Bind the module name: the token was issued with the name the
            // module authenticated as, and every container must carry that
            // same name — a valid token can't be replayed under a trusted
            // name to reach gated capabilities.
            Ok(data) => {
                data.claims.sub == instance_uuid7
                    && data.claims.exp > now
                    && data.claims.name == expected_name
            }
            Err(_) => false,
        }
    }

    /// Record inbound activity for a session and clear any pending probe —
    /// any valid container is proof of life.
    pub fn update_activity(&self, instance_uuid7: &str, now_ms: i64) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(instance_uuid7) {
            session.last_activity_ms = now_ms;
            session.probe_deadline_ms = 0;
        }
    }

    /// Update the recorded loopback status (e.g. after a reconnect from a
    /// different address).
    pub fn set_peer_loopback(&self, instance_uuid7: &str, peer_loopback: bool) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(instance_uuid7) {
            session.peer_loopback = peer_loopback;
        }
    }

    /// Bind a session to a NEW socket after a reconnect: mark it live again and
    /// stamp it with the new socket's token so the old socket's cleanup can't
    /// evict it. No-op if the session no longer exists (handled by the caller).
    pub fn claim_socket(&self, instance_uuid7: &str, socket_token: u64, now_ms: i64) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(instance_uuid7) {
            session.socket_token = socket_token;
            session.connected_at = Some(now_ms);
            session.shutdown_at = None;
            session.last_activity_ms = now_ms;
            session.probe_deadline_ms = 0;
            session.unresponsive = false;
        }
    }

    /// Remove the session ONLY if it is still bound to the given socket. Used
    /// by a disconnecting socket so it can't evict a session a reconnect took
    /// over. Returns whether the session was removed.
    pub fn remove_if_socket(&self, instance_uuid7: &str, socket_token: u64) -> bool {
        let mut sessions = self.sessions.lock().unwrap();
        if sessions
            .get(instance_uuid7)
            .map(|s| s.socket_token == socket_token)
            .unwrap_or(false)
        {
            sessions.remove(instance_uuid7);
            true
        } else {
            false
        }
    }

    /// Mark shutdown ONLY if the session is still bound to the given socket.
    pub fn set_shutdown_if_socket(&self, instance_uuid7: &str, timestamp: i64, socket_token: u64) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(instance_uuid7) {
            if session.socket_token == socket_token {
                session.shutdown_at = Some(timestamp);
            }
        }
    }

    /// Record that an AuthVerify probe was sent, opening a response window.
    pub fn mark_probed(&self, instance_uuid7: &str, now_ms: i64, deadline_ms: i64) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(instance_uuid7) {
            session.last_probe_at_ms = now_ms;
            session.probe_deadline_ms = deadline_ms;
        }
    }

    /// Flag a session as unresponsive (its probe window expired).
    pub fn mark_unresponsive(&self, instance_uuid7: &str) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.get_mut(instance_uuid7) {
            session.unresponsive = true;
            session.probe_deadline_ms = 0;
        }
    }

    pub fn values(&self) -> Vec<AuthSession> {
        let sessions = self.sessions.lock().unwrap();
        sessions.values().cloned().collect()
    }

    /// Look up a connected session by module name (the per-session key is the
    /// instance uuid7). Returns the instance uuid + last activity timestamp.
    pub fn find_by_module(&self, module_name: &str) -> Option<(String, i64)> {
        let sessions = self.sessions.lock().unwrap();
        sessions
            .values()
            .find(|s| s.module_name == module_name)
            .map(|s| (s.instance_uuid7.clone(), s.last_activity_ms))
    }

    pub fn generate_token(&self, instance_uuid7: &str, module_name: &str) -> String {
        generate_auth_token(&self.secret, instance_uuid7, module_name)
    }
}

pub fn generate_auth_token(secret: &str, instance_uuid7: &str, module_name: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);

    let claims = TokenClaims {
        sub: instance_uuid7.to_string(),
        name: module_name.to_string(),
        iat: now,
        exp: now + TOKEN_TTL_SECS,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .unwrap_or_else(|_| String::new())
}

pub fn verify_pin(pin: i32, config_pin: u32) -> bool {
    (pin as u32) == config_pin
}