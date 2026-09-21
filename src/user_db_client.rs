use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use futures_util::{SinkExt, StreamExt};
use prost::Message;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message as WsMessage};

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/cockatiel_userdb.v1.rs"));
}

use proto::{
    user_db_request, AddChannelRequest, AddUserRequest, ChannelRef, DeleteUserRequest,
    GetUserRequest, ListUsersRequest, RemoveChannelRequest, ScoreRequest, SetRolesRequest,
    UpdateFlagsRequest, UserDbRequest, UserDbResponse, UserValueDeleteRequest,
    UserValueListRequest, UserValueRequest,
};

pub struct UserDbClient {
    pub url: String,
    pub token: String,
    // None = not connected yet (lazy connect per request).
    conn: Mutex<Option<tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >>>,
}

impl UserDbClient {
    pub fn new(host: &str, port: u16, token: &str) -> Self {
        Self {
            url: format!("ws://{}:{}", host, port),
            token: token.to_string(),
            conn: Mutex::new(None),
        }
    }

    async fn request(&self, op: user_db_request::Op) -> Result<UserDbResponse, String> {
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            let (ws, _) = connect_async(&self.url)
                .await
                .map_err(|e| format!("UserDB connect failed: {}", e))?;
            *guard = Some(ws);
        }

        let req = UserDbRequest {
            auth_token: self.token.clone(),
            op: Some(op),
        };
        let mut buf = Vec::new();
        req.encode(&mut buf).map_err(|e| e.to_string())?;

        // Perform send + receive inside a scope so the split borrows drop
        // before we can reset the stored connection on failure.
        let (result, transport_failed) = {
            let ws = guard.as_mut().unwrap();
            let (mut write, mut read) = ws.split();

            let mut transport_failed = false;
            let result = match write.send(WsMessage::Binary(buf.into())).await {
                Ok(()) => {
                    let resp = tokio::time::timeout(Duration::from_secs(5), read.next()).await;
                    match resp {
                        Ok(Some(Ok(WsMessage::Binary(data)))) => {
                            UserDbResponse::decode(data.as_ref()).map_err(|e| e.to_string())
                        }
                        Ok(Some(Ok(_))) => Err("UserDB returned non-binary message".into()),
                        Ok(Some(Err(e))) => Err(format!("UserDB recv error: {}", e)),
                        Ok(None) => Err("UserDB closed connection".into()),
                        Err(_) => Err("UserDB request timed out".into()),
                    }
                }
                Err(e) => Err(format!("UserDB send failed: {}", e)),
            };
            if result.is_err() {
                transport_failed = true;
            }
            (result, transport_failed)
        };

        // Drop a stale/failed connection so the next request reconnects.
        if transport_failed {
            *guard = None;
        }
        result
    }

    pub async fn add_user(&self, username: &str, channel: Option<&ChannelRef>) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::AddUser(AddUserRequest {
            username: username.to_string(),
            channel: channel.cloned(),
        })).await
    }

    pub async fn delete_user(&self, uuid7: &str, actor_uuid7: &str, actor_role: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::DeleteUser(DeleteUserRequest {
            uuid7: uuid7.to_string(),
            actor_uuid7: actor_uuid7.to_string(),
            actor_role: actor_role.to_string(),
        })).await
    }

    pub async fn add_score(&self, uuid7: &str, delta: i64, reason: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::AddScore(ScoreRequest {
            uuid7: uuid7.to_string(),
            delta,
            reason: reason.to_string(),
        })).await
    }

    pub async fn remove_score(&self, uuid7: &str, delta: i64, reason: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::RemoveScore(ScoreRequest {
            uuid7: uuid7.to_string(),
            delta,
            reason: reason.to_string(),
        })).await
    }

    pub async fn add_channel(&self, uuid7: &str, channel: Option<&ChannelRef>) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::AddChannel(AddChannelRequest {
            uuid7: uuid7.to_string(),
            channel: channel.cloned(),
        })).await
    }

    pub async fn remove_channel(&self, uuid7: &str, platform: &str, channel_id: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::RemoveChannel(RemoveChannelRequest {
            uuid7: uuid7.to_string(),
            platform: platform.to_string(),
            channel_id: channel_id.to_string(),
        })).await
    }

    pub async fn get_user(&self, uuid7: &str, platform: &str, channel_id: &str, handle: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::GetUser(GetUserRequest {
            uuid7: uuid7.to_string(),
            platform: platform.to_string(),
            channel_id: channel_id.to_string(),
            handle: handle.to_string(),
        })).await
    }

    pub async fn list_users(&self, platform: &str, limit: i32, offset: i32) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::ListUsers(ListUsersRequest {
            platform: platform.to_string(),
            limit,
            offset,
        })).await
    }

    pub async fn update_flags(&self, uuid7: &str, flags: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::UpdateFlags(UpdateFlagsRequest {
            uuid7: uuid7.to_string(),
            flags: flags.to_string(),
        })).await
    }

    pub async fn set_roles(
        &self,
        uuid7: &str,
        actor_uuid7: &str,
        actor_role: &str,
        is_sponsor: bool,
        is_moderator: bool,
        is_admin: bool,
        is_owner: bool,
    ) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::SetRoles(SetRolesRequest {
            uuid7: uuid7.to_string(),
            actor_uuid7: actor_uuid7.to_string(),
            actor_role: actor_role.to_string(),
            is_sponsor,
            is_moderator,
            is_admin,
            is_owner,
        })).await
    }

    pub async fn read_user_value(&self, uuid7: &str, key: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::ReadUserValue(UserValueRequest {
            uuid7: uuid7.to_string(),
            key: key.to_string(),
            value: String::new(),
        })).await
    }

    pub async fn write_user_value(&self, uuid7: &str, key: &str, value: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::WriteUserValue(UserValueRequest {
            uuid7: uuid7.to_string(),
            key: key.to_string(),
            value: value.to_string(),
        })).await
    }

    pub async fn delete_user_value(&self, uuid7: &str, key: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::DeleteUserValue(UserValueDeleteRequest {
            uuid7: uuid7.to_string(),
            key: key.to_string(),
        })).await
    }

    pub async fn list_user_values(&self, uuid7: &str) -> Result<UserDbResponse, String> {
        self.request(user_db_request::Op::ListUserValues(UserValueListRequest {
            uuid7: uuid7.to_string(),
        })).await
    }
}

/// Serialize a UserDbResponse to JSON for returning to the TUI.
pub fn userdb_response_to_json(resp: &UserDbResponse) -> String {
    let user = resp.user.as_ref().map(|u| {
        serde_json::json!({
            "uuid7": u.uuid7,
            "username": u.username,
            "is_sponsor": u.is_sponsor,
            "is_moderator": u.is_moderator,
            "is_admin": u.is_admin,
            "is_owner": u.is_owner,
            "score": u.score,
            "commendations": u.commendations,
            "reprimands": u.reprimands,
            "channels": u.channels.iter().map(|c| {
                serde_json::json!({
                    "platform": c.platform,
                    "channel_id": c.channel_id,
                    "handle": c.handle,
                })
            }).collect::<Vec<_>>(),
            "flags": u.flags,
            "created_at": u.created_at,
            "updated_at": u.updated_at,
        })
    });
    serde_json::json!({
        "success": resp.success,
        "error": resp.error,
        "user": user,
        "users": resp.users.iter().map(|u| {
            serde_json::json!({
                "uuid7": u.uuid7,
                "username": u.username,
                "is_sponsor": u.is_sponsor,
                "is_moderator": u.is_moderator,
                "is_admin": u.is_admin,
                "is_owner": u.is_owner,
                "score": u.score,
                "commendations": u.commendations,
                "reprimands": u.reprimands,
                "channels": u.channels.iter().map(|c| {
                    serde_json::json!({
                        "platform": c.platform,
                        "channel_id": c.channel_id,
                        "handle": c.handle,
                    })
                }).collect::<Vec<_>>(),
                "flags": u.flags,
                "created_at": u.created_at,
                "updated_at": u.updated_at,
            })
        }).collect::<Vec<_>>(),
        "message": resp.message,
        "value": resp.value.as_ref().map(|v| serde_json::json!({
            "key": v.key,
            "value": v.value,
        })),
        "values": resp.values.iter().map(|v| serde_json::json!({
            "key": v.key,
            "value": v.value,
        })).collect::<Vec<_>>(),
    }).to_string()
}

pub type SharedUserDbClient = Arc<UserDbClient>;