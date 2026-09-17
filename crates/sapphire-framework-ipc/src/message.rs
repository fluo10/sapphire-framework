//! The JSON-RPC 2.0 envelope, restricted to what this transport uses.
//!
//! Both ends of a connection are sapphire processes, so the protocol is narrower than
//! JSON-RPC allows: request ids are always integers, and batches are not accepted.

use serde::de::Deserializer;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

/// Standard JSON-RPC error codes.
pub mod codes {
    /// The frame was not valid JSON.
    pub const PARSE_ERROR: i32 = -32700;
    /// The frame was valid JSON but not a valid request.
    pub const INVALID_REQUEST: i32 = -32600;
    /// No handler is registered for the method.
    pub const METHOD_NOT_FOUND: i32 = -32601;
    /// The parameters did not match the method.
    pub const INVALID_PARAMS: i32 = -32602;
    /// The handler failed.
    pub const INTERNAL_ERROR: i32 = -32603;
}

/// An error returned by a method handler.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcError {
    /// Machine-readable code; see [`codes`].
    pub code: i32,
    /// Human-readable, one line, no trailing period.
    pub message: String,
    /// Optional structured detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    /// The parameters did not match the method.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: codes::INVALID_PARAMS,
            message: message.into(),
            data: None,
        }
    }

    /// No handler is registered for the method.
    pub fn method_not_found(method: &str) -> Self {
        Self {
            code: codes::METHOD_NOT_FOUND,
            message: format!("no such method: {method}"),
            data: None,
        }
    }

    /// The handler failed.
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: codes::INTERNAL_ERROR,
            message: message.into(),
            data: None,
        }
    }
}

/// A method call awaiting a response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request {
    /// Client-allocated id, unique while the call is in flight.
    pub id: u64,
    /// Method name, `namespace.method`.
    pub method: String,
    /// Method parameters; `Value::Null` when there are none.
    pub params: Value,
}

/// What a response carries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResponsePayload {
    /// The call succeeded.
    Ok(Value),
    /// The call failed.
    Err(RpcError),
}

/// The answer to a [`Request`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    /// The id of the request being answered.
    pub id: u64,
    /// Success or failure.
    pub payload: ResponsePayload,
}

/// A one-way message that is never answered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Notification {
    /// Method name, `namespace.method`.
    pub method: String,
    /// Parameters; `Value::Null` when there are none.
    pub params: Value,
}

/// Anything that can travel in a frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A call.
    Request(Request),
    /// An answer.
    Response(Response),
    /// A one-way message.
    Notification(Notification),
}

/// A member that serde would otherwise confuse with an absent one.
///
/// serde maps both an absent member and a `null` member to `None`, but JSON-RPC needs the
/// difference: `"result": null` is a successful call that returned null, while a response
/// with no `result` member at all is malformed. `Present` therefore keeps three states:
/// `None` is absent, `Some(None)` is present and null, `Some(Some(_))` is present with a
/// value. Used with `#[serde(default)]`, so absence is not a deserialisation error.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
struct Present<T>(Option<Option<T>>);

impl<T> Default for Present<T> {
    /// Absent.
    fn default() -> Self {
        Present(None)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Present<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        // Only reached when the member is present; a `null` member deserialises to `None`.
        Option::<T>::deserialize(deserializer).map(|value| Present(Some(value)))
    }
}

impl<T> Present<T> {
    /// True when the member was absent from the frame.
    fn is_absent(&self) -> bool {
        self.0.is_none()
    }
}

/// The on-the-wire shape. Classification happens after deserialisation, because JSON-RPC
/// distinguishes the three kinds by which fields are present.
#[derive(Serialize, Deserialize)]
struct Raw {
    jsonrpc: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    params: Option<Value>,
    #[serde(default, skip_serializing_if = "Present::is_absent")]
    result: Present<Value>,
    #[serde(default, skip_serializing_if = "Present::is_absent")]
    error: Present<RpcError>,
}

const VERSION: &str = "2.0";

fn id_of(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .ok_or_else(|| Error::Protocol(format!("request id must be an integer, got {value}")))
}

impl Message {
    /// Serialise to a single frame's payload. The result never contains a newline, so the
    /// framing in [`Connection`](crate::Connection) can simply append one.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let raw = match self {
            Message::Request(r) => Raw {
                jsonrpc: Some(VERSION.into()),
                id: Some(Value::from(r.id)),
                method: Some(r.method.clone()),
                params: Some(r.params.clone()),
                result: Present::default(),
                error: Present::default(),
            },
            Message::Response(r) => {
                let (result, error) = match &r.payload {
                    ResponsePayload::Ok(v) => (Present(Some(Some(v.clone()))), Present::default()),
                    ResponsePayload::Err(e) => (Present::default(), Present(Some(Some(e.clone())))),
                };
                Raw {
                    jsonrpc: Some(VERSION.into()),
                    id: Some(Value::from(r.id)),
                    method: None,
                    params: None,
                    result,
                    error,
                }
            }
            Message::Notification(n) => Raw {
                jsonrpc: Some(VERSION.into()),
                id: None,
                method: Some(n.method.clone()),
                params: Some(n.params.clone()),
                result: Present::default(),
                error: Present::default(),
            },
        };
        Ok(serde_json::to_vec(&raw)?)
    }

    /// Parse one frame's payload.
    pub fn decode(bytes: &[u8]) -> Result<Message> {
        let raw: Raw = serde_json::from_slice(bytes)?;
        match raw.jsonrpc {
            Some(version) if version == VERSION => {}
            Some(version) => {
                return Err(Error::Protocol(format!(
                    "expected jsonrpc \"{VERSION}\", got \"{version}\""
                )));
            }
            None => {
                return Err(Error::Protocol(format!(
                    "expected jsonrpc \"{VERSION}\", got no version"
                )));
            }
        }
        match (raw.method, raw.id) {
            (Some(method), Some(id)) => Ok(Message::Request(Request {
                id: id_of(&id)?,
                method,
                params: raw.params.unwrap_or(Value::Null),
            })),
            (Some(method), None) => Ok(Message::Notification(Notification {
                method,
                params: raw.params.unwrap_or(Value::Null),
            })),
            (None, Some(id)) => {
                let id = id_of(&id)?;
                match raw.result.0 {
                    // Both members present: not a legal response, whichever one is `null`.
                    Some(_) if !raw.error.is_absent() => Err(Error::Protocol(
                        "a response carries both result and error".into(),
                    )),
                    Some(result) => Ok(Message::Response(Response {
                        id,
                        payload: ResponsePayload::Ok(result.unwrap_or(Value::Null)),
                    })),
                    None => match raw.error.0 {
                        Some(Some(error)) => Ok(Message::Response(Response {
                            id,
                            payload: ResponsePayload::Err(error),
                        })),
                        // A present but `null` error has nothing to report.
                        Some(None) => {
                            Err(Error::Protocol("a response carries a null error".into()))
                        }
                        None => Err(Error::Protocol(
                            "a response carries neither result nor error".into(),
                        )),
                    },
                }
            }
            (None, None) => Err(Error::Protocol("a frame with neither method nor id".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trips() {
        let msg = Message::Request(Request {
            id: 7,
            method: "workspace.read_file".into(),
            params: json!({ "path": "notes/a.md" }),
        });
        let bytes = msg.encode().unwrap();
        assert!(!bytes.contains(&b'\n'));
        assert_eq!(Message::decode(&bytes).unwrap(), msg);
    }

    #[test]
    fn ok_and_error_responses_round_trip() {
        let ok = Message::Response(Response {
            id: 1,
            payload: ResponsePayload::Ok(json!({ "content": "hi" })),
        });
        assert_eq!(Message::decode(&ok.encode().unwrap()).unwrap(), ok);

        let err = Message::Response(Response {
            id: 2,
            payload: ResponsePayload::Err(RpcError::invalid_params("path is required")),
        });
        assert_eq!(Message::decode(&err.encode().unwrap()).unwrap(), err);
    }

    #[test]
    fn notification_round_trips() {
        let msg = Message::Notification(Notification {
            method: "workspace.event".into(),
            params: json!({ "kind": "FileChanged" }),
        });
        assert_eq!(Message::decode(&msg.encode().unwrap()).unwrap(), msg);
    }

    #[test]
    fn a_response_carrying_both_result_and_error_is_rejected() {
        let raw = br#"{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":-1,"message":"x"}}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }

    #[test]
    fn a_string_request_id_is_rejected() {
        let raw = br#"{"jsonrpc":"2.0","id":"abc","method":"ping"}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }

    #[test]
    fn a_missing_jsonrpc_version_is_rejected() {
        let raw = br#"{"id":1,"method":"ping"}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }

    #[test]
    fn a_null_result_is_distinguishable_from_an_absent_one() {
        // A handler that returns `json!(null)` must survive a round trip, and must still be
        // told apart from a response that carries no result at all.
        let msg = Message::Response(Response {
            id: 3,
            payload: ResponsePayload::Ok(Value::Null),
        });
        assert_eq!(
            String::from_utf8(msg.encode().unwrap()).unwrap(),
            r#"{"jsonrpc":"2.0","id":3,"result":null}"#
        );
        assert_eq!(Message::decode(&msg.encode().unwrap()).unwrap(), msg);

        let neither = br#"{"jsonrpc":"2.0","id":3}"#;
        assert!(matches!(Message::decode(neither), Err(Error::Protocol(_))));
    }

    #[test]
    fn a_request_without_params_decodes_with_null_params() {
        let raw = br#"{"jsonrpc":"2.0","id":4,"method":"server.ping"}"#;
        assert_eq!(
            Message::decode(raw).unwrap(),
            Message::Request(Request {
                id: 4,
                method: "server.ping".into(),
                params: Value::Null
            })
        );
    }

    #[test]
    fn a_wrong_jsonrpc_version_is_rejected() {
        let raw = br#"{"jsonrpc":"1.0","id":1,"method":"ping"}"#;
        assert!(matches!(Message::decode(raw), Err(Error::Protocol(_))));
    }
}
