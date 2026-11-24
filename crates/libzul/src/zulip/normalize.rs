use crate::zulip::models::{
    ApiSubscription, ZulipDmParticipant, ZulipMessage, ZulipMessageType, ZulipStream,
};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};

/// Convert a raw Zulip subscription into cached stream metadata.
pub fn subscription(sub: ApiSubscription) -> ZulipStream {
    ZulipStream {
        id: sub.stream_id,
        name: sub.name,
        description: sub.description,
    }
}

/// Convert a raw Zulip message JSON value into a normalized message.
pub fn message(raw: serde_json::Value) -> Result<ZulipMessage> {
    let id = raw
        .get("id")
        .and_then(|v| v.as_i64())
        .context("message missing id")?;
    let message_type = match raw.get("type").and_then(|v| v.as_str()) {
        Some("private" | "direct") => ZulipMessageType::Direct,
        _ => ZulipMessageType::Stream,
    };
    let dm_participants = if message_type == ZulipMessageType::Direct {
        direct_participants(raw.get("display_recipient"))
    } else {
        Vec::new()
    };
    let dm_conversation_key = (message_type == ZulipMessageType::Direct)
        .then(|| direct_conversation_key(&dm_participants, id));
    let dm_display_name = direct_display_name(&dm_participants);
    let stream_id = raw.get("stream_id").and_then(|v| v.as_i64());
    let stream_name = if message_type == ZulipMessageType::Direct {
        "Direct messages".to_string()
    } else {
        raw.get("display_recipient")
            .and_then(|v| v.as_str())
            .or_else(|| raw.get("stream").and_then(|v| v.as_str()))
            .unwrap_or("unknown")
            .to_string()
    };
    let topic = if message_type == ZulipMessageType::Direct {
        dm_display_name
    } else {
        raw.get("subject")
            .and_then(|v| v.as_str())
            .or_else(|| raw.get("topic").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string()
    };
    let sender_id = raw.get("sender_id").and_then(|v| v.as_i64());
    let sender_name = raw
        .get("sender_full_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let sender_email = raw
        .get("sender_email")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let timestamp = raw
        .get("timestamp")
        .and_then(|v| v.as_i64())
        .and_then(|secs| Utc.timestamp_opt(secs, 0).single())
        .context("message missing valid timestamp")?;
    let rendered_html = raw
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let plain_text = html_to_text(&rendered_html);
    let permalink = raw
        .get("permalink")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);

    Ok(ZulipMessage {
        id,
        message_type,
        stream_id,
        stream_name,
        topic,
        dm_conversation_key,
        dm_participants,
        sender_id,
        sender_name,
        sender_email,
        timestamp,
        rendered_html,
        plain_text,
        permalink,
        raw_json: raw,
    })
}

fn direct_participants(value: Option<&serde_json::Value>) -> Vec<ZulipDmParticipant> {
    value
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .map(|item| ZulipDmParticipant {
                    id: item.get("id").and_then(|value| value.as_i64()),
                    name: item
                        .get("full_name")
                        .or_else(|| item.get("name"))
                        .and_then(|value| value.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    email: item
                        .get("email")
                        .and_then(|value| value.as_str())
                        .map(ToString::to_string),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn direct_display_name(participants: &[ZulipDmParticipant]) -> String {
    if participants.is_empty() {
        return "Direct message".to_string();
    }
    participants
        .iter()
        .map(|participant| participant.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn direct_conversation_key(participants: &[ZulipDmParticipant], fallback_id: i64) -> String {
    let mut parts = participants
        .iter()
        .map(|participant| {
            participant
                .id
                .map(|id| format!("id:{id}"))
                .or_else(|| {
                    participant
                        .email
                        .as_ref()
                        .map(|email| format!("email:{email}"))
                })
                .unwrap_or_else(|| format!("name:{}", participant.name))
        })
        .collect::<Vec<_>>();
    parts.sort();
    if parts.is_empty() {
        format!("message:{fallback_id}")
    } else {
        parts.join(",")
    }
}

/// Convert simple Zulip-rendered HTML into plain text for search and display.
pub fn html_to_text(html: &str) -> String {
    let mut text = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut tag = String::new();
    let mut entity = String::new();
    let mut in_entity = false;

    for ch in html.chars() {
        if in_tag {
            if ch == '>' {
                if is_spacing_tag(&tag) {
                    text.push(' ');
                }
                tag.clear();
                in_tag = false;
            } else if tag.len() < 32 {
                tag.push(ch);
            }
            continue;
        }
        if in_entity {
            if ch == ';' {
                if let Some(decoded) = decode_entity(&entity) {
                    text.push(decoded);
                }
                entity.clear();
                in_entity = false;
            } else if entity.len() < 16 {
                entity.push(ch);
            } else {
                entity.clear();
                in_entity = false;
            }
            continue;
        }
        match ch {
            '<' => {
                in_tag = true;
                tag.clear();
            }
            '&' => in_entity = true,
            _ => text.push(ch),
        }
    }

    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn is_spacing_tag(tag: &str) -> bool {
    let name = tag
        .trim()
        .trim_start_matches('/')
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    matches!(
        name.as_str(),
        "p" | "br" | "div" | "li" | "ul" | "ol" | "blockquote" | "pre" | "tr" | "td" | "th"
    )
}

fn decode_entity(entity: &str) -> Option<char> {
    match entity {
        "amp" => Some('&'),
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "#39" | "apos" => Some('\''),
        "nbsp" => Some(' '),
        value if value.starts_with("#x") => u32::from_str_radix(&value[2..], 16)
            .ok()
            .and_then(char::from_u32),
        value if value.starts_with('#') => value[1..].parse::<u32>().ok().and_then(char::from_u32),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strips_basic_html() {
        assert_eq!(
            html_to_text("<p>Hello <strong>world</strong>&amp; friends</p>"),
            "Hello world& friends"
        );
        assert_eq!(
            html_to_text("<p>one</p><p>two &#39;three&#39;</p>"),
            "one two 'three'"
        );
    }

    #[test]
    fn normalizes_zulip_message() {
        let msg = message(json!({
            "id": 42,
            "type": "stream",
            "stream_id": 7,
            "display_recipient": "core team",
            "subject": "Release",
            "sender_id": 5,
            "sender_full_name": "Ada",
            "sender_email": "ada@example.com",
            "timestamp": 1_700_000_000,
            "content": "<p>Looking now.</p>",
            "permalink": "https://zulip.example/#narrow/..."
        }))
        .unwrap();
        assert_eq!(msg.id, 42);
        assert_eq!(msg.stream_name, "core team");
        assert_eq!(msg.topic, "Release");
        assert_eq!(msg.plain_text, "Looking now.");
    }

    #[test]
    fn normalizes_direct_message() {
        let msg = message(json!({
            "id": 43,
            "type": "private",
            "display_recipient": [
                {"id": 7, "full_name": "Ada Lovelace", "email": "ada@example.com"},
                {"id": 9, "full_name": "Grace Hopper", "email": "grace@example.com"}
            ],
            "sender_id": 7,
            "sender_full_name": "Ada Lovelace",
            "sender_email": "ada@example.com",
            "timestamp": 1_700_000_000,
            "content": "<p>DM hello.</p>"
        }))
        .unwrap();

        assert_eq!(msg.message_type, ZulipMessageType::Direct);
        assert_eq!(msg.stream_name, "Direct messages");
        assert_eq!(msg.topic, "Ada Lovelace, Grace Hopper");
        assert_eq!(msg.dm_conversation_key.as_deref(), Some("id:7,id:9"));
        assert_eq!(msg.dm_participants.len(), 2);
    }
}
