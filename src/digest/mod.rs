use crate::zulip::models::ZulipMessage;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct Digest {
    pub hours: i64,
    pub total_messages: usize,
    pub topics: Vec<DigestTopic>,
    pub likely_attention_items: Vec<DigestItem>,
    pub unanswered_questions: Vec<DigestItem>,
    pub mentions: Vec<DigestItem>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DigestTopic {
    pub stream: String,
    pub topic: String,
    pub message_count: usize,
    pub last_message_at: Option<DateTime<Utc>>,
    pub sample_message_ids: Vec<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DigestItem {
    pub id: i64,
    pub stream: String,
    pub topic: String,
    pub sender: String,
    pub timestamp: DateTime<Utc>,
    pub text: String,
    pub permalink: Option<String>,
    pub reasons: Vec<String>,
}

pub fn build_digest(messages: Vec<ZulipMessage>, hours: i64, aliases: &[String]) -> Digest {
    let mut grouped: BTreeMap<(String, String), Vec<&ZulipMessage>> = BTreeMap::new();
    for message in &messages {
        grouped
            .entry((message.stream_name.clone(), message.topic.clone()))
            .or_default()
            .push(message);
    }

    let mut topics: Vec<_> = grouped
        .into_iter()
        .map(|((stream, topic), mut group)| {
            group.sort_by_key(|message| message.timestamp);
            let sample_message_ids = group.iter().take(5).map(|message| message.id).collect();
            DigestTopic {
                stream,
                topic,
                message_count: group.len(),
                last_message_at: group.last().map(|message| message.timestamp),
                sample_message_ids,
            }
        })
        .collect();
    topics.sort_by(|a, b| {
        b.message_count
            .cmp(&a.message_count)
            .then_with(|| b.last_message_at.cmp(&a.last_message_at))
    });

    let likely_attention_items = messages
        .iter()
        .filter_map(attention_item)
        .take(50)
        .collect();
    let mentions = messages
        .iter()
        .filter_map(|message| mention_item(message, aliases))
        .take(50)
        .collect();
    let unanswered_questions = unanswered_questions(&messages);

    Digest {
        hours,
        total_messages: messages.len(),
        topics,
        likely_attention_items,
        unanswered_questions,
        mentions,
    }
}

fn attention_item(message: &ZulipMessage) -> Option<DigestItem> {
    let text = message.plain_text.to_lowercase();
    let mut reasons = Vec::new();
    for (keyword, reason) in [
        ("bug", "bug"),
        ("broken", "bug"),
        ("fail", "failure"),
        ("error", "failure"),
        ("help", "help"),
        ("question", "question"),
        ("?", "question"),
        ("blocked", "blocked"),
        ("urgent", "urgent"),
    ] {
        if text.contains(keyword) && !reasons.iter().any(|r| r == reason) {
            reasons.push(reason.to_string());
        }
    }
    if reasons.is_empty() {
        None
    } else {
        Some(item(message, reasons))
    }
}

fn mention_item(message: &ZulipMessage, aliases: &[String]) -> Option<DigestItem> {
    let text = message.plain_text.to_lowercase();
    let mut reasons = Vec::new();
    for alias in aliases {
        let alias = alias.to_lowercase();
        if !alias.is_empty() && text.contains(&alias) {
            reasons.push(format!("mention:{alias}"));
        }
    }
    if reasons.is_empty() {
        None
    } else {
        Some(item(message, reasons))
    }
}

fn unanswered_questions(messages: &[ZulipMessage]) -> Vec<DigestItem> {
    let mut grouped: BTreeMap<(&str, &str), Vec<&ZulipMessage>> = BTreeMap::new();
    for message in messages {
        grouped
            .entry((&message.stream_name, &message.topic))
            .or_default()
            .push(message);
    }

    let mut unanswered = Vec::new();
    for (_, mut group) in grouped {
        group.sort_by_key(|message| message.timestamp);
        for (idx, message) in group.iter().enumerate() {
            if !looks_like_question(&message.plain_text) {
                continue;
            }
            let has_later_reply = group[idx + 1..].iter().any(|later| {
                later.sender_id != message.sender_id || later.sender_name != message.sender_name
            });
            if !has_later_reply {
                unanswered.push(item(message, vec!["unanswered_question".to_string()]));
            }
        }
    }

    unanswered.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
    unanswered.truncate(50);
    unanswered
}

fn looks_like_question(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains('?')
        || lower.contains("question")
        || lower.contains("does anyone")
        || lower.contains("can someone")
        || lower.contains("any ideas")
}

fn item(message: &ZulipMessage, reasons: Vec<String>) -> DigestItem {
    DigestItem {
        id: message.id,
        stream: message.stream_name.clone(),
        topic: message.topic.clone(),
        sender: message.sender_name.clone(),
        timestamp: message.timestamp,
        text: truncate(&message.plain_text, 180),
        permalink: message.permalink.clone(),
        reasons,
    }
}

fn truncate(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut value = text.chars().take(max.saturating_sub(3)).collect::<String>();
    value.push_str("...");
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn msg(id: i64, text: &str) -> ZulipMessage {
        ZulipMessage {
            id,
            stream_id: Some(1),
            stream_name: "core".to_string(),
            topic: "ops".to_string(),
            sender_id: None,
            sender_name: "Ada".to_string(),
            sender_email: None,
            timestamp: Utc::now(),
            rendered_html: text.to_string(),
            plain_text: text.to_string(),
            permalink: Some(format!("https://example/{id}")),
            raw_json: json!({"id":id}),
        }
    }

    #[test]
    fn groups_and_flags_attention() {
        let digest = build_digest(
            vec![msg(1, "installer bug?"), msg(2, "hi Herbert")],
            24,
            &["Herbert".to_string()],
        );
        assert_eq!(digest.total_messages, 2);
        assert_eq!(digest.topics[0].message_count, 2);
        assert_eq!(digest.likely_attention_items[0].id, 1);
        assert_eq!(digest.mentions[0].id, 2);
    }
}
