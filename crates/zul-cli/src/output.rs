use anyhow::Result;
use libzul::app::{ReplyResult, SyncResult};
use libzul::digest::Digest;
use libzul::zulip::models::{ZulipMessage, ZulipStream, ZulipTopic};
use serde::Serialize;

pub fn print_json<T: Serialize + ?Sized>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

pub struct PrintMode {
    pub json: bool,
}

impl PrintMode {
    pub fn new(json: bool) -> Self {
        Self { json }
    }

    pub fn print_sync(&self, result: &SyncResult) -> Result<()> {
        if self.json {
            return print_json(result);
        }
        if result.offline {
            println!(
                "Offline cache: {} streams, {} messages in last {}h",
                result.streams, result.messages, result.hours
            );
        } else {
            println!(
                "Synced {} streams and {} recent messages ({}h)",
                result.streams, result.messages, result.hours
            );
        }
        Ok(())
    }

    pub fn print_streams(&self, streams: &[ZulipStream]) -> Result<()> {
        if self.json {
            return print_json(streams);
        }
        for stream in streams {
            match &stream.description {
                Some(description) if !description.is_empty() => {
                    println!("{} - {}", stream.name, one_line(description));
                }
                _ => println!("{}", stream.name),
            }
        }
        Ok(())
    }

    pub fn print_topics(&self, topics: &[ZulipTopic]) -> Result<()> {
        if self.json {
            return print_json(topics);
        }
        for topic in topics {
            let last = topic
                .last_message_at
                .map(|ts| ts.format("%Y-%m-%d %H:%M UTC").to_string())
                .unwrap_or_else(|| "never".to_string());
            println!("{:>4}  {}  {}", topic.message_count, last, topic.name);
        }
        Ok(())
    }

    pub fn print_messages(&self, messages: &[ZulipMessage]) -> Result<()> {
        if self.json {
            return print_json(messages);
        }
        for message in messages {
            println!(
                "[{}] #{} > {} | {}: {}",
                message.id,
                message.stream_name,
                message.topic,
                message.sender_name,
                message.plain_text
            );
            if let Some(permalink) = &message.permalink {
                println!("  {permalink}");
            }
        }
        Ok(())
    }

    pub fn print_digest(&self, digest: &Digest) -> Result<()> {
        if self.json {
            return print_json(digest);
        }
        println!(
            "Zulip digest: {} messages in the last {}h",
            digest.total_messages, digest.hours
        );
        println!();
        println!("Most active topics:");
        for topic in digest.topics.iter().take(10) {
            println!(
                "- #{} > {}: {} messages",
                topic.stream, topic.topic, topic.message_count
            );
        }
        if !digest.likely_attention_items.is_empty() {
            println!();
            println!("Likely attention items:");
            for item in &digest.likely_attention_items {
                println!(
                    "- [{}] #{} > {} | {} ({})",
                    item.id,
                    item.stream,
                    item.topic,
                    item.text,
                    item.reasons.join(", ")
                );
                if let Some(permalink) = &item.permalink {
                    println!("  {permalink}");
                }
            }
        }
        if !digest.unanswered_questions.is_empty() {
            println!();
            println!("Recent unanswered questions:");
            for item in &digest.unanswered_questions {
                println!(
                    "- [{}] #{} > {} | {}",
                    item.id, item.stream, item.topic, item.text
                );
                if let Some(permalink) = &item.permalink {
                    println!("  {permalink}");
                }
            }
        }
        if !digest.mentions.is_empty() {
            println!();
            println!("Mentions:");
            for item in &digest.mentions {
                println!(
                    "- [{}] #{} > {} | {}",
                    item.id, item.stream, item.topic, item.text
                );
            }
        }
        Ok(())
    }

    pub fn print_reply(&self, reply: &ReplyResult) -> Result<()> {
        if self.json {
            return print_json(reply);
        }
        match reply {
            ReplyResult::DryRun {
                realm,
                account,
                stream,
                topic,
                message,
                content_hash,
            } => {
                println!("Dry run only; no message sent.");
                println!("Realm: {realm}");
                println!("Account: {account}");
                println!("Destination: #{stream} > {topic}");
                println!("Hash: {content_hash}");
                println!();
                println!("{message}");
            }
            ReplyResult::Sent {
                realm,
                account,
                stream,
                topic,
                zulip_message_id,
                content_hash,
            } => {
                println!("Sent message {zulip_message_id}");
                println!("Realm: {realm}");
                println!("Account: {account}");
                println!("Destination: #{stream} > {topic}");
                println!("Hash: {content_hash}");
            }
        }
        Ok(())
    }
}

fn one_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}
