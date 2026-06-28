use crate::zulip::models::{ImageAttachment, ZulipMessage};
use scraper::{Html, Selector};
use std::collections::BTreeSet;
use url::Url;

const IMAGE_EXTENSIONS: &[&str] = &["avif", "gif", "jpeg", "jpg", "png", "svg", "webp"];

/// Extract renderable image references from Zulip-rendered HTML.
pub fn attachments(message: &ZulipMessage, base_url: Option<&str>) -> Vec<ImageAttachment> {
    let base = base_url.and_then(|value| Url::parse(value).ok());
    let fragment = Html::parse_fragment(&message.rendered_html);
    let img_selector = Selector::parse("img").expect("static selector");
    let link_selector = Selector::parse("a").expect("static selector");
    let mut seen = BTreeSet::new();
    let mut attachments = Vec::new();

    for element in fragment.select(&img_selector) {
        let Some(source_url) = element.value().attr("src") else {
            continue;
        };
        let Some(absolute_url) = absolute_url(source_url, base.as_ref()) else {
            continue;
        };
        if !seen.insert(absolute_url.clone()) {
            continue;
        }
        let alt_text = element
            .value()
            .attr("alt")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string);
        attachments.push(ImageAttachment {
            message_id: message.id,
            source_url: source_url.to_string(),
            filename_hint: filename_hint(&absolute_url, alt_text.as_deref()),
            absolute_url,
            alt_text,
        });
    }

    for element in fragment.select(&link_selector) {
        let Some(source_url) = element.value().attr("href") else {
            continue;
        };
        let Some(absolute_url) = absolute_url(source_url, base.as_ref()) else {
            continue;
        };
        if !has_image_extension(&absolute_url) || !seen.insert(absolute_url.clone()) {
            continue;
        }
        let text = element
            .text()
            .collect::<Vec<_>>()
            .join(" ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let alt_text = (!text.is_empty()).then_some(text);
        attachments.push(ImageAttachment {
            message_id: message.id,
            source_url: source_url.to_string(),
            filename_hint: filename_hint(&absolute_url, alt_text.as_deref()),
            absolute_url,
            alt_text,
        });
    }

    attachments
}

fn absolute_url(source: &str, base: Option<&Url>) -> Option<String> {
    if let Ok(url) = Url::parse(source) {
        return is_http_url(&url).then(|| url.to_string());
    }
    let base = base?;
    let url = base.join(source).ok()?;
    is_http_url(&url).then(|| url.to_string())
}

fn is_http_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
}

fn has_image_extension(absolute_url: &str) -> bool {
    Url::parse(absolute_url)
        .ok()
        .and_then(|url| {
            url.path_segments()
                .and_then(Iterator::last)
                .map(|segment| segment.to_ascii_lowercase())
        })
        .and_then(|filename| filename.rsplit('.').next().map(ToString::to_string))
        .map(|extension| IMAGE_EXTENSIONS.contains(&extension.as_str()))
        .unwrap_or(false)
}

fn filename_hint(absolute_url: &str, fallback: Option<&str>) -> Option<String> {
    Url::parse(absolute_url)
        .ok()
        .and_then(|url| {
            url.path_segments()
                .and_then(Iterator::last)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
        .or_else(|| fallback.map(ToString::to_string))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn extracts_img_and_resolves_relative_upload_urls() {
        let message =
            message_with_html(r#"<p>look</p><img src="/user_uploads/1/cat.png" alt="rack photo">"#);

        let attachments = attachments(&message, Some("https://zulip.example"));

        assert_eq!(attachments.len(), 1);
        assert_eq!(
            attachments[0].absolute_url,
            "https://zulip.example/user_uploads/1/cat.png"
        );
        assert_eq!(attachments[0].alt_text.as_deref(), Some("rack photo"));
        assert_eq!(attachments[0].filename_hint.as_deref(), Some("cat.png"));
    }

    #[test]
    fn extracts_linked_image_files_and_deduplicates() {
        let message = message_with_html(
            r#"
            <p><a href="https://zulip.example/user_uploads/1/plot.webp">plot</a></p>
            <img src="https://zulip.example/user_uploads/1/plot.webp" alt="duplicate">
            <a href="https://zulip.example/doc.txt">doc</a>
            "#,
        );

        let attachments = attachments(&message, Some("https://zulip.example"));

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].alt_text.as_deref(), Some("duplicate"));
    }

    fn message_with_html(html: &str) -> ZulipMessage {
        ZulipMessage {
            id: 10,
            stream_id: Some(1),
            stream_name: "core".to_string(),
            topic: "ops".to_string(),
            sender_id: Some(2),
            sender_name: "Ada".to_string(),
            sender_email: Some("ada@example.com".to_string()),
            timestamp: Utc::now(),
            rendered_html: html.to_string(),
            plain_text: "look".to_string(),
            permalink: None,
            raw_json: json!({ "id": 10 }),
        }
    }
}
