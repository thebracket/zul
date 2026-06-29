#[tokio::main]
async fn main() -> anyhow::Result<()> {
    load_dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "zul_tui=info,libzul=info,warn".into()),
        )
        .with_target(false)
        .without_time()
        .init();

    let settings = libzul::Settings::load()?;
    let app = libzul::App::open(settings, false)?;
    zul_tui::run(app).await
}

fn load_dotenv() {
    if let Some(path) = explicit_dotenv_path(std::env::var("ZUL_DOTENV").ok()) {
        let _ = dotenvy::from_path(path);
    }
}

fn explicit_dotenv_path(value: Option<String>) -> Option<String> {
    value.filter(|path| !path.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dotenv_is_only_loaded_when_explicitly_configured() {
        assert_eq!(explicit_dotenv_path(None), None);
        assert_eq!(explicit_dotenv_path(Some("  ".to_string())), None);
        assert_eq!(
            explicit_dotenv_path(Some("/tmp/zul.env".to_string())),
            Some("/tmp/zul.env".to_string())
        );
    }
}
