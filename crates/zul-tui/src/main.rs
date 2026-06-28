#[tokio::main]
async fn main() -> anyhow::Result<()> {
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
