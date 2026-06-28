#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "zq=info,warn".into()),
        )
        .with_target(false)
        .without_time()
        .init();

    zq::cli::run().await
}
