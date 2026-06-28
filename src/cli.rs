use crate::app::App;
use crate::auth::{AuthLoginArgs, AuthStatus};
use crate::config::Settings;
use crate::digest;
use crate::output::{print_json, PrintMode};
use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "zq", version, about = "Terminal-first Zulip client")]
pub struct Cli {
    #[arg(long, global = true, help = "Emit stable JSON output")]
    json: bool,
    #[arg(long, global = true, help = "Use cache only and avoid network writes")]
    offline: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Auth {
        #[command(subcommand)]
        command: AuthCommand,
    },
    Sync(SyncArgs),
    Streams,
    Topics(TopicsArgs),
    Topic(TopicArgs),
    Search(SearchArgs),
    Digest(DigestArgs),
    Reply(ReplyArgs),
    Tui,
}

#[derive(Debug, Subcommand)]
enum AuthCommand {
    Login(AuthLoginArgs),
    Status,
}

#[derive(Debug, Args)]
struct SyncArgs {
    #[arg(long, default_value_t = 24)]
    hours: i64,
}

#[derive(Debug, Args)]
struct TopicsArgs {
    stream: String,
}

#[derive(Debug, Args)]
struct TopicArgs {
    stream: String,
    topic: String,
}

#[derive(Debug, Args)]
struct SearchArgs {
    query: String,
}

#[derive(Debug, Args)]
struct DigestArgs {
    #[arg(long, default_value_t = 24)]
    hours: i64,
}

#[derive(Debug, Args)]
struct ReplyArgs {
    #[arg(long)]
    stream: String,
    #[arg(long)]
    topic: String,
    #[arg(long)]
    message: String,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    yes: bool,
}

pub async fn run() -> anyhow::Result<()> {
    load_dotenv();
    let cli = Cli::parse();
    let mode = PrintMode::new(cli.json);
    let settings = Settings::load()?;

    match cli.command {
        Command::Auth { command } => match command {
            AuthCommand::Login(args) => {
                let saved = crate::auth::login(args)?;
                if mode.json {
                    print_json(&saved)?;
                } else {
                    println!("Saved Zulip config for {}", saved.base_url);
                    if saved.secret_saved {
                        println!("Saved API key to {}", saved.secret_file);
                    } else {
                        println!("No API key was provided.");
                        println!("Run `zq auth login --base-url ... --email ... --api-key ...` or set ZULIP_API_KEY.");
                    }
                }
            }
            AuthCommand::Status => {
                let app = App::open(settings, cli.offline)?;
                let status = app.auth_status().await;
                if mode.json {
                    print_json(&status)?;
                } else {
                    match status {
                        AuthStatus::Ready { base_url, email } => {
                            println!("Authenticated config present");
                            println!("Realm: {base_url}");
                            println!("Account: {email}");
                        }
                        AuthStatus::Missing { problems } => {
                            println!("Auth/config incomplete:");
                            for problem in problems {
                                println!("- {problem}");
                            }
                        }
                    }
                }
            }
        },
        Command::Sync(args) => {
            let app = App::open(settings, cli.offline)?;
            let result = app.sync(args.hours).await?;
            mode.print_sync(&result)?;
        }
        Command::Streams => {
            let app = App::open(settings, cli.offline)?;
            mode.print_streams(&app.streams()?)?;
        }
        Command::Topics(args) => {
            let app = App::open(settings, cli.offline)?;
            mode.print_topics(&app.topics(&args.stream)?)?;
        }
        Command::Topic(args) => {
            let app = App::open(settings, cli.offline)?;
            mode.print_messages(&app.topic(&args.stream, &args.topic)?)?;
        }
        Command::Search(args) => {
            let app = App::open(settings, cli.offline)?;
            mode.print_messages(&app.search(&args.query)?)?;
        }
        Command::Digest(args) => {
            let app = App::open(settings, cli.offline)?;
            let messages = app.recent_messages(args.hours)?;
            let digest = digest::build_digest(messages, args.hours, &app.settings().digest.aliases);
            mode.print_digest(&digest)?;
        }
        Command::Reply(args) => {
            let app = App::open(settings, cli.offline)?;
            let result = app
                .reply(
                    &args.stream,
                    &args.topic,
                    &args.message,
                    args.dry_run,
                    args.yes,
                )
                .await?;
            mode.print_reply(&result)?;
        }
        Command::Tui => {
            let app = App::open(settings, cli.offline)?;
            crate::tui::run(app).await?;
        }
    }

    Ok(())
}

fn load_dotenv() {
    match std::env::var("ZQ_DOTENV") {
        Ok(path) if path.trim().is_empty() => {}
        Ok(path) => {
            let _ = dotenvy::from_path(path);
        }
        Err(_) => {
            let _ = dotenvy::dotenv();
        }
    }
}
