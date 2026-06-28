use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn offline_sync_json_is_stable() {
    let cache_dir = TempDir::new().unwrap();
    let config_dir = TempDir::new().unwrap();

    let mut cmd = Command::cargo_bin("zul-cli").unwrap();
    cmd.env("XDG_CACHE_HOME", cache_dir.path())
        .env("XDG_CONFIG_HOME", config_dir.path())
        .args(["--json", "--offline", "sync", "--hours", "1"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"streams\": 0"))
        .stdout(predicate::str::contains("\"messages\": 0"))
        .stdout(predicate::str::contains("\"hours\": 1"))
        .stdout(predicate::str::contains("\"offline\": true"));
}

#[test]
fn auth_login_reads_dotenv_values() {
    let cache_dir = TempDir::new().unwrap();
    let config_dir = TempDir::new().unwrap();
    let dotenv_dir = TempDir::new().unwrap();
    let dotenv_path = dotenv_dir.path().join(".env");
    fs::write(
        &dotenv_path,
        "ZULIP_BASE_URL=https://dotenv.example\nZULIP_EMAIL=dotenv@example.com\nZULIP_API_KEY=dotenv-secret\n",
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("zul-cli").unwrap();
    cmd.env("XDG_CACHE_HOME", cache_dir.path())
        .env("XDG_CONFIG_HOME", config_dir.path())
        .env("ZUL_DOTENV", &dotenv_path)
        .args(["--json", "auth", "login"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"base_url\": \"https://dotenv.example\"",
        ))
        .stdout(predicate::str::contains(
            "\"email\": \"dotenv@example.com\"",
        ))
        .stdout(predicate::str::contains("\"secret_saved\": true"));

    assert_eq!(
        fs::read_to_string(config_dir.path().join("zul").join("api_key")).unwrap(),
        "dotenv-secret\n"
    );
}
