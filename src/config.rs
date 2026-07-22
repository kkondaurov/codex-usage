use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use std::{
    ffi::OsString,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

pub const DEFAULT_PRICING_URL: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";
pub const MIN_PRICING_REFRESH_HOURS: u64 = 1;
pub const MAX_PRICING_REFRESH_HOURS: u64 = 24 * 366;

fn parse_pricing_refresh_hours(value: &str) -> std::result::Result<u64, String> {
    let hours = value
        .parse::<u64>()
        .map_err(|_| format!("`{value}` is not a valid number of hours"))?;
    if !(MIN_PRICING_REFRESH_HOURS..=MAX_PRICING_REFRESH_HOURS).contains(&hours) {
        return Err(format!(
            "pricing refresh hours must be between {MIN_PRICING_REFRESH_HOURS} and {MAX_PRICING_REFRESH_HOURS}"
        ));
    }
    Ok(hours)
}

#[derive(Clone, Debug)]
pub struct PricingConfig {
    pub url: String,
    pub refresh_interval_hours: u64,
    pub timeout_seconds: u64,
}

#[derive(Debug, Parser)]
#[command(name = "codex-usage", about = "Local Codex usage explorer")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

pub fn parse_cli() -> Cli {
    parse_cli_from(std::env::args_os())
}

fn parse_cli_from<I, T>(args: I) -> Cli
where
    I: IntoIterator<Item = T>,
    T: Into<OsString>,
{
    let mut args = args.into_iter().map(Into::into).collect::<Vec<_>>();
    if args.len() == 1 {
        args.push(OsString::from("serve"));
    }
    Cli::parse_from(args)
}

pub fn require_repository_root() -> Result<()> {
    let current = std::env::current_dir().context("failed to determine current directory")?;
    validate_repository_root(&current)
}

fn validate_repository_root(root: &Path) -> Result<()> {
    let manifest = root.join("Cargo.toml");
    let frontend_manifest = root.join("frontend/package.json");
    let is_backend_root = std::fs::read_to_string(manifest)
        .is_ok_and(|contents| contents.contains("name = \"codex-usage\""));
    let is_frontend_root = std::fs::read_to_string(frontend_manifest)
        .is_ok_and(|contents| contents.contains("\"name\": \"codex-usage-web\""));
    if !is_backend_root || !is_frontend_root {
        bail!(
            "codex-usage must be run from its repository root; current directory is {}",
            root.display()
        );
    }
    Ok(())
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(ServeArgs),
    Ingest(IngestArgs),
}

#[derive(Clone, Debug, clap::Args)]
pub struct CommonArgs {
    #[arg(long, env = "CODEX_USAGE_DB_PATH", default_value = "codex-usage.db")]
    pub db: PathBuf,
    #[arg(long, env = "CODEX_USAGE_PRICING_CONFIG_PATH")]
    pub pricing_config: Option<PathBuf>,
    #[arg(long, env = "CODEX_USAGE_SESSIONS_DIR")]
    pub sessions: Option<PathBuf>,
    #[arg(long, env = "CODEX_USAGE_ARCHIVE_DIR")]
    pub archive: Option<PathBuf>,
    #[arg(
        long,
        env = "CODEX_USAGE_PRICING_URL",
        default_value = DEFAULT_PRICING_URL
    )]
    pub pricing_url: String,
    #[arg(
        long,
        env = "CODEX_USAGE_PRICING_REFRESH_HOURS",
        default_value_t = 24,
        value_parser = parse_pricing_refresh_hours
    )]
    pub pricing_refresh_hours: u64,
    #[arg(long, env = "CODEX_USAGE_PRICING_TIMEOUT_SECONDS", default_value_t = 5)]
    pub pricing_timeout_seconds: u64,
}

impl CommonArgs {
    pub fn resolved(mut self) -> Self {
        if self.sessions.is_none() && self.archive.is_none() {
            let home = std::env::var_os("HOME").map(PathBuf::from);
            self.sessions = home.as_ref().map(|path| path.join(".codex/sessions"));
            self.archive = home
                .as_ref()
                .map(|path| path.join(".codex/archived_sessions"));
        }
        self
    }

    pub fn pricing(&self) -> PricingConfig {
        PricingConfig {
            url: self.pricing_url.clone(),
            refresh_interval_hours: self.pricing_refresh_hours,
            timeout_seconds: self.pricing_timeout_seconds,
        }
    }

    pub fn pricing_config_path(&self) -> PathBuf {
        self.pricing_config
            .clone()
            .unwrap_or_else(|| self.db.with_extension("pricing.json"))
    }
}

#[derive(Clone, Debug, clap::Args)]
pub struct ServeArgs {
    #[command(flatten)]
    pub common: CommonArgs,
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,
    #[arg(long, default_value_t = 5610)]
    pub port: u16,
    #[arg(long, default_value = "frontend/dist")]
    pub frontend: PathBuf,
    #[arg(long, default_value_t = 30)]
    pub poll_seconds: u64,
    #[arg(long)]
    pub no_ingest: bool,
}

impl ServeArgs {
    pub fn bind_address(&self) -> Result<SocketAddr> {
        let host = self.host.trim();
        let ip: IpAddr = host.parse().with_context(|| {
            format!("invalid bind host `{host}`; expected an IPv4 or IPv6 address")
        })?;
        if !ip.is_loopback() {
            bail!("refusing non-loopback bind address {ip}: codex-usage is localhost-only");
        }
        Ok(SocketAddr::new(ip, self.port))
    }

    pub fn require_frontend_build(&self) -> Result<()> {
        let index = self.frontend.join("index.html");
        if !index.is_file() {
            bail!(
                "frontend build not found at {}; run `npm --prefix frontend run build` from the repository root",
                index.display()
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, clap::Args)]
pub struct IngestArgs {
    #[command(flatten)]
    pub common: CommonArgs,
}

impl Default for ServeArgs {
    fn default() -> Self {
        Self {
            common: CommonArgs {
                db: PathBuf::from("codex-usage.db"),
                pricing_config: None,
                sessions: None,
                archive: None,
                pricing_url: DEFAULT_PRICING_URL.to_string(),
                pricing_refresh_hours: 24,
                pricing_timeout_seconds: 5,
            },
            host: "127.0.0.1".into(),
            port: 5610,
            frontend: PathBuf::from("frontend/dist"),
            poll_seconds: 30,
            no_ingest: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_bind_contract_accepts_ipv4_and_ipv6() {
        for host in ["127.0.0.1", "127.42.0.7", "::1"] {
            let args = ServeArgs {
                host: host.into(),
                port: 5610,
                ..ServeArgs::default()
            };
            let address = args.bind_address().unwrap();
            assert!(address.ip().is_loopback(), "{host}");
            assert_eq!(address.port(), 5610);
        }
    }

    #[test]
    fn loopback_bind_contract_rejects_wildcard_lan_and_public_addresses() {
        for host in ["0.0.0.0", "192.168.1.20", "8.8.8.8", "::", "2001:db8::1"] {
            let args = ServeArgs {
                host: host.into(),
                ..ServeArgs::default()
            };
            let error = args.bind_address().unwrap_err().to_string();
            assert!(
                error.contains("refusing non-loopback bind address"),
                "{error}"
            );
            assert!(error.contains("localhost-only"), "{error}");
        }
    }

    #[test]
    fn one_explicit_ingest_root_does_not_enable_the_other_default_root() {
        let sessions = PathBuf::from("/explicit/sessions");
        let sessions_only = CommonArgs {
            sessions: Some(sessions.clone()),
            ..ServeArgs::default().common
        }
        .resolved();
        assert_eq!(sessions_only.sessions, Some(sessions));
        assert_eq!(sessions_only.archive, None);

        let archive = PathBuf::from("/explicit/archive");
        let archive_only = CommonArgs {
            archive: Some(archive.clone()),
            ..ServeArgs::default().common
        }
        .resolved();
        assert_eq!(archive_only.sessions, None);
        assert_eq!(archive_only.archive, Some(archive));
    }

    #[test]
    fn configured_invalid_host_fails_with_a_clear_error() {
        let cli = Cli::try_parse_from(["codex-usage", "serve", "--host", "localhost"]).unwrap();
        let Command::Serve(args) = cli.command.unwrap() else {
            panic!("serve command expected");
        };
        let error = args.bind_address().unwrap_err().to_string();
        assert_eq!(
            error,
            "invalid bind host `localhost`; expected an IPv4 or IPv6 address"
        );
    }

    #[test]
    fn omitted_subcommand_parses_as_the_serve_command() {
        let cli = parse_cli_from(["codex-usage"]);
        let Some(Command::Serve(args)) = cli.command else {
            panic!("serve command expected");
        };
        assert_eq!(args.common.db, PathBuf::from("codex-usage.db"));
        assert_eq!(args.frontend, PathBuf::from("frontend/dist"));
    }

    #[test]
    fn pricing_refresh_hours_are_bounded_at_the_cli_boundary() {
        for invalid in ["0", "8785", "18446744073709551615"] {
            let error =
                Cli::try_parse_from(["codex-usage", "serve", "--pricing-refresh-hours", invalid])
                    .unwrap_err()
                    .to_string();
            assert!(error.contains("pricing-refresh-hours"), "{error}");
            assert!(error.contains("between 1 and 8784"), "{error}");
        }

        let cli = Cli::try_parse_from(["codex-usage", "serve", "--pricing-refresh-hours", "8784"])
            .unwrap();
        let Some(Command::Serve(args)) = cli.command else {
            panic!("serve command expected");
        };
        assert_eq!(args.common.pricing_refresh_hours, 8784);
    }

    #[test]
    fn repository_root_contract_rejects_a_missing_source_tree() {
        validate_repository_root(Path::new(env!("CARGO_MANIFEST_DIR"))).unwrap();

        let temp = tempfile::tempdir().unwrap();
        std::fs::create_dir(temp.path().join("frontend")).unwrap();
        std::fs::write(
            temp.path().join("Cargo.toml"),
            "[package]\nname = \"another-package\"\n",
        )
        .unwrap();
        std::fs::write(
            temp.path().join("frontend/package.json"),
            r#"{"name": "another-frontend"}"#,
        )
        .unwrap();
        let error = validate_repository_root(temp.path())
            .unwrap_err()
            .to_string();
        assert!(error.contains("must be run from its repository root"));
        assert!(error.contains(&temp.path().display().to_string()));
    }

    #[test]
    fn frontend_build_contract_requires_an_index() {
        let temp = tempfile::tempdir().unwrap();
        let args = ServeArgs {
            frontend: temp.path().to_path_buf(),
            ..ServeArgs::default()
        };
        let error = args.require_frontend_build().unwrap_err().to_string();
        assert!(error.contains("frontend build not found"));

        std::fs::write(temp.path().join("index.html"), "<!doctype html>").unwrap();
        args.require_frontend_build().unwrap();
    }
}
