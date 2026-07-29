# Codex Usage

Codex Usage is a local dashboard for understanding how you use Codex. It turns
the session history already stored on your Mac into searchable sessions,
activity timelines, and summaries of token usage and estimated cost.

## What you can explore

- See today, this week, and this month at a glance.
- Browse, search, filter, and sort Codex sessions.
- Open a session to review its messages, agents, tools, duration, tokens, and
  estimated cost.
- Compare usage over days, weeks, months, years, or your complete history.
- Add model prices and aliases when a model is missing from the bundled pricing
  data.

Everything runs on your computer and is served only on localhost. Codex Usage
does not upload your session history. Costs are estimates based on the currently
configured model prices; they are not an OpenAI bill.

The dashboard keeps the information needed for browsing and totals, but omits
bulky tool inputs and outputs, attachments, and generated images. Every session
page includes a link that opens the original thread in Codex when you want the
complete record.

## Install on macOS

You need Git, Rust, Node.js, and npm. The repository pins the supported Rust,
Node.js, and npm versions.

From a fresh clone:

```sh
git clone https://github.com/kkondaurov/codex-usage.git
cd codex-usage
./scripts/codex-usage-service install
```

The command builds Codex Usage, installs it as a user LaunchAgent, and starts
it. It will start automatically when you log in. Then open
<http://127.0.0.1:5610>.

The first launch may take a little longer while your existing Codex history is
indexed.

## Manage the service

Run these commands from the cloned repository:

```sh
./scripts/codex-usage-service start
./scripts/codex-usage-service stop
./scripts/codex-usage-service status
./scripts/codex-usage-service uninstall
```

`stop` leaves the service installed so you can start it again later. `uninstall`
stops it and removes the LaunchAgent, but preserves the local database, pricing
settings, logs, and build files.

Rerun `install` after updating or moving the repository. The LaunchAgent stores
the checkout's absolute path.

Logs are written to:

- `~/Library/Logs/codex-usage.log`
- `~/Library/Logs/codex-usage.error.log`

## Run it in a terminal

To run Codex Usage without installing the background service:

```sh
npm --prefix frontend ci
npm --prefix frontend run build
cargo run
```

Run it from the repository root and leave the terminal open. Press `Ctrl-C` to
stop it. The UI is available at <http://127.0.0.1:5610>.

By default, Codex Usage reads `~/.codex/sessions` and
`~/.codex/archived_sessions`. It stores its local database and pricing settings
in the repository directory; those files are ignored by Git. New session
activity is picked up automatically while the application is running. Both
plain `.jsonl` rollouts and Codex's lossless `.jsonl.zst` archives are supported;
when both representations exist briefly, the plain file takes precedence.

For a one-time refresh without starting the web interface:

```sh
cargo run -- ingest
```

To see optional paths, ports, and other configuration:

```sh
cargo run -- serve --help
cargo run -- ingest --help
```
