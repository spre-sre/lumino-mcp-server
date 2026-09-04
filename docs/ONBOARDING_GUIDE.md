# Team Onboarding Guide: MCP-Based Diagnostics

Step-by-step setup of the MCP diagnostics stack used by the SPRE SRE team, plus how to run a first diagnostic, interpret results, and troubleshoot connectivity. By the end you will have the core diagnostics servers working in Claude Code: Lumino, PagerDuty, Slack, DevLake, ngit-memory, plus GitHub, GitLab CEE, Konflux Portal, Sumo Logic, and Jira.

**Time to complete:** ~2 hours for setup, ~30 minutes for the worked examples.

**Sources of truth for setup:**

| Server | Canonical setup source |
|--------|------------------------|
| Lumino | [spre-sre/lumino-mcp-server](https://github.com/spre-sre/lumino-mcp-server) (this repo) |
| PagerDuty | [ggeorgie/pager-duty-mcp](https://gitlab.cee.redhat.com/ggeorgie/pager-duty-mcp) |
| Slack | [Slack MCP setup](https://gitlab.cee.redhat.com/qe-ds/rh-ai-slack-newsletter#slack-mcp-setup-redhat-community-ai-toolsslack-mcp) → [redhat-community-ai-tools/slack-mcp](https://github.com/redhat-community-ai-tools/slack-mcp) |
| ngit-memory | [ggeorgie/neural-git](https://gitlab.cee.redhat.com/ggeorgie/neural-git) |
| DevLake | Hosted Konflux DevLake MCP (HTTP); see [DevLake setup](#devlake) |
| GitHub | [github/github-mcp-server](https://github.com/github/github-mcp-server) |
| GitLab | [SPRE AI Marketplace — GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp) (`shared-mcp` plugin configs) |
| Konflux Portal | Image `quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8`; see [Konflux Portal setup](#konflux-portal) |
| Sumo Logic | [Sumo Logic MCP Server](https://www.sumologic.com/help/docs/api/mcp-server/) |
| Jira | [SPRE AI Marketplace — GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp) (`shared-mcp` / `mcp-atlassian` plugin configs) |

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Part 1: MCP Server Setup](#setup) — Lumino, PagerDuty, Slack, DevLake, ngit-memory, GitHub, GitLab, Konflux Portal, Sumo Logic, Jira
- [Part 2: Running Your First Diagnostic](#first-diagnostic)
- [Part 3: Interpreting Results](#interpreting-results)
- [Part 4: Troubleshooting Connectivity Issues](#troubleshooting)
- [Part 5: Worked Examples](#worked-examples)
  - [Example 1: Investigate a Failed Pipeline](#example-1)
  - [Example 2: Check Namespace Health](#example-2)
  - [Example 3: Incident Context from PagerDuty + Lumino](#example-3)
- [Next Steps](#next-steps)

---

<details id="prerequisites">
<summary><strong>Prerequisites</strong></summary>

Required for the servers in this guide:

- [ ] **Claude Code CLI** ([installation guide](https://docs.anthropic.com/en/docs/claude-code/overview))
- [ ] **Python 3.10+** and **[uv](https://docs.astral.sh/uv/)** (Lumino, ngit, Jira/`mcp-atlassian`; PagerDuty needs Python 3.8+)
- [ ] **Node.js 24+** — GitLab MCP (`npx @structured-world/gitlab-mcp`)
- [ ] **GitLab CEE access** — SSH (or HTTPS) to clone `gitlab.cee.redhat.com` repos (PagerDuty, ngit-memory)
- [ ] **Kubernetes/OpenShift access** — valid kubeconfig with read permissions on at least one cluster
- [ ] **oc CLI** — logged in to the target cluster (`oc login ...`)
- [ ] **PagerDuty API token** — see [PagerDuty setup](#pagerduty)
- [ ] **Slack session tokens** (`xoxc` / `xoxd`) — see [Slack setup](#slack)
- [ ] **Podman or Docker** — required for Slack, Konflux Portal, and GitHub Option B (all container paths; use whichever you already run; the JSON examples are equivalent aside from the command name)
- [ ] **Red Hat VPN** — hosted DevLake MCP, GitLab CEE, and Konflux Portal (Portal uses internal DNS from the container)
- [ ] **DevLake SSO Bearer token** — see [DevLake setup](#devlake)
- [ ] **GitHub Personal Access Token** — see [GitHub setup](#github)
- [ ] **GitLab CEE Personal Access Token** — see [GitLab setup](#gitlab)
- [ ] **Jira Cloud API token** — see [Jira setup](#jira)
- [ ] **Sumo Logic account** in the deployment your org uses — see [Sumo Logic setup](#sumo-logic)

> **Config note:** Put MCP server entries in `~/.claude.json` under `mcpServers`. If `${VAR}` expansion does not resolve in your Claude Code build, put the real value in the `env` block or export the variable in the shell before launching Claude Code.
>
> Plugin-based servers (`shared-mcp@spre-ai-marketplace`, `technical-investigation@spre-ai-marketplace`) use the env-file and launcher layout from the [SPRE AI Marketplace GitLab MCP section](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp). Match those configs rather than inventing new paths.
>
> Each Claude Code JSON example in Part 1 shows the full `mcpServers` shape so first-time setup is copy-paste friendly. If `~/.claude.json` already exists, **merge** the new server key into the existing `mcpServers` object — do not replace the whole file (Claude Code also stores other settings there).
>
> ```json
> {
>   "mcpServers": {
>     "lumino": { },
>     "pagerduty": { },
>     "slack": { },
>     "konflux-devlake-mcp-prd": { },
>     "ngit-memory": { },
>     "github": { },
>     "gitlab": { },
>     "konflux-portal": { },
>     "sumo-logic": { },
>     "mcp-atlassian": { }
>   }
> }
> ```

</details>

---

<details id="setup">
<summary><strong>Part 1: MCP Server Setup</strong></summary>

> **Tip:** After adding each server, fully restart Claude Code and run `/mcp` to confirm it is connected.

### 1.1 Lumino MCP Server

Lumino is the core cluster/pipeline diagnostics server. Tool inventory lives in the [Available Tools](../README.md#available-tools) section of this repo's README (do not hard-code a count here; it changes over time).

> A shared multi-cluster **Pharos** endpoint is coming to the inner-services stage cluster ([SPRE-6615](https://redhat.atlassian.net/browse/SPRE-6615)). Once it lands, connecting is one JSON block (URL + bearer token) and this local Lumino setup becomes the documented fallback. [SPRE-6652](https://redhat.atlassian.net/browse/SPRE-6652) will do the full rewrite.

**Option A: SPRE AI Marketplace plugin (recommended for the team)**

Plugin enablement, `~/.config/mcp-tools/` env files, and launchers follow [SPRE AI Marketplace — GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp). Enabling `shared-mcp` here also registers GitLab and Jira; you still add their env files in [§1.7](#gitlab) and [§1.10](#jira).

The `technical-investigation` plugin does **not** ship a remote Lumino binary. Its launch script runs a **local clone** via `uv run main.py`. You must configure the path first.

1. Clone and sync Lumino:

```bash
git clone https://github.com/spre-sre/lumino-mcp-server.git
cd lumino-mcp-server
uv sync
```

2. Create `~/.config/mcp-tools/lumino-mcp.env`:

```bash
mkdir -p ~/.config/mcp-tools
cat > ~/.config/mcp-tools/lumino-mcp.env <<'EOF'
export LUMINO_MCP_PATH=$HOME/path/to/lumino-mcp-server
EOF
```

Replace `$HOME/path/to/lumino-mcp-server` with the absolute path of your clone.

3. Enable the marketplace plugins in `~/.claude/settings.json`:

```json
{
  "enabledPlugins": {
    "technical-investigation@spre-ai-marketplace": true,
    "shared-mcp@spre-ai-marketplace": true
  },
  "extraKnownMarketplaces": {
    "spre-ai-marketplace": {
      "source": {
        "source": "git",
        "url": "https://gitlab.cee.redhat.com/spre-ai-marketplace/marketplace.git"
      }
    }
  }
}
```

4. Restart Claude Code. Lumino tools appear under the plugin (names typically prefixed with `mcp__plugin_technical-investigation_lumino__`). Confirm with `/mcp`.

**Option B: Manual registration in `~/.claude.json`**

```bash
git clone https://github.com/spre-sre/lumino-mcp-server.git
cd lumino-mcp-server
uv sync
```

Merge under `mcpServers` in `~/.claude.json` (or use this as a starter file if you do not have one yet):

```json
{
  "mcpServers": {
    "lumino": {
      "type": "stdio",
      "command": "/absolute/path/to/lumino-mcp-server/.venv/bin/python",
      "args": ["/absolute/path/to/lumino-mcp-server/main.py"],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

**Verify:** `List all namespaces in my cluster` — expect a call to Lumino `list_namespaces`.

---

<h3 id="pagerduty">1.2 PagerDuty MCP Server</h3>

Use the team repo from [SPRE-5903](https://redhat.atlassian.net/browse/SPRE-5903): [ggeorgie/pager-duty-mcp](https://gitlab.cee.redhat.com/ggeorgie/pager-duty-mcp). Follow that repo's [SETUP.md](https://gitlab.cee.redhat.com/ggeorgie/pager-duty-mcp/-/blob/main/SETUP.md) and [CLAUDE_CODE_INTEGRATION.md](https://gitlab.cee.redhat.com/ggeorgie/pager-duty-mcp/-/blob/main/CLAUDE_CODE_INTEGRATION.md).

**Get an API token:**

1. Log in at https://redhat.pagerduty.com/
2. Go to **Configuration** → **API Access**
3. **Create New API Key** with read access to incidents, services, users, schedules (add write only if you need create/acknowledge/resolve)
4. Copy the token

**Install:**

```bash
git clone git@gitlab.cee.redhat.com:ggeorgie/pager-duty-mcp.git
cd pager-duty-mcp
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

**Configure environment** (repo uses these names — not `PAGERDUTY_USER_API_KEY`):

```bash
cp .env.example .env
# Edit .env:
# PAGERDUTY_API_TOKEN=your-token
# PAGERDUTY_SUBDOMAIN=redhat
```

Export for your shell as well so Claude Code's env block can resolve values if you use references:

```bash
export PAGERDUTY_API_TOKEN="your-token"
export PAGERDUTY_SUBDOMAIN="redhat"
```

**Add to `~/.claude.json`** (merge under `mcpServers`):

```json
{
  "mcpServers": {
    "pagerduty": {
      "type": "stdio",
      "command": "/absolute/path/to/pager-duty-mcp/.venv/bin/python",
      "args": ["-m", "pagerduty_mcp.server"],
      "cwd": "/absolute/path/to/pager-duty-mcp",
      "env": {
        "PAGERDUTY_API_TOKEN": "your-token",
        "PAGERDUTY_SUBDOMAIN": "redhat"
      }
    }
  }
}
```

Alternatively, after `pip install -e .` with the venv activated, the entrypoint `pagerduty-mcp` is available (see CLAUDE_CODE_INTEGRATION.md). Prefer the absolute `.venv` path above so Claude Code does not depend on your interactive shell PATH.

**Tools this server exposes** (from the repo): `list_incidents`, `get_incident`, `list_services`, `get_service`, `list_users`, `get_user`, `list_schedules`, `get_schedule`, `get_oncall_users`, `list_maintenance_windows`, plus write tools (`create_incident`, `acknowledge_incident`, `resolve_incident`, `add_incident_note`) when your token allows.

**Verify:** `Who is currently on-call?` — expect `get_oncall_users`. `List triggered PagerDuty incidents` — expect `list_incidents` with `status=triggered`.

---

<h3 id="slack">1.3 Slack MCP Server</h3>

Follow the [Slack MCP setup](https://gitlab.cee.redhat.com/qe-ds/rh-ai-slack-newsletter#slack-mcp-setup-redhat-community-ai-toolsslack-mcp) section (linked from SPRE-5903). Upstream server: [redhat-community-ai-tools/slack-mcp](https://github.com/redhat-community-ai-tools/slack-mcp).

**Get tokens (`xoxc` / `xoxd`):**

Preferred: [slack-token-extractor](https://github.com/maorfr/slack-token-extractor) (Chrome extension instructions in that repo). Never commit tokens.

**Run via Podman or Docker** (image `quay.io/redhat-ai-tools/slack-mcp`). The server is a container on stdio - pick the runtime you already have. Merge under `mcpServers` in `~/.claude.json`.

**Podman:**

```json
{
  "mcpServers": {
    "slack": {
      "type": "stdio",
      "command": "podman",
      "args": [
        "run", "-i", "--rm",
        "-e", "SLACK_XOXC_TOKEN",
        "-e", "SLACK_XOXD_TOKEN",
        "-e", "MCP_TRANSPORT",
        "-e", "LOGS_CHANNEL_ID",
        "quay.io/redhat-ai-tools/slack-mcp"
      ],
      "env": {
        "SLACK_XOXC_TOKEN": "xoxc-...",
        "SLACK_XOXD_TOKEN": "xoxd-...",
        "MCP_TRANSPORT": "stdio",
        "LOGS_CHANNEL_ID": "C..."
      }
    }
  }
}
```

**Docker:** same block, but set `"command": "docker"` instead of `"podman"`. Everything else (args, env, image) is identical.

Confirm the runtime is up before restarting Claude Code:

```bash
# Podman
podman info >/dev/null && podman pull quay.io/redhat-ai-tools/slack-mcp

# Docker
docker info >/dev/null && docker pull quay.io/redhat-ai-tools/slack-mcp
```

`LOGS_CHANNEL_ID` is optional (server logging channel). Use a real channel ID from Slack → channel details if you set it; do not leave a placeholder ID and assume it works.

**Verify:** `Search Slack for recent messages about pipeline failures` — expect `search_messages`.

---

<h3 id="devlake">1.4 DevLake MCP Server</h3>

Hosted HTTP MCP for Konflux engineering metrics (PR stats, DORA-style tools, etc.).

**Endpoint (not secret):**

```text
https://konflux-mcp-server-konflux-devlake.apps.rosa.kflux-c-prd-i01.7hyu.p3.openshiftapps.com/mcp
```

**Auth:** Red Hat SSO offline token as a Bearer header. Connect to the Red Hat VPN. Refresh the token when you get 401/403.

**Get a token:**

1. Open https://console.redhat.com/openshift/token (sign in with your Red Hat account)
2. Copy the offline token value only (do **not** include a `Bearer ` prefix)
3. Paste it into the config below as `<YOUR_SSO_TOKEN>`

The hosted DevLake MCP accepts offline tokens and exchanges them for short-lived access tokens. Merge under `mcpServers` in `~/.claude.json`:

```json
{
  "mcpServers": {
    "konflux-devlake-mcp-prd": {
      "type": "http",
      "url": "https://konflux-mcp-server-konflux-devlake.apps.rosa.kflux-c-prd-i01.7hyu.p3.openshiftapps.com/mcp",
      "headers": {
        "Authorization": "Bearer <YOUR_SSO_TOKEN>"
      }
    }
  }
}
```

**Verify:** `What are the PR stats for project "Secureflow - Konflux - Build Team" for the last 7 days?` — expect `get_pr_stats` (requires a valid DevLake `project_name`).

---

### 1.5 ngit-memory MCP Server

Canonical repo: [ggeorgie/neural-git](https://gitlab.cee.redhat.com/ggeorgie/neural-git) (see SPRE-5903).

```bash
git clone git@gitlab.cee.redhat.com:ggeorgie/neural-git.git
cd neural-git
./setup-brain.sh
```

Defaults:

- Brain path: `~/neural-memory` (override with `./setup-brain.sh --brain-path /path`)
- Installs the package, runs `ngit init`, registers `ngit-memory` in `~/.claude.json`, and can add a SessionStart auto-ingest hook

Do **not** pass a bare path as a positional argument (`./setup-brain.sh ~/neural-memory` fails). Do **not** manually `git init` the brain before the script; `setup-brain.sh` initializes it.

If you need only the MCP entry (script already wrote something similar), merge under `mcpServers` in `~/.claude.json`:

```json
{
  "mcpServers": {
    "ngit-memory": {
      "type": "stdio",
      "command": "/absolute/path/to/neural-git/.venv/bin/ngit-mcp",
      "args": [],
      "env": {
        "NGIT_BRAIN_PATH": "/absolute/path/to/neural-memory"
      }
    }
  }
}
```

**Verify:** `Check memory status` — expect `memory_status`.

---

<h3 id="github">1.6 GitHub MCP Server</h3>

Official server: [github/github-mcp-server](https://github.com/github/github-mcp-server). Do **not** use the archived npm package `@modelcontextprotocol/server-github`.

**Get a token:** create a [fine-grained PAT](https://github.com/settings/personal-access-tokens/new) (or a classic PAT with `repo` and `read:org`) for the GitHub orgs you investigate (`spre-sre`, `konflux-ci`, `redhat-appstudio`, and so on). Never commit the token.

**Option A: Remote HTTP** (no container). Merge under `mcpServers` in `~/.claude.json`:

```json
{
  "mcpServers": {
    "github": {
      "type": "http",
      "url": "https://api.githubcopilot.com/mcp/",
      "headers": {
        "Authorization": "Bearer YOUR_GITHUB_PAT"
      }
    }
  }
}
```

**Option B: Local container** with Podman or Docker (image `ghcr.io/github/github-mcp-server`):

```json
{
  "mcpServers": {
    "github": {
      "type": "stdio",
      "command": "podman",
      "args": [
        "run", "-i", "--rm",
        "-e", "GITHUB_PERSONAL_ACCESS_TOKEN",
        "ghcr.io/github/github-mcp-server"
      ],
      "env": {
        "GITHUB_PERSONAL_ACCESS_TOKEN": "YOUR_GITHUB_PAT"
      }
    }
  }
}
```

Docker users: set `"command": "docker"` instead of `"podman"`. Confirm the runtime first:

```bash
podman info >/dev/null && podman pull ghcr.io/github/github-mcp-server
# or
docker info >/dev/null && docker pull ghcr.io/github/github-mcp-server
```

**Verify:** `Get the latest pull request on spre-sre/lumino-mcp-server` — expect `get_pull_request` / `list_pull_requests`.

---

<h3 id="gitlab">1.7 GitLab MCP Server</h3>

Team GitLab CEE access uses [`@structured-world/gitlab-mcp`](https://www.npmjs.com/package/@structured-world/gitlab-mcp) (Node.js 24+). Plugin config (env file path, launcher, package pin) is taken from [SPRE AI Marketplace — GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp) — that pin is authoritative for Option A. The `shared-mcp@spre-ai-marketplace` plugin launched in [Lumino Option A](#setup) already registers this server; you only need the env file.

**Get a token:** GitLab CEE → **Preferences** → **Access Tokens** (`https://gitlab.cee.redhat.com/-/user_settings/personal_access_tokens`). `read_api` is enough for investigation; use `api` only if you need write tools. VPN required.

**Env file** (both plugin and manual paths):

```bash
mkdir -p ~/.config/mcp-tools
cat > ~/.config/mcp-tools/gitlab.env <<'EOF'
export GITLAB_TOKEN=your-gitlab-cee-token
export GITLAB_API_URL=https://gitlab.cee.redhat.com/
EOF
chmod 600 ~/.config/mcp-tools/gitlab.env
```

Never commit this file (it holds a live PAT).

**Option A: SPRE AI Marketplace plugin (recommended)**

With `shared-mcp@spre-ai-marketplace` enabled (see Lumino setup and [marketplace GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp)), restart Claude Code. GitLab tools appear under the plugin after the env file exists.

**Option B: Manual registration in `~/.claude.json`**

```json
{
  "mcpServers": {
    "gitlab": {
      "type": "stdio",
      "command": "npx",
      "args": ["-y", "@structured-world/gitlab-mcp@9"],
      "env": {
        "GITLAB_TOKEN": "your-gitlab-cee-token",
        "GITLAB_API_URL": "https://gitlab.cee.redhat.com/",
        "LOG_LEVEL": "error"
      }
    }
  }
}
```

If the server starts but only exposes `manage_context`, it is in disconnected mode (VPN down, bad token, or wrong API URL). Fix the env and restart.

For Option B, `@9` tracks the 9.x line (npm latest at review time was 9.1.3). Prefer the marketplace pin when using Option A.

**Verify:** `Who am I on GitLab CEE?` — expect `manage_context` with action `whoami`. Then `Search GitLab CEE for the pager-duty-mcp project`.

---

<h3 id="konflux-portal">1.8 Konflux Portal MCP Server</h3>

Read-only catalog of Konflux environments, teams, docs, and observability links. It runs as a **container** (same as Slack), so Podman or Docker from [Prerequisites](#prerequisites) is required.

Pin the image by digest rather than floating on `latest` from a personal namespace: `quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8` (tag `d96b57e` at review time). Connect to the Red Hat VPN. The container needs the `mcp-net` bridge plus internal DNS so it can resolve Konflux portal hosts. `mcp-net` is a normal bridge and is **not** persisted across `podman machine` recreate — create it before every start if it is missing.

The `--dns` values (`10.11.5.19`, `10.2.32.1`) are Red Hat VPN resolvers. If name resolution starts failing, re-derive them from your VPN DNS (for example `scutil --dns` on macOS while connected) instead of copying stale IPs.

**Podman** (merge under `mcpServers` in `~/.claude.json`):

```json
{
  "mcpServers": {
    "konflux-portal": {
      "type": "stdio",
      "command": "podman",
      "args": [
        "run", "-i", "--rm",
        "--network=mcp-net",
        "--dns=10.11.5.19",
        "--dns=10.2.32.1",
        "quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8"
      ]
    }
  }
}
```

Create the network and pull the image once:

```bash
podman network exists mcp-net || podman network create mcp-net
podman pull quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8
```

**Docker:** same JSON with `"command": "docker"`, and:

```bash
docker network inspect mcp-net >/dev/null 2>&1 || docker network create mcp-net
docker pull quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8
```

**Verify:** `List Konflux production environments` — expect `list_environments`. `Get console and Grafana URLs for stone-prd-rh01` — expect `get_environment_urls`.

---

<h3 id="sumo-logic">1.9 Sumo Logic MCP Server</h3>

Hosted HTTP MCP for log search, alerts, and related Sumo tools. Canonical docs: [Sumo Logic MCP Server](https://www.sumologic.com/help/docs/api/mcp-server/).

OAuth tokens are bound to **one** Sumo deployment. Use the URL that matches your org (Red Hat commonly uses US East):

| Deployment | MCP Server URL |
|------------|----------------|
| US East (N. Virginia) | `https://mcp.sumologic.com/mcp` |
| US West (Oregon) | `https://mcp.us2.sumologic.com/mcp` |
| Europe (Ireland) | `https://mcp.eu.sumologic.com/mcp` |

See the Sumo docs for the full deployment table.

**Add to `~/.claude.json`** (merge under `mcpServers`):

```json
{
  "mcpServers": {
    "sumo-logic": {
      "type": "http",
      "url": "https://mcp.sumologic.com/mcp"
    }
  }
}
```

Or:

```bash
claude mcp add --scope user --transport http sumo-logic "https://mcp.sumologic.com/mcp"
```

Then in Claude Code run `/mcp`, select **sumo-logic**, and **Authenticate**. A browser window opens for Sumo / IdP login.

An org admin must enable **two independent gates** (not one) before this works:

1. **MCP Server access** — Administration → Feature Management
2. **Enable CIMD Clients** — Administration → Account Security Settings → Policies

**Verify:** `List my available Sumo Logic MCP tools`. Then search logs you already have access to in the Sumo UI.

---

<h3 id="jira">1.10 Jira MCP Server</h3>

Red Hat Jira Cloud (`https://redhat.atlassian.net`) via [`mcp-atlassian`](https://github.com/sooperset/mcp-atlassian). Plugin config is taken from [SPRE AI Marketplace — GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp) (`shared-mcp`). The plugin already registers this server; you only need the env file.

**Get a token:**

1. Open https://id.atlassian.com/manage-profile/security/api-tokens
2. **Create API token**, copy it
3. Username is your Red Hat email (Cloud), not your Jira display name

**Env file:**

```bash
mkdir -p ~/.config/mcp-tools
cat > ~/.config/mcp-tools/mcp-atlassian.env <<'EOF'
export JIRA_URL=https://redhat.atlassian.net
export JIRA_USERNAME=you@redhat.com
export JIRA_API_TOKEN=your-jira-api-token
export JIRA_SSL_VERIFY=true
EOF
chmod 600 ~/.config/mcp-tools/mcp-atlassian.env
```

Never commit this file (it holds a live API token).

**Option A: SPRE AI Marketplace plugin (recommended)**

With `shared-mcp@spre-ai-marketplace` enabled (see Lumino setup and [marketplace GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp)), restart Claude Code. Jira tools appear as `mcp-atlassian` (issue search, get issue, comments, and so on).

**Option B: Manual registration in `~/.claude.json`**

```json
{
  "mcpServers": {
    "mcp-atlassian": {
      "type": "stdio",
      "command": "uvx",
      "args": ["mcp-atlassian"],
      "env": {
        "JIRA_URL": "https://redhat.atlassian.net",
        "JIRA_USERNAME": "you@redhat.com",
        "JIRA_API_TOKEN": "your-jira-api-token",
        "JIRA_SSL_VERIFY": "true",
        "TRANSPORT": "stdio",
        "MCP_LOGGING_STDOUT": "false"
      }
    }
  }
}
```

`TRANSPORT` and `MCP_LOGGING_STDOUT=false` are already the `mcp-atlassian` defaults. Setting them explicitly is optional belt-and-braces so logs cannot leak onto stdout (which would break the JSON-RPC handshake).

**Verify:** `Get Jira issue SPRE-5970` — expect `jira_get_issue` / `jira_search`.

---

### Setup Checklist

| Server | Status | Key tools |
|--------|--------|-----------|
| lumino (or plugin:technical-investigation:lumino) | Connected | `list_namespaces`, `list_pipelineruns`, `analyze_failed_pipeline`, `conservative_namespace_overview` |
| pagerduty ([pager-duty-mcp](https://gitlab.cee.redhat.com/ggeorgie/pager-duty-mcp)) | Connected | `list_incidents`, `get_incident`, `get_oncall_users` |
| slack | Connected | `search_messages`, `get_channel_history`, `post_message` |
| konflux-devlake-mcp-prd | Connected | `get_pr_stats`, `get_deployment_frequency`, `get_lead_time_for_changes` |
| ngit-memory | Connected | `memory_recall`, `memory_learn`, `memory_think` |
| github | Connected | `get_pull_request`, `list_pull_requests`, `get_issue`, `search_code` |
| gitlab (or plugin:shared-mcp:gitlab) | Connected | `manage_context`, plus browse/search tools when connected |
| konflux-portal | Connected | `list_environments`, `get_environment_urls`, `get_team`, `search_links` |
| sumo-logic | Connected | log search / alert tools (after `/mcp` authenticate) |
| mcp-atlassian (Jira) | Connected | `jira_get_issue`, `jira_search` |

If any server is disconnected, see [Troubleshooting](#troubleshooting).

</details>

---

<details id="first-diagnostic">
<summary><strong>Part 2: Running Your First Diagnostic</strong></summary>

Replace `<tenant-namespace>` with a namespace your kubeconfig can read.

### Step 1: Cluster connectivity

```text
List all namespaces in my cluster and tell me how many there are
```

Tool: Lumino `list_namespaces`.

### Step 2: Namespace sample

```text
Give me a health overview of the "<tenant-namespace>" namespace
```

Tools: typically `conservative_namespace_overview` or `adaptive_namespace_investigation`.

### Step 3: PagerDuty

```text
List triggered PagerDuty incidents
```

Tool: `list_incidents` (`status=triggered`).

### Step 4: Memory

```text
Remember that I verified MCP diagnostics are working on my cluster
```

Tool: ngit-memory `memory_learn`.

</details>

---

<details id="interpreting-results">
<summary><strong>Part 3: Interpreting Results</strong></summary>

### Lumino

Tools return structured JSON; Claude summarizes in natural language. Useful distinctions:

**Pod `status` / phase** (Kubernetes): `Pending`, `Running`, `Succeeded`, `Failed`, `Unknown`.

**Container waiting / last termination reasons** (not phases): `CrashLoopBackOff`, `ImagePullBackOff`, `OOMKilled`, etc. Lumino surfaces these under container state fields (for example in `list_pods_in_namespace` or `check_resource_constraints`). In `conservative_namespace_overview`, they usually appear only as log-derived text in `top_issue` / `critical_issues`, not as the pod `status` phase.

**PipelineRun status:** `Succeeded`, `Failed`, `Running`, etc. Use `analyze_failed_pipeline` for failed runs. Return keys include `pipeline_name`, `pipeline_status`, `overall_message`, `failed_task_count`, `failed_tasks`, `probable_root_cause`, `recommended_actions`.

**Events:** `Warning` vs `Normal`; high `count` often means a recurring condition.

### Cross-server correlation

| If you find... | Then check... |
|----------------|---------------|
| Failed PipelineRun in Lumino | Slack for related discussion; GitHub/GitLab for the PR; DevLake for PR/CI metrics on the same repo/project |
| PagerDuty incident | Lumino for cluster/namespace state; Konflux Portal for env URLs; Slack for war-room threads; Jira for the linked ticket |
| Weak CI/merge rates in DevLake | Lumino for failing pipelines/pods (DevLake is engineering metrics, not live cluster error rate) |
| Slack thread about an outage | ngit-memory for prior investigation notes; Sumo Logic for matching log queries |
| "What cluster is tenant X on?" | Konflux Portal `get_team` / `list_environments`, then Lumino against that cluster |

</details>

---

<details id="troubleshooting">
<summary><strong>Part 4: Troubleshooting Connectivity Issues</strong></summary>

### MCP server won't connect

1. Confirm the command exists:

```bash
# Lumino
/path/to/lumino-mcp-server/.venv/bin/python --version

# PagerDuty (pager-duty-mcp)
/path/to/pager-duty-mcp/.venv/bin/python -m pagerduty_mcp.server --help
# or: /path/to/pager-duty-mcp/.venv/bin/pagerduty-mcp --help

# Slack (timeout: stdio images often hang on --help)
timeout 10 podman run --rm quay.io/redhat-ai-tools/slack-mcp --help
# or: timeout 10 docker run --rm quay.io/redhat-ai-tools/slack-mcp --help

# Konflux Portal
timeout 10 podman run --rm --network=mcp-net quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8 --help
# or: timeout 10 docker run --rm --network=mcp-net quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8 --help
```

2. Check env vars used by **this** stack:

```bash
echo "${PAGERDUTY_API_TOKEN:+set}"
echo "${PAGERDUTY_SUBDOMAIN:+set}"
echo "${SLACK_XOXC_TOKEN:+set}"
echo "${LUMINO_MCP_PATH:+set}"
echo "${GITHUB_PERSONAL_ACCESS_TOKEN:+set}"
# sourced from ~/.config/mcp-tools/:
grep -E '^(export )?[A-Z_]+=' ~/.config/mcp-tools/gitlab.env ~/.config/mcp-tools/mcp-atlassian.env 2>/dev/null | sed 's/=.*/=***/'
```

3. Validate JSON:

```bash
python3 -m json.tool ~/.claude.json > /dev/null
```

4. Fully restart Claude Code (reload alone may not pick up config changes).

### Lumino: unable to load kubeconfig

```bash
oc whoami
kubectl cluster-info
# oc login <cluster-url> --token=<token>
```

### Lumino: Forbidden

```bash
kubectl config get-contexts
kubectl config use-context <context-with-permissions>
kubectl auth can-i list pods -n <tenant-namespace>
```

### Lumino: tools return Unauthorized despite `oc whoami` working

Lumino reads the kubeconfig once at MCP server startup. If the token expires mid-session, or you switch `kubectl` context after launching Claude Code, Lumino will still use the old connection. Fix: fully restart Claude Code so the MCP server picks up the current context and token.

### PagerDuty: auth / "client not initialized"

- Confirm `PAGERDUTY_API_TOKEN` is set (name from [pager-duty-mcp](https://gitlab.cee.redhat.com/ggeorgie/pager-duty-mcp), not `PAGERDUTY_USER_API_KEY`)
- Confirm `PAGERDUTY_SUBDOMAIN=redhat` for Red Hat's instance
- Recreate the API key under **Configuration → API Access** if needed
- Ensure `cwd` / module path points at your clone with `pip install -e .` completed

### Slack: timeout or invalid_auth

```bash
# Podman
podman info
podman pull quay.io/redhat-ai-tools/slack-mcp

# Docker
docker info
docker pull quay.io/redhat-ai-tools/slack-mcp
```

Re-extract `xoxc`/`xoxd` when the browser session expires (tokens are session-bound).

### GitHub: 401 / Authentication Failed

- Confirm the PAT is not expired and has `repo` (or equivalent fine-grained Contents/PRs/Issues) plus `read:org` for private org repos
- For the remote server, the URL must be `https://api.githubcopilot.com/mcp/`
- For the container path: `podman pull ghcr.io/github/github-mcp-server` or `docker pull ghcr.io/github/github-mcp-server` (if pull fails: `podman logout ghcr.io` / `docker logout ghcr.io` then retry)

### GitLab: only `manage_context` / disconnected

1. Connect to Red Hat VPN
2. Confirm `GITLAB_API_URL=https://gitlab.cee.redhat.com/` and `GITLAB_TOKEN` in `~/.config/mcp-tools/gitlab.env`
3. Confirm Node.js 24+: `node -v`
4. Recreate the PAT at https://gitlab.cee.redhat.com/-/user_settings/personal_access_tokens if needed

### Konflux Portal: cannot connect or DNS failures

1. Connect to Red Hat VPN
2. Recreate the network if you recently recreated the Podman machine:

```bash
podman network exists mcp-net || podman network create mcp-net
# Docker: docker network inspect mcp-net >/dev/null 2>&1 || docker network create mcp-net
```

3. Pull the image: `podman pull quay.io/eisraeli/konflux-portal-mcp@sha256:9476ead9108e6630e22061ab45ed2ffbb157697040103b1cb901a4178ddb71d8` (or `docker pull ...`)
4. Keep the `--dns=10.11.5.19` and `--dns=10.2.32.1` args (Red Hat VPN resolvers) — without them the container cannot resolve internal portal hosts

### Sumo Logic: needs authentication / 401

1. Confirm the MCP URL matches your Sumo deployment ([deployment table](https://www.sumologic.com/help/docs/api/mcp-server/))
2. In Claude Code run `/mcp` → sumo-logic → **Authenticate** (browser IdP login)
3. Ask an org admin to confirm both gates: **MCP Server access** (Administration → Feature Management) and **Enable CIMD Clients** (Administration → Account Security Settings → Policies)
4. To switch orgs: `/mcp` → sumo-logic → Clear authentication, then Authenticate again

### Jira: 401 / 403

- Username must be the Atlassian account email (`you@redhat.com`), not a display name
- Recreate the API token at https://id.atlassian.com/manage-profile/security/api-tokens and update `~/.config/mcp-tools/mcp-atlassian.env`
- Confirm `JIRA_URL=https://redhat.atlassian.net` (no trailing path)
- If logs appear on stdout, set `MCP_LOGGING_STDOUT=false` (already the default) so they cannot leak onto the JSON-RPC channel

### DevLake: 401/403 or connection refused

1. Connect to Red Hat VPN
2. Get a fresh offline token from https://console.redhat.com/openshift/token and update `Authorization` in `~/.claude.json` (token only after `Bearer `)
3. Confirm the URL path ends with `/mcp`

### ngit-memory: brain not found

```bash
ls -la ~/neural-memory
# Re-run from the neural-git clone:
./setup-brain.sh
# or: ./setup-brain.sh --brain-path ~/neural-memory
```

</details>

---

<details id="worked-examples">
<summary><strong>Part 5: Worked Examples</strong></summary>

Replace `<tenant-namespace>` and PipelineRun names with resources your kubeconfig can read. Claude's natural-language summary will vary; the JSON shapes below show the key fields each tool returns.

---

<details id="example-1">
<summary><strong>Example 1: Investigate a Failed Pipeline</strong></summary>

**Scenario:** A PipelineRun failed in a tenant namespace you can access.

#### Step 1: List recent runs

**Prompt:**

```text
List recent pipeline runs in namespace "<tenant-namespace>" and show any failures
```

**Tool:** `list_pipelineruns`

**Expected output:**

```json
[
  {
    "name": "build-pipeline-run-abc12",
    "pipeline": "build-pipeline",
    "status": "Failed",
    "started_at": "2026-08-04T09:15:00Z",
    "completed_at": "2026-08-04T09:19:32Z",
    "duration": "4m32s",
    "duration_seconds": 272
  },
  {
    "name": "build-pipeline-run-def34",
    "pipeline": "build-pipeline",
    "status": "Succeeded",
    "started_at": "2026-08-03T16:30:00Z",
    "completed_at": "2026-08-03T16:36:12Z",
    "duration": "6m12s",
    "duration_seconds": 372
  }
]
```

#### Step 2: Analyze the failure

**Prompt:**

```text
Analyze the failed pipeline run "<pipelinerun-name>" in namespace "<tenant-namespace>"
```

**Tool:** `analyze_failed_pipeline`

**Expected output:**

```json
{
  "pipeline_name": "build-pipeline",
  "pipeline_status": "Failed",
  "overall_message": "Tasks Completed: 1 (Failed: 1, Cancelled 0), Skipped: 0",
  "failed_task_count": 1,
  "failed_tasks": [
    {
      "task_name": "build-image",
      "task_run": "build-pipeline-run-abc12-build-image",
      "status": "Failed",
      "message": "step build exited with code 1",
      "error_patterns": ["Step 'build' failed with exit code 1"],
      "error_categories": { "step_failures": 1 },
      "pod": "build-pipeline-run-abc12-build-image-pod",
      "failed_steps": [
        { "step_name": "build", "exit_code": 1, "reason": "Error" }
      ]
    }
  ],
  "probable_root_cause": "Task 'build-image' failed: step build exited with code 1",
  "recommended_actions": [
    "Inspect TaskRun logs for the failed step",
    "Re-run analyze_failed_pipeline after confirming the PipelineRun name",
    "Check recent related events in the namespace"
  ]
}
```

#### Step 3: Optional Slack context

**Prompt:**

```text
Search Slack for messages about "<pipelinerun-name>" or the failing task name from the analysis
```

**Tool:** `search_messages`  
Results depend on your workspace; do not expect a fixed transcript.

#### Step 4: Store the finding

**Prompt:**

```text
Remember: <short factual summary of root cause and namespace/PipelineRun>
```

**Tool:** `memory_learn`

</details>

---

<details id="example-2">
<summary><strong>Example 2: Check Namespace Health</strong></summary>

**Scenario:** On-call shift start; verify a namespace you can read.

#### Step 1: Overview

**Prompt:**

```text
Give me a comprehensive health check of the "<tenant-namespace>" namespace
```

**Tool:** `conservative_namespace_overview`

**Expected output:**

```json
{
  "overview": {
    "namespace": "<tenant-namespace>",
    "total_pods": 14,
    "pods_analyzed": 10,
    "pods_with_issues": 2,
    "critical_issues_found": 1,
    "analysis_strategy": "conservative sampling of 10/14 pods"
  },
  "pod_findings": {
    "controller-7f8d9c6b4-x2m1p": {
      "status": "Running",
      "log_lines": 420,
      "patterns_found": 3,
      "has_errors": true,
      "has_warnings": true,
      "top_issue": "OOMKilled: container last terminated with exit code 137..."
    }
  },
  "critical_issues": [
    "Pod controller-7f8d9c6b4-x2m1p: OOMKilled: container last terminated with exit code 137..."
  ],
  "recommendations": [
    "Found 1 issues requiring investigation",
    "Pod controller-7f8d9c6b4-x2m1p: OOMKilled: container last terminated with exit code 137...",
    "Analyzed 10/14 pods - use focused investigation for complete coverage"
  ],
  "conservative_metadata": {
    "token_budget": "<45,000 tokens (conservative)",
    "sampling_strategy": "smart",
    "coverage_ratio": "10/14",
    "optimized_for": "large_namespaces"
  }
}
```

> **Note:** Pod `status` here is the Kubernetes phase (`Running`, `Failed`, …). Reasons like `CrashLoopBackOff` / `OOMKilled` show up in log-derived fields (`top_issue`, `critical_issues` strings), not as the pod phase. For container waiting/termination state without log analysis, use `list_pods_in_namespace` or `check_resource_constraints`.

#### Step 2: Logs for an unhealthy pod

**Prompt:**

```text
Summarize the logs for pod "<pod-name>" in namespace "<tenant-namespace>"
```

**Tool:** `smart_summarize_pod_logs`

#### Step 3: Optional resource check

**Prompt:**

```text
Check resource constraints for the "<tenant-namespace>" namespace
```

**Tool:** `check_resource_constraints`

#### Step 4: Optional engineering metrics

**Prompt:**

```text
Get PR stats for DevLake project "Secureflow - Konflux - Build Team" for the last 7 days
```

**Tool:** `get_pr_stats`

**Expected output (summary section):**

```json
{
  "success": true,
  "project_name": "Secureflow - Konflux - Build Team",
  "analysis_period_days": 7,
  "summary": {
    "total_prs": 55,
    "merged_prs": 16,
    "open_prs": 30,
    "closed_prs": 9,
    "merge_rate": 29.1,
    "stale_prs_7d": 0,
    "stale_prs_14d": 0
  },
  "repo_breakdown": [
    { "repo_name": "konflux-ci/build-definitions", "total_prs": 22, "merged_prs": 8, "merge_rate": 36.4 }
  ],
  "pr_type_breakdown": {
    "dependency_bot": { "total": 19, "merged": 3, "merge_rate": 15.8 },
    "engineering": { "total": 36, "merged": 13, "merge_rate": 36.1 }
  }
}
```

</details>

---

<details id="example-3">
<summary><strong>Example 3: Gather Incident Context from PagerDuty + Lumino</strong></summary>

**Scenario:** You need incident context from PagerDuty, then cluster state from Lumino.

#### Step 1: List active incidents

**Prompt:**

```text
List triggered PagerDuty incidents
```

**Tool:** `list_incidents` with `status=triggered`

**Expected output:**

```json
{
  "incidents": [
    {
      "id": "Q285NZXJG65XDH",
      "type": "incident",
      "summary": "[#3124279] Warning Alert: SP vitals-testing-warning",
      "incident_number": 3124279,
      "status": "triggered",
      "title": "Warning Alert: SP vitals-testing-warning",
      "created_at": "2026-07-06T11:19:22Z",
      "urgency": "high",
      "service": {
        "id": "PS0U7AJ",
        "type": "service_reference",
        "summary": "vitals-testing"
      },
      "assignments": [
        {
          "at": "2026-07-06T11:19:22Z",
          "assignee": { "id": "PPZZAYT", "type": "user_reference", "summary": "Nobody SP" }
        }
      ]
    }
  ]
}
```

#### Step 2: Incident details

**Prompt:**

```text
Get details for PagerDuty incident "<incident_id>"
```

**Tool:** `get_incident` (`incident_id` required)

**Expected output:**

```json
{
  "incident": {
    "id": "Q285NZXJG65XDH",
    "type": "incident",
    "status": "triggered",
    "title": "Warning Alert: SP vitals-testing-warning",
    "created_at": "2026-07-06T11:19:22Z",
    "urgency": "high",
    "html_url": "https://redhat.pagerduty.com/incidents/Q285NZXJG65XDH",
    "service": { "id": "PS0U7AJ", "summary": "vitals-testing" },
    "assignments": [
      { "at": "2026-07-06T11:19:22Z", "assignee": { "summary": "Nobody SP" } }
    ]
  }
}
```

#### Step 3: Cluster / namespace check with Lumino

**Prompt:**

```text
Investigate namespace "<namespace-from-incident-or-known-service>" — pods, events, anything abnormal
```

**Tool:** `adaptive_namespace_investigation` or `conservative_namespace_overview`

**Expected output (shape for `conservative_namespace_overview`):** same fields as [Example 2](#example-2) — `overview`, `pod_findings`, `critical_issues` (string list), `recommendations`, `conservative_metadata`. Values depend on the namespace tied to the incident.

#### Step 4: Optional Slack

**Prompt:**

```text
Search Slack for messages about this incident title or service name today
```

**Tool:** `search_messages`

#### Step 5: Record findings

**Prompt:**

```text
Remember: <incident id, service, Lumino findings, next action>
```

**Tool:** `memory_learn`

#### Flow

```text
list_incidents (PagerDuty)
    -> get_incident (PagerDuty)
        -> namespace investigation (Lumino)
            -> optional search_messages (Slack)
                -> memory_learn (ngit-memory)
```

</details>

</details>

---

<details id="next-steps">
<summary><strong>Next Steps</strong></summary>

- Run each example against a cluster and PagerDuty account you can access
- Browse tools with `/mcp`; try prompts from [Usage Examples](../README.md#usage-examples)
- Marketplace skill for JIRA-driven work: `/technical-investigation:ticket-investigation` (from `technical-investigation@spre-ai-marketplace`; see [SPRE AI Marketplace — GitLab MCP](https://gitlab.cee.redhat.com/spre-ai-marketplace#gitlab-mcp) for plugin setup) — needs the [Jira MCP](#jira) connected
- Use Konflux Portal when you need env URLs or tenant→cluster mapping before opening Lumino
- Contributing new Lumino tools: [Tool Development Guide](./LUMINO_MCP_TOOL_DEVELOPMENT_GUIDE.md)

</details>
