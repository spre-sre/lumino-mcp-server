# LUMINO MCP Server

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![MCP](https://img.shields.io/badge/MCP-1.10%2B-green.svg)](https://modelcontextprotocol.io/)
[![PyPI](https://img.shields.io/pypi/v/lumino-mcp-server.svg)](https://pypi.org/project/lumino-mcp-server/)

<!-- mcp-name: io.github.geored/lumino -->

An open source MCP (Model Context Protocol) server empowering SREs with intelligent observability, predictive analytics, and AI-driven automation across Kubernetes, OpenShift, and Tekton environments.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Team Onboarding Guide](#team-onboarding-guide)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [ML Model Persistence](#ml-model-persistence)
- [KubeArchive Integration](#kubearchive-integration)
- [Prometheus/Thanos Integration](#prometheusthanos-integration)
- [Available Tools](#available-tools)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [MCP Client Integration](#mcp-client-integration)
- [Performance Considerations](#performance-considerations)
- [Troubleshooting](#troubleshooting)
- [Dependencies](#dependencies)
- [Contributing](#contributing)
- [Security](#security)
- [License](#license)
- [Acknowledgments](#acknowledgments)

## Team Onboarding Guide

New to the SPRE MCP diagnostics stack (Lumino, PagerDuty, Slack, DevLake, ngit-memory)? Start here:

- **[docs/ONBOARDING_GUIDE.md](docs/ONBOARDING_GUIDE.md)** — setup, first diagnostic, troubleshooting, three worked examples, and expected tool outputs

## Overview

LUMINO MCP Server transforms how Site Reliability Engineers (SREs) and DevOps teams interact with Kubernetes clusters. By exposing 39 specialized tools through the Model Context Protocol, it enables AI assistants to:

- **Monitor** cluster health, resources, and pipeline status in real-time
- **Analyze** logs, events, and anomalies using statistical and ML techniques
- **Troubleshoot** failed pipelines with automated root cause analysis
- **Predict** resource bottlenecks and potential issues before they occur
- **Simulate** configuration changes to assess impact before deployment

## Features

### Kubernetes & OpenShift Operations

- Namespace and pod management
- Resource querying with flexible output formats
- Label-based resource search across clusters
- OpenShift operator and MachineConfigPool status
- etcd log analysis

### Tekton Pipeline Intelligence

- Pipeline and task run monitoring across namespaces
- Detailed log retrieval with optional cleaning
- Failed pipeline root cause analysis
- Cross-cluster pipeline tracing
- CI/CD performance baselining

### Advanced Log Analysis

- Smart log summarization with configurable detail levels
- Streaming analysis for large log volumes
- Hybrid analysis combining multiple strategies
- Semantic search using NLP techniques
- Anomaly detection with severity classification

### Predictive & Proactive Monitoring

- Statistical anomaly detection using z-score analysis
- Predictive log analysis for early warning
- Resource bottleneck forecasting
- Certificate health monitoring with expiry alerts
- TLS certificate issue investigation

### Event Intelligence

- Smart event retrieval with multiple strategies
- Progressive event analysis (overview to deep-dive)
- Advanced analytics with ML pattern detection
- Log-event correlation

### Simulation & What-If Analysis

- Monte Carlo simulation for configuration changes
- Impact analysis before deployment
- Risk assessment with configurable tolerance
- Affected component identification

## Quick Start

Get started with LUMINO in under 2 minutes:

### For Claude Code CLI Users (Easiest)

Simply ask Claude Code to provision the Lumino MCP server for you by pasting this prompt:

```text
Provision the Lumino MCP server as a project-local MCP integration:

1. Clone the repository:
   git clone https://github.com/spre-sre/lumino-mcp-server.git

2. Install Python dependencies using uv:
   cd lumino-mcp-server && uv sync

3. Create .mcp.json in the current project root (NOT inside lumino-mcp-server) with this configuration.
   IMPORTANT: Replace <ABSOLUTE_PATH_TO_LUMINO> with the actual absolute path to the cloned lumino-mcp-server directory:

   {
     "mcpServers": {
       "lumino": {
         "type": "stdio",
         "command": "<ABSOLUTE_PATH_TO_LUMINO>/.venv/bin/python",
         "args": ["<ABSOLUTE_PATH_TO_LUMINO>/main.py"],
         "env": {
           "PYTHONUNBUFFERED": "1"
         }
       }
     }
   }

4. After creating .mcp.json, inform the user to:
   - Exit Claude Code completely
   - Connect to their Kubernetes or OpenShift cluster (kubectl/oc login)
   - Restart Claude Code in this project directory
   - They will see a prompt to approve the Lumino MCP server
   - Once approved, Lumino tools will be available (check with /mcp command)
```

### For Other MCP Clients

Choose your preferred installation method:

- **MCPM (Recommended)**: `mcpm install @spre-sre/lumino-mcp-server`
- **Manual Setup**: See detailed [MCP Client Integration](#mcp-client-integration) instructions

### Verify Installation

Once installed, test with a simple query:

```text
"List all namespaces in my Kubernetes cluster"
```

## Prerequisites

### Required

- **Python 3.10 or higher** - Core runtime
- **MCP Client** - One of:
  - [Claude Desktop](https://claude.ai/download)
  - [Claude Code CLI](https://github.com/anthropics/claude-code)
  - [Gemini CLI](https://github.com/google/generative-ai-cli)
  - [Cursor IDE](https://cursor.sh/)

### For Kubernetes Features

- **Kubernetes/OpenShift Access** - Valid kubeconfig with read permissions
- **RBAC Permissions** - Ability to list pods, namespaces, and other resources

### Optional (Recommended)

- **[uv](https://docs.astral.sh/uv/)** - Faster dependency management than pip
- **[MCPM](https://github.com/spre-sre/mcpm)** - Easiest installation experience
- **Prometheus** - For advanced metrics and forecasting features

## Installation

### Using uv (recommended)

```bash
# Clone the repository
git clone https://github.com/spre-sre/lumino-mcp-server.git
cd lumino-mcp-server

# Install dependencies
uv sync

# Run the server
uv run python main.py
```

### Using pip

```bash
# Clone the repository
git clone https://github.com/spre-sre/lumino-mcp-server.git
cd lumino-mcp-server

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -e .

# Run the server
python main.py
```

## Usage

### Local Mode (stdio transport)

By default, the server runs in local mode using stdio transport, suitable for direct integration with MCP clients:

```bash
python main.py
```

### Kubernetes Mode (HTTP streaming transport)

When running inside Kubernetes, set the namespace environment variable to enable HTTP streaming:

```bash
export KUBERNETES_NAMESPACE=my-namespace
python main.py
```

The server automatically detects the environment and switches transport modes.

## Usage Examples

### 🔍 Intelligent Root Cause Analysis

Investigate and diagnose complex failures with automated analysis:

```text
"Generate a comprehensive RCA report for the failed pipeline run 'build-api-pr-456' in namespace ci-cd"
```

```text
"Analyze what caused pod crashes in namespace production over the last 6 hours and correlate with resource events"
```

```text
"Investigate the TLS certificate issues affecting services in namespace ingress-nginx"
```

### 🎯 Predictive Intelligence & Forecasting

Anticipate problems before they impact your systems:

```text
"Predict resource bottlenecks across all production namespaces for the next 48 hours"
```

```text
"Analyze historical pipeline performance and detect anomalies in build times for the last 30 days"
```

```text
"Check cluster certificate health and alert me about any certificates expiring in the next 60 days"
```

```text
"Use predictive log analysis to identify potential failures in namespace monitoring before they occur"
```

### 🧪 Simulation & What-If Analysis

Test changes safely before applying them to production:

```text
"Simulate the impact of increasing memory limits to 4Gi for all pods in namespace backend-services"
```

```text
"Run a what-if scenario for scaling deployments to 10 replicas and analyze resource consumption"
```

```text
"Simulate configuration changes for nginx ingress controller and assess risk to existing traffic"
```

### 🗺️ Topology & Dependency Mapping

Understand system architecture and component relationships:

```text
"Generate a live topology map of all services, deployments, and their dependencies in namespace microservices"
```

```text
"Map the complete dependency graph for the payment-service including all connected resources"
```

```text
"Show me the topology of components affected by the cert-manager service"
```

### 🔬 Advanced Investigation & Forensics

Deep-dive into complex issues with multi-faceted analysis:

```text
"Perform an adaptive namespace investigation for production - analyze logs, events, and resource patterns"
```

```text
"Create a detailed investigation report for resource constraints and bottlenecks in namespace data-processing"
```

```text
"Trace pipeline execution for commit SHA abc123def from source to deployment across all namespaces"
```

```text
"Search logs semantically for 'authentication failures related to expired tokens' across the last 24 hours"
```

### 📊 CI/CD Pipeline Intelligence

Optimize and troubleshoot your continuous delivery pipelines:

```text
"Establish performance baselines for all Tekton pipelines and flag runs deviating by more than 2 standard deviations"
```

```text
"Trace the complete pipeline flow for image 'api:v2.5.3' from build to production deployment"
```

```text
"Analyze failed pipeline runs in namespace tekton-pipelines and identify common failure patterns"
```

```text
"Compare current pipeline run times against 30-day baseline and highlight performance degradation"
```

### 🎨 Progressive Event Analysis

Multi-level event investigation from overview to deep-dive:

```text
"Start with an overview of events in namespace kube-system, then drill down into critical issues"
```

```text
"Perform advanced event analytics with ML pattern detection for namespace monitoring over the last 12 hours"
```

```text
"Correlate events with pod logs to identify the root cause of CrashLoopBackOff in namespace applications"
```

### 🚀 Real-Time Monitoring & Alerts

Stay informed about cluster health and pipeline status:

```text
"Show me the status of all Tekton pipeline runs cluster-wide and highlight long-running pipelines"
```

```text
"List all failed TaskRuns in the last hour with error details and recommended actions"
```

```text
"Monitor OpenShift cluster operators and alert on any degraded components"
```

```text
"Check MachineConfigPool status and show which nodes are being updated"
```

### 🔐 Security & Compliance

Ensure cluster security and certificate management:

```text
"Scan all namespaces for expiring certificates and generate a renewal schedule"
```

```text
"Investigate TLS certificate issues causing handshake failures in namespace istio-system"
```

```text
"Audit all secrets and configmaps for sensitive data exposure patterns"
```

### 📈 Advanced Analytics & ML Insights

Leverage machine learning for pattern detection:

```text
"Use streaming log analysis to process large log volumes from namespace data-pipeline with error pattern detection"
```

```text
"Detect anomalies in log patterns using ML analysis with medium severity threshold for namespace api-gateway"
```

```text
"Analyze resource utilization trends using Prometheus metrics and forecast capacity needs"
```

## Configuration

### Kubernetes Authentication

The server automatically detects Kubernetes configuration:

1. **In-cluster config** - When running inside a Kubernetes pod
2. **Local kubeconfig** - When running locally (uses `~/.kube/config`)

### Environment Variables

| Variable | Description | Default | When to Use |
|----------|-------------|---------|-------------|
| `KUBERNETES_NAMESPACE` | Namespace for K8s mode | - | When running server inside a Kubernetes pod |
| `K8S_NAMESPACE` | Alternative namespace variable | - | Alternative to `KUBERNETES_NAMESPACE` |
| `PROMETHEUS_URL` | Prometheus server URL for metrics | Auto-detected | Custom Prometheus endpoint or non-standard port |
| `KUBECONFIG` | Path to kubeconfig file | `~/.kube/config` | Multiple clusters or custom kubeconfig location |
| `LOG_LEVEL` | Logging verbosity (DEBUG, INFO, WARNING, ERROR) | `INFO` | Debugging issues or reducing log noise |
| `MCP_SERVER_LOG_LEVEL` | MCP framework log level | `INFO` | Troubleshooting MCP protocol issues |
| `PYTHONUNBUFFERED` | Disable Python output buffering | - | Recommended for MCP clients to see real-time logs |
| `KUBEARCHIVE_HOST` | Explicit KubeArchive API endpoint URL | Auto-detected | Custom KubeArchive endpoint or non-standard deployment |
| `KUBEARCHIVE_ENABLED` | Enable/disable KubeArchive integration | `true` | Set to `false` to disable KubeArchive queries entirely |
| `THANOS_URL` | Thanos Query endpoint URL (highest priority for metrics) | Auto-detected | Custom Thanos Query endpoint; takes precedence over `PROMETHEUS_URL` |
| `PROMETHEUS_TOKEN` | Bearer token for Prometheus/Thanos authentication | Auto-detected | Explicit auth token when auto-detection fails |
| `OPENSHIFT_TOKEN` | OpenShift bearer token for Prometheus/Thanos | Auto-detected | Alternative to `PROMETHEUS_TOKEN` for OpenShift clusters |
| `OC_TOKEN` | OpenShift CLI token fallback | Auto-detected | Last-resort token fallback for Prometheus/Thanos auth |

## ML Model Persistence

The `predictive_log_analyzer` tool persists trained ML models and training data locally in `~/.lumino/`. This enables model reuse across server restarts and incremental learning from historical failure patterns.

### Directory Structure

```text
~/.lumino/
├── models/                     # Trained ML models
│   ├── {model_id}.joblib       # Serialized model (e.g. IsolationForest via joblib)
│   ├── {model_id}.meta.json    # Model metadata (created, last used, performance metrics)
│   └── model_index.json        # Index tracking all models and the current active model
└── training_data/              # Training data store
    └── training_data.db        # SQLite database
```

Model IDs follow the pattern `predictive_log_v1_YYYYMMDD_HHMMSS`.

### Training Data Database

The SQLite database (`training_data.db`) contains four tables:

| Table | Purpose |
|-------|---------|
| `log_samples` | Preprocessed log samples with extracted features, namespace, pod name, error indicators, and message entropy |
| `failure_labels` | Failure events collected from Kubernetes events, failed PipelineRuns, and unhealthy pod statuses. Failure types include: `oom`, `crash`, `image`, `scheduling`, `storage`, `config`, `health`, `network`, `timeout`, `pipeline_failure`, `permission`, `resource_limits`, `general`, `pod_failure` |
| `log_failure_correlations` | Time-proximity correlations between log samples and failure events (scored 0.5--1.0 based on temporal distance within a 30-minute window) |
| `training_runs` | Training run history recording model_id, samples used, labels used, performance metrics, and completion status |

All four tables define a `cluster_id` column for multi-cluster support. Currently, only `log_samples` and `failure_labels` actively populate it during writes; `log_failure_correlations` and `training_runs` leave it NULL. The cluster ID is derived from the active kubeconfig context name (e.g. `api-stone-prod-p02-hjvn-p1-openshiftapps-com:6443`) or falls back to `in-cluster-{KUBERNETES_SERVICE_HOST}` when running inside a pod.

### Clearing Stale Data

**Programmatic cleanup** via the `manage_prediction_training_data` tool (action `cleanup`):

- `cleanup_old_models(max_age_days=30, keep_min=3)` -- removes models older than 30 days, always keeping the 3 most recent
- `cleanup_old_data(max_age_days=90)` -- removes log samples, failure labels, and correlations older than 90 days

**Manual cleanup**:

```bash
rm -rf ~/.lumino/              # Clear everything (models + training data)
rm -rf ~/.lumino/models/       # Clear just models
rm -rf ~/.lumino/training_data/ # Clear just training data (SQLite DB)
```

**Disk usage note**: Models accumulate over time. The default cleanup keeps models up to 30 days old with a minimum of 3 retained. Training data is kept for 90 days. Run the `manage_prediction_training_data` tool with action `cleanup` periodically to reclaim disk space.

## KubeArchive Integration

KubeArchive stores Kubernetes resources off-cluster and provides a REST API for historical resource states and logs. LUMINO uses KubeArchive as a fallback when pods, PipelineRuns, or TaskRuns have been garbage-collected from the live cluster. The `query_kubearchive` tool queries this archive transparently.

### Endpoint Auto-Discovery

The endpoint is discovered automatically using a 5-step chain (first match wins):

1. **`KUBEARCHIVE_HOST` environment variable** (highest priority)
2. **OpenShift Route** named `kubearchive-api-server` in namespaces: `kubearchive`, `product-kubearchive`, `default`
3. **Kubernetes Ingress** named `kubearchive-api-server` in the same namespaces
4. **Kubernetes Service** named `kubearchive-api-server` (in-cluster DNS: `https://kubearchive-api-server.<namespace>.svc.cluster.local:<port>`)
5. **Kubeconfig-based Route inference** -- constructs candidate URLs from the API server domain (pattern: `https://kubearchive-api-server-{namespace}.apps.{cluster-domain}`) and probes `/livez`

Results are cached at startup. On connection failure, the cache is cleared and re-probed on the next request.

### Local Development (Port-Forwarding)

When running outside the cluster, if an in-cluster Service endpoint is discovered (step 4), LUMINO automatically sets up `kubectl port-forward`:

```bash
kubectl port-forward -n {namespace} svc/kubearchive-api-server {local_port}:{remote_port}
```

- Tries ports 8081--8090, then falls back to a system-assigned port
- The port-forward process is auto-started and auto-cleaned up on server exit
- If `kubectl` is not available, LUMINO logs a manual fallback command for the user

### Authentication

KubeArchive authentication uses the following priority chain:

1. Provided token (from constructor / previous session)
2. In-cluster service account token (`/var/run/secrets/kubernetes.io/serviceaccount/token`)
3. Existing Kubernetes client token (extracted from the API client initialized at server startup)
4. OpenShift `oc whoami -t` token (for OpenShift clusters)
5. Auto-created short-lived service account token (`kubectl create token`, 1-hour duration, Kubernetes only)

### KubeArchive Configuration

See the [Configuration](#configuration) section above for `KUBEARCHIVE_HOST` and `KUBEARCHIVE_ENABLED` environment variables.

## Prometheus/Thanos Integration

LUMINO auto-discovers Prometheus or Thanos Query endpoints for the `prometheus_query`, `resource_bottleneck_forecaster`, and `ci_cd_performance_baselining_tool` tools. Thanos Query implements the Prometheus HTTP API and is preferred when available since it provides a unified, deduplicated view across replicas.

### Endpoint Discovery Priority

| Priority | Source | Endpoint Type |
|----------|--------|---------------|
| 0 | `THANOS_URL` env var | thanos |
| 1 | `PROMETHEUS_URL` env var | prometheus |
| 2 | Predefined cluster endpoints (in code) | varies |
| 3 | 5-minute TTL cache | cached |
| 4 | Auto-discovery chain (see below) | detected |
| 5 | Predefined fallback endpoints | varies |

Auto-discovery order depends on runtime environment:

- **In-cluster**: Thanos services --> Prometheus services --> Prometheus Operator CRD --> OpenShift Routes
- **Local/outside cluster**: OpenShift Routes --> Thanos services --> Prometheus Operator CRD --> Prometheus services

### Auto-Discovery Details

**OpenShift Routes**: Searches the `openshift-monitoring` namespace. Prefers the `thanos-querier` route over `prometheus-k8s`. Falls back to any route with `prometheus` in the name. Detects protocol from TLS termination config.

**Thanos Services**: Searches namespaces `openshift-monitoring`, `monitoring`, `thanos`, `observability`, `kube-prometheus`. Priority service names: `thanos-query-frontend`, `thanos-querier`, `thanos-query`. Also searches via label selectors: `app.kubernetes.io/name=thanos-query`, `app.kubernetes.io/component=query,app.kubernetes.io/name=thanos`, `app=thanos-query`, `app=thanos-querier`.

**Prometheus Services**: Searches namespaces `openshift-monitoring`, `monitoring`, `prometheus`, `kube-prometheus`, `observability`. Priority service names: `prometheus-server`, `prometheus-k8s`, `prometheus`. Also searches via label selectors: `app=prometheus`, `app.kubernetes.io/name=prometheus`, `app.kubernetes.io/component=prometheus`.

**Prometheus Operator CRD**: Discovers via `monitoring.coreos.com/v1` Prometheus custom resources and their associated services (pattern: `prometheus-{name}` in the same namespace).

### Prometheus/Thanos Authentication

Authentication for Prometheus/Thanos uses the following 5 methods in priority order:

1. `oc whoami -t` -- fresh OpenShift token (most reliable for OpenShift)
2. Re-read kubeconfig file for current token
3. In-memory Kubernetes client config token
4. ServiceAccount token file (`/var/run/secrets/kubernetes.io/serviceaccount/token`)
5. Environment variables: `PROMETHEUS_TOKEN`, `OPENSHIFT_TOKEN`, `OC_TOKEN` (checked in that order)

### Configuration Example

```json
{
  "env": {
    "THANOS_URL": "https://thanos-querier.example.com",
    "PROMETHEUS_TOKEN": "your-bearer-token"
  }
}
```

**Note**: The endpoint cache has a 5-minute TTL. If you change `THANOS_URL` or `PROMETHEUS_URL` at runtime, the new value takes effect on the next query.

## Available Tools

### Kubernetes Core (5 tools)

| Tool | Description |
|------|-------------|
| `list_namespaces` | List all namespaces in the cluster |
| `list_pods_in_namespace` | List pods with status and placement info |
| `get_kubernetes_resource` | Get any Kubernetes resource with flexible output |
| `search_resources_by_labels` | Search resources across namespaces by labels |
| `query_kubearchive` | Query archived Kubernetes resources from KubeArchive with optional log retrieval |

### Tekton Pipelines (6 tools)

| Tool | Description |
|------|-------------|
| `list_pipelineruns` | List PipelineRuns with status and timing |
| `list_taskruns` | List TaskRuns, optionally filtered by pipeline |
| `get_pipelinerun_logs` | Retrieve pipeline logs with optional cleaning |
| `list_recent_pipeline_runs` | Recent pipelines across all namespaces |
| `find_pipeline` | Find pipelines by pattern matching |
| `get_tekton_pipeline_runs_status` | Cluster-wide pipeline status summary |

### Log Analysis (8 tools)

| Tool | Description |
|------|-------------|
| `analyze_logs` | Extract error patterns from log text |
| `smart_summarize_pod_logs` | Intelligent log summarization |
| `stream_analyze_pod_logs` | Streaming analysis for large logs |
| `analyze_pod_logs_hybrid` | Combined analysis strategies |
| `detect_log_anomalies` | Anomaly detection with severity levels |
| `semantic_log_search` | NLP-based semantic log search |
| `templatize_pod_logs` | Cluster logs into unique structural templates using Drain3 (requires optional `logan` dependency) |
| `deep_analyze_pod_logs` | Classify log templates into golden signals and fault categories using zero-shot ML (requires optional `logan` dependency) |


### Event Analysis (3 tools)

| Tool | Description |
|------|-------------|
| `smart_get_namespace_events` | Smart event retrieval with strategies |
| `progressive_event_analysis` | Multi-level event analysis |
| `advanced_event_analytics` | ML-powered event pattern detection |

### Failure Analysis & RCA (2 tools)

| Tool | Description |
|------|-------------|
| `analyze_failed_pipeline` | Root cause analysis for failed pipelines |
| `automated_triage_rca_report_generator` | Automated incident reports |

### Resource Monitoring (4 tools)

| Tool | Description |
|------|-------------|
| `check_resource_constraints` | Detect resource issues in namespace |
| `detect_anomalies` | Statistical anomaly detection |
| `prometheus_query` | Execute PromQL queries |
| `resource_bottleneck_forecaster` | Predict resource exhaustion |

### Namespace Investigation (2 tools)

| Tool | Description |
|------|-------------|
| `conservative_namespace_overview` | Focused namespace health check |
| `adaptive_namespace_investigation` | Dynamic investigation based on query |

### Certificate & Security (2 tools)

| Tool | Description |
|------|-------------|
| `investigate_tls_certificate_issues` | Find TLS-related problems |
| `check_cluster_certificate_health` | Certificate expiry monitoring |

### OpenShift Specific (3 tools)

| Tool | Description |
|------|-------------|
| `get_machine_config_pool_status` | MachineConfigPool status and updates |
| `get_openshift_cluster_operator_status` | Cluster operator health |
| `get_etcd_logs` | etcd log retrieval and analysis |

### CI/CD Performance (2 tools)

| Tool | Description |
|------|-------------|
| `ci_cd_performance_baselining_tool` | Pipeline performance baselines |
| `pipeline_tracer` | Trace pipelines by commit, PR, or image |

### Topology & Prediction (3 tools)

| Tool | Description |
|------|-------------|
| `live_system_topology_mapper` | Real-time system topology mapping |
| `predictive_log_analyzer` | Predict issues from log patterns |
| `manage_prediction_training_data` | Manage training data for predictive log analyzer |

### Simulation (1 tool)

| Tool | Description |
|------|-------------|
| `what_if_scenario_simulator` | Simulate configuration changes |

## Architecture

```text
lumino-mcp-server/
├── main.py                 # Entry point with transport detection
├── src/
│   ├── server-mcp.py       # MCP server with all 39 tools
│   └── helpers/
│       ├── constants.py              # Shared constants
│       ├── event_analysis.py         # Event processing logic
│       ├── failure_analysis.py       # RCA algorithms
│       ├── kubearchive_integration.py # KubeArchive API client & discovery
│       ├── log_analysis.py           # Log processing
│       ├── ml_persistence.py         # ML model & training data storage
│       ├── resource_topology.py      # Topology mapping
│       ├── semantic_search.py        # NLP search
│       └── utils.py                  # Utility functions
└── pyproject.toml          # Project configuration
```

## How It Works

LUMINO acts as a bridge between AI assistants and your Kubernetes infrastructure through the Model Context Protocol:

```text
┌─────────────────────────────────────────────────────────────────┐
│                        AI Assistant Layer                        │
│          (Claude Desktop, Claude Code CLI, Gemini CLI)          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Natural Language Queries
                             │ "Analyze failed pipelines"
                             │ "Predict resource bottlenecks"
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Model Context Protocol                       │
│                      (MCP Communication)                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Tool Invocations & Results
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                       LUMINO MCP Server                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ Log Analysis │  │ Event Intel  │  │  Predictive  │         │
│  │   (6 tools)  │  │  (3 tools)   │  │  (2 tools)   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │   Pipeline   │  │  Simulation  │  │   Topology   │         │
│  │  (6 tools)   │  │  (1 tool)    │  │  (2 tools)   │         │
│  └──────────────┘  └──────────────┘  └──────────────┘         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ Kubernetes API Calls
                             │ Prometheus Queries
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Kubernetes/OpenShift Cluster                  │
│                                                                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
│  │   Pods   │  │ Services │  │ Tekton   │  │etcd/Logs │       │
│  └──────────┘  └──────────┘  │Pipelines │  └──────────┘       │
│  ┌──────────┐  ┌──────────┐  └──────────┘  ┌──────────┐       │
│  │  Events  │  │ Configs  │  ┌──────────┐  │Prometheus│       │
│  └──────────┘  └──────────┘  │OpenShift │  └──────────┘       │
│                               │Operators │                       │
│                               └──────────┘                       │
└─────────────────────────────────────────────────────────────────┘
```

### Workflow

1. **User Query** → AI assistant receives natural language request
2. **MCP Translation** → Assistant converts query to appropriate tool calls
3. **LUMINO Processing** → Server executes Kubernetes/Prometheus operations
4. **Data Analysis** → ML/statistical algorithms process raw data
5. **AI Synthesis** → Assistant formats results into human-readable insights

### Key Features

- **Minimal Local State** - Queries cluster in real-time; optional ML model persistence in `~/.lumino/` for predictive analytics (see [ML Model Persistence](#ml-model-persistence))
- **Automatic Transport Detection** - Switches between stdio (local) and HTTP (K8s) modes
- **Token Budget Management** - Adaptive strategies to handle large log volumes
- **Intelligent Caching** - Smart caching for frequently accessed data
- **Security First** - Uses existing kubeconfig RBAC permissions, no separate auth

## MCP Client Integration

### Method 1: Using MCPM (Recommended for Claude Code CLI / Gemini CLI)

The easiest way to install LUMINO MCP Server for Claude Code CLI or Gemini CLI is using [MCPM](https://github.com/spre-sre/mcpm) - an MCP server package manager.

#### Install MCPM

```bash
# Clone and build MCPM
git clone https://github.com/spre-sre/mcpm.git
cd mcpm
go build -o mcpm .

# Optional: Add to PATH
sudo mv mcpm /usr/local/bin/
```

**Requirements**: Go 1.23+, Git, Python 3.10+, uv (or pip)

#### Install LUMINO MCP Server

```bash
# Install from GitHub repository (short syntax)
mcpm install @spre-sre/lumino-mcp-server

# Or use full GitHub URL
mcpm install https://github.com/spre-sre/lumino-mcp-server.git

# For GitLab repositories (if hosted on GitLab)
mcpm install gl:@spre-sre/lumino-mcp-server

# Install for specific client
mcpm install @spre-sre/lumino-mcp-server --claude  # For Claude Code CLI
mcpm install @spre-sre/lumino-mcp-server --gemini  # For Gemini CLI

# Install globally (works with both Claude and Gemini)
mcpm install @spre-sre/lumino-mcp-server --global
```

**Short syntax explained**:

- `@owner/repo` - Installs from GitHub (default: `https://github.com/owner/repo.git`)
- `gl:@owner/repo` - Installs from GitLab (`https://gitlab.com/owner/repo.git`)
- Full URL - Works with any Git repository

This will:

- Clone the repository to `~/.mcp/servers/lumino-mcp-server/`
- Auto-detect Python project and install dependencies using `uv` (or pip)
- Register with Claude Code CLI or Gemini CLI configuration automatically

#### Manage LUMINO

```bash
# List installed servers
mcpm list

# Update LUMINO
mcpm update lumino-mcp-server

# Remove LUMINO
mcpm remove lumino-mcp-server
```

---

### Method 2: Manual Configuration

If you prefer manual setup or need to configure Claude Desktop / Cursor, follow these client-specific guides:

#### Claude Desktop

1. **Find your config file location**:
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`
   - Linux: `~/.config/Claude/claude_desktop_config.json`

2. **Add LUMINO configuration**:

```json
{
  "mcpServers": {
    "lumino": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/lumino-mcp-server",
        "python",
        "main.py"
      ],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

3. **Restart Claude Desktop**

4. **Verify**: Look for the hammer icon (🔨) in Claude Desktop to see available tools

---

#### Claude Code CLI

**Option A: Using MCPM** (see Method 1 above)

**Option B: Automatic Provisioning via Claude Code** (Recommended and easiest way)

Copy and paste the provisioning prompt from the [Quick Start](#for-claude-code-cli-users-easiest) section above into Claude Code. Claude will clone the repository, install dependencies, and configure the MCP server for your project.

**Option C: Manual Configuration**

1. **Clone and install**:

```bash
git clone https://github.com/spre-sre/lumino-mcp-server.git
cd lumino-mcp-server
uv sync  # Creates .venv with all dependencies
```

2. **Create `.mcp.json`** in your project root (for project-local config) or update `~/.claude.json` (for global config):

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

**Important**: Replace `/absolute/path/to/lumino-mcp-server` with the actual absolute path where you cloned the repository (e.g., `/Users/username/projects/lumino-mcp-server`).

3. **Verify installation**:

```bash
# Check MCP servers
claude mcp list

# Test with a query
claude "List all namespaces in my cluster"
```

---

#### Gemini CLI

**Option A: Using MCPM** (Recommended - see Method 1 above)

**Option B: Manual Configuration**

1. **Find your config file location**:
   - macOS/Linux: `~/.config/gemini/mcp_servers.json`
   - Windows: `%APPDATA%\gemini\mcp_servers.json`

2. **Add LUMINO configuration**:

```json
{
  "mcpServers": {
    "lumino": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/lumino-mcp-server",
        "python",
        "main.py"
      ],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

3. **Verify installation**:

```bash
# Check MCP servers
gemini mcp list

# Test with a query
gemini "Show me failed pipeline runs"
```

---

#### Cursor IDE

1. **Open Cursor Settings**:
   - Press `Cmd+,` (macOS) or `Ctrl+,` (Windows/Linux)
   - Search for "MCP" or "Model Context Protocol"

2. **Add MCP Server Configuration**:

In Cursor's MCP settings, add:

```json
{
  "mcpServers": {
    "lumino": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/lumino-mcp-server",
        "python",
        "main.py"
      ],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

**Alternative - Using Cursor's settings.json**:

1. Open Command Palette (`Cmd+Shift+P` or `Ctrl+Shift+P`)
2. Type "Preferences: Open User Settings (JSON)"
3. Add the MCP configuration:

```json
{
  "mcp.servers": {
    "lumino": {
      "command": "uv",
      "args": [
        "run",
        "--directory",
        "/path/to/lumino-mcp-server",
        "python",
        "main.py"
      ],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

3. **Restart Cursor IDE**

4. **Verify**: Open Cursor's AI chat and check if LUMINO tools are available

---

### Configuration Notes

**Replace `/path/to/lumino-mcp-server`** with the actual path where you cloned the repository:

```bash
# Example paths:
# macOS/Linux: /Users/username/projects/lumino-mcp-server
# Windows: C:\Users\username\projects\lumino-mcp-server

# If installed via MCPM:
# ~/.mcp/servers/lumino-mcp-server/
```

**Environment Variables** (optional):

Add these to the `env` section if needed:

```json
{
  "env": {
    "PYTHONUNBUFFERED": "1",
    "KUBERNETES_NAMESPACE": "default",
    "PROMETHEUS_URL": "http://prometheus:9090",
    "LOG_LEVEL": "INFO"
  }
}
```

---

### Using Alternative Python Package Managers

#### With pip instead of uv

```json
{
  "command": "python",
  "args": [
    "/path/to/lumino-mcp-server/main.py"
  ]
}
```

**Note**: Ensure you've activated the virtual environment first:

```bash
cd /path/to/lumino-mcp-server
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e .
```

#### With poetry

```json
{
  "command": "poetry",
  "args": [
    "run",
    "python",
    "main.py"
  ],
  "cwd": "/path/to/lumino-mcp-server"
}
```

---

### Testing Your Configuration

After configuring any client, test the connection:

1. **Check if tools are loaded**:
   - Claude Desktop: Look for 🔨 hammer icon
   - Claude Code CLI: `claude mcp list`
   - Gemini CLI: `gemini mcp list`
   - Cursor: Check AI chat for available tools

2. **Test a simple query**:

```text
"List all namespaces in my Kubernetes cluster"
```

3. **Check server logs** (if issues):

```bash
# Run server manually to see errors
cd /path/to/lumino-mcp-server
uv run python main.py
```

Expected output:
```text
MCP Server running in stdio mode
Available tools: 39
Waiting for requests...
```

---

### Advanced Configuration

#### Multiple Clusters

Configure multiple LUMINO instances for different clusters:

```json
{
  "mcpServers": {
    "lumino-prod": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/lumino-mcp-server", "python", "main.py"],
      "env": {
        "KUBECONFIG": "/path/to/prod-kubeconfig.yaml"
      }
    },
    "lumino-dev": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/lumino-mcp-server", "python", "main.py"],
      "env": {
        "KUBECONFIG": "/path/to/dev-kubeconfig.yaml"
      }
    }
  }
}
```

#### Custom Log Level

```json
{
  "env": {
    "LOG_LEVEL": "DEBUG",
    "MCP_SERVER_LOG_LEVEL": "DEBUG"
  }
}
```

---

### Supported Transports

The server automatically detects the appropriate transport:

- **stdio** - For local desktop integrations (Claude Desktop, Claude Code CLI, Gemini CLI, Cursor)
- **streamable-http** - For Kubernetes deployments (when `KUBERNETES_NAMESPACE` is set)

## Performance Considerations

### Optimizing for Large Clusters

LUMINO is designed to handle clusters of any size efficiently:

| Cluster Size | Recommendation | Tool Strategy |
|--------------|----------------|---------------|
| **Small** (< 50 pods) | Use default settings | All tools work optimally |
| **Medium** (50-500 pods) | Use namespace filtering | Leverage adaptive tools with auto-sampling |
| **Large** (500+ pods) | Specify time windows and namespaces | Use conservative and streaming tools |
| **Very Large** (1000+ pods) | Combine filters and pagination | Progressive analysis with targeted queries |

### Token Budget Management

LUMINO automatically manages AI context limits:

- **Adaptive Sampling** - Smart tools auto-sample data when volumes are high
- **Progressive Loading** - Stream analysis processes data in chunks
- **Token Budgets** - Configurable limits prevent context overflow
- **Hybrid Strategies** - Automatically selects best analysis approach

### Query Optimization Tips

**Use Namespace Filtering**
```text
✅ "Analyze logs for pods in namespace production"
❌ "Analyze all pod logs in the cluster"
```

**Specify Time Windows**
```text
✅ "Show events from the last 2 hours"
❌ "Show all events" (might return thousands)
```

**Leverage Smart Tools**
```text
✅ "smart_summarize_pod_logs" - Adaptive analysis
❌ Direct log dumps - No processing
```

**Use Progressive Analysis**
```text
✅ Start with "overview" → drill down to "detailed"
❌ Jump directly to "deep_dive" on large datasets
```

### Performance Metrics

| Operation | Typical Response Time | Scalability |
|-----------|----------------------|-------------|
| List namespaces | < 1s | O(1) |
| Get pod logs (1 pod) | 1-3s | O(log size) |
| Analyze pipeline run | 2-5s | O(task count) |
| Cluster-wide search | 5-15s | O(namespace count) |
| ML anomaly detection | 3-10s | O(data points) |
| Topology mapping | 5-20s | O(resource count) |

### Caching Strategy

LUMINO uses intelligent caching for frequently accessed data:

- **15-minute cache** - For web-fetched content
- **Session cache** - For hybrid log analysis
- **ML model persistence** - Predictive models and training data stored locally in `~/.lumino/` (see [ML Model Persistence](#ml-model-persistence))

### Concurrent Requests

The server handles multiple concurrent requests efficiently:

- **Thread-safe operations** - Safe parallel tool execution
- **Connection pooling** - Reuses Kubernetes API connections
- **Async HTTP** - Non-blocking Prometheus queries

### Resource Usage

**Server Resource Requirements**

| Deployment | CPU | Memory | Disk |
|------------|-----|--------|------|
| Local (stdio) | 100-500m | 256-512Mi | Minimal |
| Kubernetes | 200m-1 | 512Mi-1Gi | Minimal |
| High-load | 1-2 | 1-2Gi | Minimal |

**Note**: LUMINO requires minimal resources. ML models and training data are persisted locally in `~/.lumino/` (see [ML Model Persistence](#ml-model-persistence)). Most processing happens in the AI assistant.

## Troubleshooting

### Common Issues

**No Kubernetes cluster found**
```text
Error: Unable to load kubeconfig
```
Ensure you have a valid kubeconfig at `~/.kube/config` or are running inside a cluster.

**Permission denied for resources**
```text
Error: Forbidden - User cannot list resource
```
Check your RBAC permissions. The server needs read access to the resources you want to query.

**Tool timeout**
For large clusters, some tools may timeout. Use filtering options (namespace, labels) to reduce scope.

## Dependencies

- `mcp[cli]>=1.10.1` - Model Context Protocol SDK
- `kubernetes>=32.0.1` - Kubernetes Python client
- `pandas>=2.0.0` - Data analysis
- `scikit-learn>=1.6.1` - ML algorithms
- `prometheus-client>=0.22.0` - Prometheus integration
- `aiohttp>=3.12.2` - Async HTTP client

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) before submitting pull requests.

## Security

For security vulnerabilities, please see our [Security Policy](SECURITY.md).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [FastMCP](https://github.com/jlowin/fastmcp) framework
- Inspired by the needs of SRE teams managing complex Kubernetes environments
