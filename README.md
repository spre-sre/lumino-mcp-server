# LUMINO MCP Server

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![MCP](https://img.shields.io/badge/MCP-1.10%2B-green.svg)](https://modelcontextprotocol.io/)

An open source MCP (Model Context Protocol) server providing AI-powered tools for Kubernetes, OpenShift, and Tekton monitoring, analysis, and troubleshooting.

## Overview

LUMINO MCP Server transforms how Site Reliability Engineers (SREs) and DevOps teams interact with Kubernetes clusters. By exposing 37 specialized tools through the Model Context Protocol, it enables AI assistants to:

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

## Requirements

- Python 3.10+
- Access to a Kubernetes/OpenShift cluster (for Kubernetes tools)
- [uv](https://docs.astral.sh/uv/) for dependency management (recommended)

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

## Configuration

### Kubernetes Authentication

The server automatically detects Kubernetes configuration:

1. **In-cluster config** - When running inside a Kubernetes pod
2. **Local kubeconfig** - When running locally (uses `~/.kube/config`)

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KUBERNETES_NAMESPACE` | Namespace for K8s mode | - |
| `K8S_NAMESPACE` | Alternative namespace variable | - |
| `PROMETHEUS_URL` | Prometheus server URL for metrics | Auto-detected |

## Available Tools

### Kubernetes Core (4 tools)

| Tool | Description |
|------|-------------|
| `list_namespaces` | List all namespaces in the cluster |
| `list_pods_in_namespace` | List pods with status and placement info |
| `get_kubernetes_resource` | Get any Kubernetes resource with flexible output |
| `search_resources_by_labels` | Search resources across namespaces by labels |

### Tekton Pipelines (6 tools)

| Tool | Description |
|------|-------------|
| `list_pipelineruns` | List PipelineRuns with status and timing |
| `list_taskruns` | List TaskRuns, optionally filtered by pipeline |
| `get_pipelinerun_logs` | Retrieve pipeline logs with optional cleaning |
| `list_recent_pipeline_runs` | Recent pipelines across all namespaces |
| `find_pipeline` | Find pipelines by pattern matching |
| `get_tekton_pipeline_runs_status` | Cluster-wide pipeline status summary |

### Log Analysis (6 tools)

| Tool | Description |
|------|-------------|
| `analyze_logs` | Extract error patterns from log text |
| `smart_summarize_pod_logs` | Intelligent log summarization |
| `stream_analyze_pod_logs` | Streaming analysis for large logs |
| `analyze_pod_logs_hybrid` | Combined analysis strategies |
| `detect_log_anomalies` | Anomaly detection with severity levels |
| `semantic_log_search` | NLP-based semantic log search |

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

### Certificate & Security (3 tools)

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
| `cross_cluster_pipeline_tracer` | Trace pipelines across clusters |

### Topology & Prediction (2 tools)

| Tool | Description |
|------|-------------|
| `live_system_topology_mapper` | Real-time system topology mapping |
| `predictive_log_analyzer` | Predict issues from log patterns |

### Simulation (1 tool)

| Tool | Description |
|------|-------------|
| `what_if_scenario_simulator` | Simulate configuration changes |

## Architecture

```
lumino-mcp-server/
├── main.py                 # Entry point with transport detection
├── src/
│   ├── server-mcp.py       # MCP server with all 37 tools
│   └── helpers/
│       ├── constants.py    # Shared constants
│       ├── event_analysis.py    # Event processing logic
│       ├── failure_analysis.py  # RCA algorithms
│       ├── log_analysis.py      # Log processing
│       ├── resource_topology.py # Topology mapping
│       ├── semantic_search.py   # NLP search
│       └── utils.py             # Utility functions
└── pyproject.toml          # Project configuration
```

## MCP Client Integration

### Claude Desktop

Add to your Claude Desktop configuration (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "lumino": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/lumino-mcp-server", "python", "main.py"]
    }
  }
}
```

### Other MCP Clients

The server supports standard MCP transports:
- **stdio** - For local desktop integrations
- **streamable-http** - For Kubernetes deployments

## Troubleshooting

### Common Issues

**No Kubernetes cluster found**
```
Error: Unable to load kubeconfig
```
Ensure you have a valid kubeconfig at `~/.kube/config` or are running inside a cluster.

**Permission denied for resources**
```
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
