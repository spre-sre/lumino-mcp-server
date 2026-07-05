# Changelog

All notable changes to Lumino MCP Server will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.3] - 2026-07-05

### Added
- `query_kubearchive` tool - Query archived Kubernetes resources from KubeArchive with optional log retrieval
- `manage_prediction_training_data` tool - Manage training data for the predictive log analyzer
- KubeArchive integration helper (`kubearchive_integration.py`) for archived resource access
- ML persistence helper (`ml_persistence.py`) for training data and model storage
- Full lifecycle chain following in `pipeline_tracer` (Application and Component resolution)
- Tekton/Konflux and OpenShift runbooks in `advanced_event_analytics`
- Konflux resource types in `get_kubernetes_resource`

### Fixed
- SQL injection vulnerability in `ml_persistence.py` - parameterized all SQL queries (#59)
- Prometheus bearer token now refreshed on each query to prevent stale auth (#150)
- Async lock added to `list_namespaces` cache to prevent race conditions (#52)
- `severity_threshold` now correctly detects WARN-level anomalies (#53)
- `analysis_depth` parameter now maps to progressive analysis level (#54)
- Pipeline success rate calculation, `tail_lines` handling, running task detection, and sort order (#55)
- Two critical regressions from Phase 1: missing `await` and stale `search_pattern` reference (#36)
- Critical bugs, removed unused parameters, and improved correctness across 30 items (#35)
- Risk contradiction, event `time_span`, root cause misclassification, and timezone handling (#30)
- KubeArchive 401 error when retrieving token from kubeconfig

### Changed
- Tool count updated from 37 to **39 MCP tools**
- Documentation corrected to reflect optional ML data persistence (SQLite)
- Architecture diagram updated to include all 9 helper modules

## [0.9.2] - 2026-01-24

### Added
- Published to [MCP Registry](https://registry.modelcontextprotocol.io) as `io.github.geored/lumino`
- Added PyPI badge to README
- Added `mcp-name` comment to README for registry validation

### Changed
- Registry name changed to `io.github.geored/lumino` for publishing

## [0.9.1] - 2026-01-24

### Added
- MCP Registry support with `server.json` for registry submission
- `[tool.mcp]` section in pyproject.toml for registry integration
- Published to [PyPI](https://pypi.org/project/lumino-mcp-server/) with full README documentation

### Changed
- Updated package description to better reflect SRE observability capabilities
- Added keywords: `sre`, `observability`, `pipelines`

### Fixed
- `analyze_failed_pipeline` - Handle deleted pods gracefully with fallback to TaskRun step info
- `check_cluster_certificate_health` - Fix duplicate namespace entries and respect user namespace filter
- `check_resource_constraints` - Add lowercase 'k' suffix support for count quotas (e.g., "2k" = 2000)
- `detect_log_anomalies` - Fix pattern key extraction showing "?i)" instead of category names
- `ci_cd_performance_baselining` - Filter out "unknown" task entries from Prometheus metrics

## [0.9.0] - 2026-01-18

### Added

#### Core Infrastructure
- Initial release of Lumino MCP Server with **37 MCP tools** (since expanded to 39)
- MCP (Model Context Protocol) integration for AI-powered Kubernetes operations
- Multi-cluster support with automatic context detection
- Prometheus integration for metrics queries
- Namespace caching with 1-day TTL for performance

#### Kubernetes Tools
- `list_namespaces` - List all namespaces in the cluster
- `list_pods_in_namespace` - List pods with status and placement info
- `get_kubernetes_resource` - Retrieve details about any Kubernetes resource
- `search_resources_by_labels` - Search resources across types and namespaces
- `check_resource_constraints` - Identify resource bottlenecks

#### Tekton Pipeline Tools
- `list_pipelineruns` - List PipelineRuns with status and timing
- `list_taskruns` - List TaskRuns, optionally filtered by PipelineRun
- `find_pipeline` - Find pipelines matching a pattern across namespaces
- `get_pipelinerun_logs` - Fetch logs from all pods in a PipelineRun
- `get_tekton_pipeline_runs_status` - Cluster-wide status summary
- `list_recent_pipeline_runs` - Recent PipelineRuns across all namespaces
- `analyze_failed_pipeline` - Root cause analysis for failed pipelines

#### Analysis & Diagnostics
- `analyze_logs` - Extract error patterns and insights from logs
- `detect_anomalies` - Statistical anomaly detection in PipelineRuns
- `detect_log_anomalies` - ML-powered log anomaly detection
- `smart_get_namespace_events` - Adaptive event analysis with auto-filtering
- `progressive_event_analysis` - Multi-level event correlation
- `advanced_event_analytics` - ML patterns with runbook suggestions

#### Log Analysis Tools
- `smart_summarize_pod_logs` - Adaptive pod log analysis
- `stream_analyze_pod_logs` - Streaming log analysis with pattern detection
- `analyze_pod_logs_hybrid` - Intelligent strategy selection for log analysis
- `semantic_log_search` - Natural language log search

#### Predictive & Forecasting
- `predictive_log_analyzer` - ML-based failure prediction
- `resource_bottleneck_forecaster` - Resource exhaustion forecasting
- `what_if_scenario_simulator` - Impact simulation for config changes

#### CI/CD Performance
- `ci_cd_performance_baselining_tool` - Performance baselines with statistical analysis
- `pipeline_tracer` - Trace operations through pipeline flows
- `automated_triage_rca_report_generator` - Automated root cause analysis reports

#### OpenShift Support
- `get_machine_config_pool_status` - MCP status and update monitoring
- `get_openshift_cluster_operator_status` - Cluster operator health checks
- `get_etcd_logs` - Retrieve etcd pod logs
- `check_cluster_certificate_health` - Certificate expiration scanning
- `investigate_tls_certificate_issues` - TLS issue investigation

#### Namespace Investigation
- `conservative_namespace_overview` - Quick namespace health check
- `adaptive_namespace_investigation` - Progressive namespace analysis
- `live_system_topology_mapper` - Real-time dependency graph generation

#### Prometheus Integration
- `prometheus_query` - Execute PromQL queries with automatic auth

### Performance Optimizations
- `find_pipeline` - Cluster-wide queries with API limits and optional TaskRun fetching
- `ci_cd_performance_baselining_tool` - Parallelized Prometheus queries
- `get_tekton_pipeline_runs_status` - Configurable limits to prevent timeouts on large clusters
- `pipeline_tracer` - Parallelization and namespace targeting
- `predictive_log_analyzer` - Namespace targeting and improved log collection

### Container Image
- Available at `quay.io/geored/lumino-mcp-server`
- Multi-architecture support (amd64, arm64)
