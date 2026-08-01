# AGENTS.md — Lumino MCP Server: Agentic AI Architecture

This file documents the `.agents/` system for AI clients other than Claude Code
(Cursor, Codex, OpenCode, and any future agentic runtimes).

For Claude Code-specific instructions, see `.agents/CLAUDE.md`.

---

## Overview

The `.agents/` directory co-locates all agentic AI artefacts with the lumino MCP server tools
they operate on (per ADR-006). It provides:

- **Skills** — reusable, parameterised SRE investigation patterns
- **Programs** — multi-step orchestration scripts that chain skills and tools
- **Runbooks** — machine-readable YAML failure response guides
- **Config** — safety guardrails, autonomy levels, and cluster inventory
- **Tests** — evaluation harnesses and fixtures for validating agent behaviour

---

## Directory Layout

```
.agents/
├── CLAUDE.md           # Claude Code system instructions (skill triggers, tool index, safety rules)
├── AGENTS.md           # This file — architecture overview for all other clients
│
├── skills/             # Reusable investigation and remediation skills (Markdown)
│   └── README.md
│
├── programs/           # Multi-step agentic programs that orchestrate skills + tools
│   └── README.md
│
├── runbooks/           # Machine-readable YAML runbooks for known failure patterns
│   └── README.md
│
├── config/
│   ├── safety-guardrails.yaml   # Hard boundaries on autonomous actions
│   ├── autonomy-levels.yaml     # What the agent may do vs. must escalate to a human
│   └── clusters.yaml            # Cluster inventory and access context
│
└── tests/
    ├── fixtures/        # Static mock data (API responses, logs) for offline evaluation
    └── evaluation/      # Scoring harnesses for measuring agent diagnostic accuracy
```

---

## MCP Tool Catalog

All tools are exposed by the lumino MCP server (`src/server-mcp.py`) and are **strictly read-only**.
No tool performs writes, deletes, or mutations on any cluster resource.

### Categories

| Category | Tools |
|----------|-------|
| Kubernetes Core | `list_namespaces`, `list_pods_in_namespace`, `get_kubernetes_resource`, `search_resources_by_labels`, `query_kubearchive` |
| Tekton Pipelines | `list_pipelineruns`, `list_taskruns`, `get_pipelinerun_logs`, `list_recent_pipeline_runs`, `find_pipeline`, `get_tekton_pipeline_runs_status` |
| Log Analysis | `analyze_logs`, `smart_summarize_pod_logs`, `stream_analyze_pod_logs`, `analyze_pod_logs_hybrid`, `detect_log_anomalies`, `semantic_log_search` |
| Event Analysis | `smart_get_namespace_events`, `progressive_event_analysis`, `advanced_event_analytics` |
| Failure Analysis & RCA | `analyze_failed_pipeline`, `automated_triage_rca_report_generator` |
| Resource Monitoring | `check_resource_constraints`, `detect_anomalies`, `prometheus_query`, `resource_bottleneck_forecaster` |
| Namespace Investigation | `conservative_namespace_overview`, `adaptive_namespace_investigation` |
| Certificate & Security | `investigate_tls_certificate_issues`, `check_cluster_certificate_health` |
| OpenShift Specific | `get_machine_config_pool_status`, `get_openshift_cluster_operator_status`, `get_etcd_logs` |
| CI/CD Performance | `ci_cd_performance_baselining_tool`, `pipeline_tracer` |
| Topology & Prediction | `live_system_topology_mapper`, `predictive_log_analyzer`, `manage_prediction_training_data` |
| Simulation | `what_if_scenario_simulator` |

Full tool parameter documentation is in `README.md` at the repository root.

---

## Skills

Skills live in `.agents/skills/` as Markdown files. Each skill file defines:

- **Trigger phrases** — natural language patterns that indicate when to apply this skill
- **Inputs** — parameters the invoking agent must supply (namespace, pod name, time range, etc.)
- **Steps** — ordered sequence of MCP tool calls with parameter templates
- **Output** — what the skill produces (findings summary, hypothesis list, etc.)

**Discovery:** list `.agents/skills/` and read the file whose trigger phrases match the user request.

---

## Runbooks

Runbooks live in `.agents/runbooks/` as YAML files. Schema:

```yaml
name: <runbook-name>
description: <one-line description>
trigger_patterns:
  - <log or event string that indicates this failure>
diagnostic_steps:
  - step: <human-readable step name>
    tool: <mcp_tool_name>
    params:
      <param>: <value or template variable>
    interpret: <what to look for in the output>
remediation:
  - <human-actionable step — never auto-applied by the agent>
references:
  - <doc or runbook URL>
```

**Execution:** read the runbook YAML, execute each `diagnostic_steps` entry using the named
MCP tool with the given params, interpret the output per the `interpret` field, then present
the `remediation` steps to the human operator for approval.

---

## Safety Contract

All agents operating in this system must honour:

1. **Read-only** — no writes, deletes, or mutations to cluster state.
2. **Hypothesis framing** — findings are presented as hypotheses, not facts.
3. **Human approval for remediation** — no autonomous remediation without explicit operator sign-off.
4. **Guardrail compliance** — check `.agents/config/safety-guardrails.yaml` and
   `.agents/config/autonomy-levels.yaml` before any action that could affect production.

---

## Supported Clients

| Client | Entry point |
|--------|------------|
| Claude Code | `.agents/CLAUDE.md` (auto-discovered) |
| Cursor | `.agents/AGENTS.md` (add to context manually or via `.cursorrules`) |
| Codex / OpenAI Agents | `.agents/AGENTS.md` (pass as system context) |
| OpenCode | `.agents/AGENTS.md` (reference in project config) |
