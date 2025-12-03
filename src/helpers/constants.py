# ============================================================================
# CONSTANTS AND CONFIGURATIONS
# ============================================================================
#
# This file contains constants and configurations used across the MCP server
# and helper modules.
# ============================================================================

from typing import Dict, Any

# ============================================================================
# SMART EVENTS HANDLER CONFIGURATION
# ============================================================================

SMART_EVENTS_CONFIG: Dict[str, Any] = {
    "defaults": {
        "default_time_window": "2h",
        "max_events_auto": 50,
        "max_events_raw": 100,
        "token_threshold": 8000,
        "critical_event_limit": 20
    },
    "severity_keywords": {
        "CRITICAL": [
            "oom", "killed", "crash", "panic", "fatal", "critical",
            "emergency", "disaster", "outage", "down", "unavailable"
        ],
        "HIGH": [
            "error", "failed", "failure", "exception", "timeout",
            "unreachable", "denied", "refused", "invalid"
        ],
        "MEDIUM": [
            "warning", "warn", "retry", "slow", "degraded",
            "pending", "waiting", "delayed"
        ],
        "LOW": [
            "info", "created", "started", "completed", "successful",
            "ready", "healthy", "normal"
        ]
    },
    "category_keywords": {
        "FAILURE": [
            "failed", "failure", "error", "crash", "panic", "exception",
            "abort", "terminated", "killed", "died"
        ],
        "NETWORKING": [
            "network", "dns", "connection", "timeout", "unreachable",
            "endpoint", "route", "ingress", "service"
        ],
        "STORAGE": [
            "volume", "disk", "storage", "mount", "pvc", "pv",
            "filesystem", "space", "capacity"
        ],
        "SCHEDULING": [
            "schedule", "node", "resource", "capacity", "allocation",
            "affinity", "taint", "toleration"
        ],
        "SECURITY": [
            "permission", "auth", "forbidden", "unauthorized", "rbac",
            "security", "policy", "scc"
        ],
        "CONFIGURATION": [
            "config", "configmap", "secret", "env", "variable",
            "setting", "parameter"
        ],
        "RESOURCE": [
            "memory", "cpu", "oom", "limit", "request", "quota",
            "resource", "utilization"
        ],
        "IMAGE": [
            "image", "pull", "registry", "tag", "digest", "repository",
            "imagepull", "pullimage"
        ]
    },
    "focus_area_mappings": {
        "errors": ["CRITICAL", "HIGH"],
        "warnings": ["MEDIUM"],
        "failures": ["FAILURE", "NETWORKING", "STORAGE"],
        "performance": ["RESOURCE", "SCHEDULING"],
        "security": ["SECURITY", "CONFIGURATION"],
        "infrastructure": ["SCHEDULING", "STORAGE", "NETWORKING"]
    }
}

# ============================================================================
# LOG ANALYSIS CONFIGURATION
# ============================================================================

LOG_ANALYSIS_CONFIG: Dict[str, Any] = {
    "streaming": {
        "chunk_size": 500,
        "max_chunks": 10,
        "overlap_lines": 50
    },
    "summary": {
        "max_lines": 1000,
        "pattern_threshold": 3
    },
    "hybrid": {
        "streaming_threshold": 800,
        "summary_fallback": True
    }
}

# ============================================================================
# PIPELINE ANALYSIS CONFIGURATION
# ============================================================================

PIPELINE_ANALYSIS_CONFIG: Dict[str, Any] = {
    "bottleneck_thresholds": {
        "duration_variance": 0.5,
        "failure_rate": 0.1,
        "resource_utilization": 0.8
    },
    "failure_patterns": {
        "retry_threshold": 3,
        "timeout_threshold": 3600,  # 1 hour
        "memory_threshold": "1Gi",
        "cpu_threshold": "1000m"
    }
}

# ============================================================================
# SEMANTIC SEARCH CONFIGURATION
# ============================================================================

SEMANTIC_SEARCH_CONFIG = {
    "intent_keywords": {
        "troubleshooting": [
            "error", "issue", "problem", "failure", "bug", "broken",
            "not working", "failing", "crashed", "down"
        ],
        "monitoring": [
            "status", "health", "metrics", "performance", "usage",
            "monitoring", "observability", "alerts"
        ],
        "deployment": [
            "deploy", "deployment", "rollout", "release", "install",
            "update", "upgrade", "version"
        ],
        "configuration": [
            "config", "configure", "settings", "environment", "variables",
            "parameters", "properties"
        ],
        "security": [
            "security", "permissions", "access", "rbac", "policies",
            "authentication", "authorization"
        ]
    },
    "k8s_entities": [
        "pod", "pods", "deployment", "deployments", "service", "services",
        "configmap", "configmaps", "secret", "secrets", "namespace", "namespaces",
        "node", "nodes", "pv", "pvc", "ingress", "networkpolicy",
        "serviceaccount", "role", "rolebinding", "clusterrole", "clusterrolebinding"
    ],
    "tekton_entities": [
        "pipeline", "pipelines", "pipelinerun", "pipelineruns",
        "task", "tasks", "taskrun", "taskruns", "trigger", "triggers"
    ]
}
