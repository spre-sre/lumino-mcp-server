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
        "critical_event_limit": 20,
    },
    "severity_keywords": {
        "CRITICAL": [
            "oom",
            "killed",
            "crash",
            "panic",
            "fatal",
            "critical",
            "emergency",
            "disaster",
            "outage",
            "down",
            "unavailable",
        ],
        "HIGH": [
            "error",
            "failed",
            "failure",
            "exception",
            "timeout",
            "unreachable",
            "denied",
            "refused",
            "invalid",
        ],
        "MEDIUM": [
            "warning",
            "warn",
            "retry",
            "slow",
            "degraded",
            "pending",
            "waiting",
            "delayed",
        ],
        "LOW": [
            "info",
            "created",
            "started",
            "completed",
            "successful",
            "ready",
            "healthy",
            "normal",
        ],
    },
    "category_keywords": {
        # Order matters: more specific categories should come first
        "FAILURE": [
            "failed",
            "failure",
            "error",
            "crash",
            "panic",
            "exception",
            "abort",
            "terminated",
            "killed",
            "died",
            "backoff",
        ],
        "IMAGE": [
            # IMAGE before SECURITY to avoid false positives with pod names like "image-rbac-proxy"
            "imagepull",
            "pullimage",
            "errimagepull",
            "imagepullbackoff",
            "pull image",
            "pulling image",
            "image pull",
            "registry",
        ],
        "STORAGE": [
            "volume",
            "disk",
            "storage",
            "mount",
            "pvc",
            "pv",
            "filesystem",
            "unmount",
            "failedmount",
            "failedattach",
        ],
        "NETWORKING": [
            "network",
            "dns",
            "connection",
            "unreachable",
            "endpoint",
            "route",
            "ingress",
            "addedinterface",
        ],
        "RESOURCE": [
            "memory",
            "cpu",
            "oom",
            "oomkilled",
            "quota exceeded",
            "resource quota",
            "limitrange",
            "evicted",
        ],
        "SCHEDULING": [
            "scheduled",
            "unschedulable",
            "failedscheduling",
            "preempted",
            "affinity",
            "taint",
            "toleration",
            "nodeaffinity",
        ],
        "CONFIGURATION": [
            "configmap",
            "secret",
            "createcontainerconfigerror",
            "invalidargument",
            "envvar",
        ],
        "SECURITY": [
            "forbidden",
            "unauthorized",
            "accessdenied",
            "permission denied",
            "securitycontext",
            "podsecurity",
            "scc violation",
        ],
        "SCALING": [
            "scaled",
            "scaling",
            "replicas",
            "horizontalpodautoscaler",
            "hpa",
            "scaleup",
            "scaledown",
        ],
        "LIFECYCLE": [
            "created",
            "started",
            "stopped",
            "deleted",
            "killing",
            "prestop",
            "poststart",
            "liveness",
            "readiness",
        ],
        "HEALTH": [
            "healthy",
            "unhealthy",
            "probe",
            "livenessprobe",
            "readinessprobe",
            "startupprobe",
            "health check",
        ],
    },
    "focus_area_mappings": {
        "errors": ["CRITICAL", "HIGH"],
        "warnings": ["MEDIUM"],
        "failures": ["FAILURE", "NETWORKING", "STORAGE", "IMAGE", "CONFIGURATION"],
        "performance": ["RESOURCE", "SCHEDULING", "SCALING"],
        "security": ["SECURITY"],
        "infrastructure": ["SCHEDULING", "STORAGE", "NETWORKING"],
        "health": ["HEALTH", "LIFECYCLE"],
    },
}

# ============================================================================
# LOG ANALYSIS CONFIGURATION
# ============================================================================

LOG_ANALYSIS_CONFIG: Dict[str, Any] = {
    "streaming": {"chunk_size": 500, "max_chunks": 10, "overlap_lines": 50},
    "summary": {"max_lines": 1000, "pattern_threshold": 3},
    "hybrid": {"streaming_threshold": 800, "summary_fallback": True},
}

# ============================================================================
# PIPELINE ANALYSIS CONFIGURATION
# ============================================================================

PIPELINE_ANALYSIS_CONFIG: Dict[str, Any] = {
    "bottleneck_thresholds": {
        "duration_variance": 0.5,
        "failure_rate": 0.1,
        "resource_utilization": 0.8,
    },
    "failure_patterns": {
        "retry_threshold": 3,
        "timeout_threshold": 3600,  # 1 hour
        "memory_threshold": "1Gi",
        "cpu_threshold": "1000m",
    },
}

# ============================================================================
# SEMANTIC SEARCH CONFIGURATION
# ============================================================================

SEMANTIC_SEARCH_CONFIG = {
    "intent_keywords": {
        "troubleshooting": [
            "error",
            "issue",
            "problem",
            "failure",
            "bug",
            "broken",
            "not working",
            "failing",
            "crashed",
            "down",
        ],
        "monitoring": [
            "status",
            "health",
            "metrics",
            "performance",
            "usage",
            "monitoring",
            "observability",
            "alerts",
        ],
        "deployment": [
            "deploy",
            "deployment",
            "rollout",
            "release",
            "install",
            "update",
            "upgrade",
            "version",
        ],
        "configuration": [
            "config",
            "configure",
            "settings",
            "environment",
            "variables",
            "parameters",
            "properties",
        ],
        "security": [
            "security",
            "permissions",
            "access",
            "rbac",
            "policies",
            "authentication",
            "authorization",
        ],
    },
    "k8s_entities": [
        "pod",
        "pods",
        "deployment",
        "deployments",
        "service",
        "services",
        "configmap",
        "configmaps",
        "secret",
        "secrets",
        "namespace",
        "namespaces",
        "node",
        "nodes",
        "pv",
        "pvc",
        "ingress",
        "networkpolicy",
        "serviceaccount",
        "role",
        "rolebinding",
        "clusterrole",
        "clusterrolebinding",
    ],
    "tekton_entities": [
        "pipeline",
        "pipelines",
        "pipelinerun",
        "pipelineruns",
        "task",
        "tasks",
        "taskrun",
        "taskruns",
        "trigger",
        "triggers",
    ],
}

# ============================================================================
# KUBEARCHIVE CONFIGURATION
# ============================================================================

KUBEARCHIVE_CONFIG = {
    "endpoints": {
        "default": {
            "url": None,  # Must be set via environment variable: KUBEARCHIVE_URL
            "description": "Default Kubearchive endpoint",
        }
    },
    "authentication": {
        # Token MUST be set via environment variable: KUBEARCHIVE_TOKEN
        "token": None
    },
    "defaults": {
        "fetch_logs": True,
        "max_results_per_type": 100,
        "timeout": 30,
        "include_pipelineruns": True,
        "include_taskruns": True,
    },
    "api_structure": {
        # Expected API paths - may need adjustment based on actual Kubearchive deployment
        "list_resources": "/api/v1/{kind}",
        "get_resource": "/api/v1/namespaces/{namespace}/{kind}/{name}",
        "get_logs": "/api/v1/namespaces/{namespace}/{kind}/{name}/log",
    },
}
