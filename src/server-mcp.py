"""
LUMINO MCP Server - FastMCP Server Module

This module provides the core MCP (Model Context Protocol) server implementation
for Kubernetes, OpenShift, and Tekton monitoring and analysis.
"""

import re
import os
import json
import yaml
import time
import base64
import asyncio
import logging
import requests
import aiohttp
import tempfile
import shutil
from datetime import datetime
from typing import Dict, List, Optional, Any, Union, Callable
from mcp.server.fastmcp import FastMCP
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from collections import defaultdict

# For metrics and analysis
import pandas as pd
import numpy as np
import networkx as nx
from sklearn.ensemble import IsolationForest

from prometheus_client.parser import text_string_to_metric_families

# Helper imports
from helpers import (
    calculate_duration,
    calculate_duration_seconds,
    parse_time_period,
    parse_time_parameters,
    format_yaml_output,
    format_detailed_output,
    format_summary_output,
    calculate_context_tokens,
    get_all_pod_logs,
    clean_pipeline_logs,
    calculate_utilization,
    list_pods,
    detect_anomalies_in_data,
    SMART_EVENTS_CONFIG,
    LOG_ANALYSIS_CONFIG,
    EventSeverity,
    EventCategory,
    ProgressiveEventAnalyzer,
    MLPatternDetector,
    LogMetricsIntegrator,
    RunbookSuggestionEngine,
    assess_overall_risk,
    generate_strategic_recommendations,
    generate_comprehensive_insights,
    smart_sample_string_events,
    generate_string_events_summary,
    generate_string_events_insights,
    generate_string_events_recommendations,
    # Log analysis helpers
    extract_error_patterns,
    categorize_errors,
    generate_log_summary,
    # Advanced log analysis helpers
    extract_log_patterns,
    sample_logs_by_time,
    generate_focused_summary,
    LogStreamProcessor,
    generate_streaming_summary,
    analyze_trending_patterns,
    generate_streaming_recommendations,
    combine_analysis_results,
    generate_supplementary_insights,
    generate_hybrid_recommendations,
    LogAnalysisStrategy,
    LogAnalysisContext,
    StrategySelector,
    get_strategy_selection_reason,
    analysis_cache,
    # ML/Data processing helpers for predictive analysis
    preprocess_log_data,
    extract_log_features,
    train_anomaly_model,
    train_or_load_model,
    analyze_log_patterns_for_failure_prediction,
    generate_failure_predictions,
    # Token limit truncation helpers
    truncate_to_token_limit,
    truncate_streaming_results,
    # Pipeline analysis helpers
    determine_root_cause,
    recommend_actions,
    get_pipeline_details,
    get_task_details,
    # Resource search helpers
    build_advanced_label_selector,
    get_resource_api_info,
    extract_resource_info,
    analyze_labels,
    calculate_namespace_distribution,
    sort_resources,
    # Certificate parsing helpers
    parse_certificate,
    categorize_certificate_status,
    # Performance analysis helpers
    detect_performance_trend,
    # Failure analysis helpers
    identify_failure_context,
    analyze_pipeline_failure,
    analyze_pod_failure,
    analyze_generic_failure,
    build_failure_timeline,
    find_related_failures,
    perform_advanced_rca,
    analyze_resource_constraints,
    analyze_configuration_issues,
    analyze_pipeline_dependencies,
    analyze_pipeline_performance,
    generate_remediation_plan,
    calculate_confidence_score,
    assess_failure_severity,
    # Resource topology helpers
    get_multi_cluster_clients,
    correlate_pipeline_events,
    follow_lifecycle_chain,
    track_artifacts,
    analyze_bottlenecks,
    # Machine config pool helpers
    analyze_machine_config_pool_status,
    detect_pool_issues,
    generate_update_recommendations,
    # Operator analysis helpers
    analyze_operator_dependencies,
    identify_critical_issues,
    analyze_operator_conditions,
    # Topology mapping helpers
    get_multi_cluster_topology_clients,
    generate_node_id,
    calculate_dependency_weight,
    get_resource_metrics,
    analyze_owner_references,
    analyze_service_dependencies,
    analyze_volume_dependencies,
    handle_resource_fetch_error,
    convert_to_graphviz,
    convert_to_mermaid,
    # Resource forecasting helpers
    calculate_forecast_intervals,
    simple_linear_forecast,
    # Semantic search helpers
    interpret_semantic_query,
    determine_search_strategy,
    extract_k8s_entities,
    find_semantic_matches,
    calculate_semantic_relevance,
    identify_match_reasons,
    extract_log_metadata,
    rank_results_by_semantic_relevance,
    identify_common_patterns,
    analyze_severity_distribution,
    generate_semantic_suggestions,
    _build_log_params,
    _get_target_namespaces,
    _search_pod_logs_semantically,
    _search_events_semantically,
    _search_tekton_resources_semantically,
    # Simulation helpers
    convert_duration_to_seconds,
    calibrate_simulation_models,
    run_monte_carlo_simulation,
    collect_baseline_system_data,
    build_system_behavior_models,
    load_historical_performance_data,
    # Simulation impact analysis
    analyze_system_impact,
    perform_risk_assessment,
    calculate_simulation_quality,
    generate_simulation_recommendations,
    # Simulation affected components
    identify_affected_components,
)

# Configure logging with custom format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("lumino-mcp")


# Suppress the default MCP server logging to replace with our enhanced version
mcp_server_logger = logging.getLogger("mcp.server.lowlevel.server")
mcp_server_logger.setLevel(logging.WARNING)  # Only show warnings and errors

# Initialize FastMCP server with streaming support
mcp = FastMCP(name="lumino-mcp-server", stateless_http=False)

# Health check functionality will be handled by the MCP server itself
# The FastMCP framework provides its own health endpoints

from helpers.kubearchive_integration import (
    KubeArchiveEndpointDiscovery,
    KubeArchiveClient,
    check_kubearchive_availability,
    query_kubearchive_resources,
    normalize_to_rfc3339,
    setup_kubearchive_client
)

# Create a decorator to add tool execution logging
def log_tool_execution(func):
    """Decorator to log tool execution with tool name."""
    import functools

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        tool_name = func.__name__
        logger.info(f"Executing tool: {tool_name}")
        try:
            result = await func(*args, **kwargs)
            logger.info(f"Tool completed: {tool_name}")
            return result
        except Exception as e:
            logger.error(f"Tool failed: {tool_name} - Error: {str(e)}")
            raise

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        tool_name = func.__name__
        logger.info(f"Executing tool: {tool_name}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Tool completed: {tool_name}")
            return result
        except Exception as e:
            logger.error(f"Tool failed: {tool_name} - Error: {str(e)}")
            raise

    # Return appropriate wrapper based on whether function is async
    import asyncio
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper


# Override the mcp.tool decorator to include our logging
original_tool_decorator = mcp.tool


def enhanced_tool_decorator(*args, **kwargs):
    """Enhanced tool decorator that adds logging."""
    def decorator(func):
        # First apply our logging decorator
        logged_func = log_tool_execution(func)
        # Then apply the original MCP tool decorator
        return original_tool_decorator(*args, **kwargs)(logged_func)

    # Handle both @mcp.tool and @mcp.tool() usage
    if len(args) == 1 and callable(args[0]) and not kwargs:
        # Direct decoration: @mcp.tool
        func = args[0]
        logged_func = log_tool_execution(func)
        return original_tool_decorator(logged_func)
    else:
        # Parameterized decoration: @mcp.tool()
        return decorator


# Replace the tool decorator
mcp.tool = enhanced_tool_decorator


# Configure Kubernetes client
try:
    config.load_incluster_config()
    logger.info("Loaded Kubernetes configuration from cluster")
except config.ConfigException:
    try:
        config.load_kube_config()
        logger.info("Loaded Kubernetes configuration from local kubeconfig")
    except config.ConfigException:
        logger.warning("No Kubernetes configuration found. Some tools may not work.")

# Initialize Kubernetes API clients
try:
    k8s_core_api = client.CoreV1Api()
    k8s_apps_api = client.AppsV1Api()
    k8s_custom_api = client.CustomObjectsApi()
    k8s_batch_api = client.BatchV1Api()
    k8s_storage_api = client.StorageV1Api()
    k8s_autoscaling_api = client.AutoscalingV2Api()
except Exception as e:
    logger.warning(f"Failed to initialize Kubernetes API clients: {e}")
    k8s_core_api = None
    k8s_apps_api = None
    k8s_custom_api = None
    k8s_batch_api = None
    k8s_storage_api = None
    k8s_autoscaling_api = None

# Initialize NetworkingV1Api for Ingress support (for KubeArchive discovery on plain Kubernetes)
try:
    k8s_networking_api = client.NetworkingV1Api()
except Exception as e:
    logger.warning(f"Failed to initialize NetworkingV1Api: {e}")
    k8s_networking_api = None

if k8s_core_api is not None and k8s_custom_api is not None:
    kubearchive_endpoint_discovery = KubeArchiveEndpointDiscovery(
        k8s_core_api=k8s_core_api,
        k8s_custom_api=k8s_custom_api,
        k8s_networking_api=k8s_networking_api,
        auto_port_forward=True,
    )
else:
    kubearchive_endpoint_discovery = None


# Prometheus endpoints configuration (local Tekton components)
PROMETHEUS_ENDPOINTS = {
    'tekton-operator': 'http://localhost:9092/metrics',
    'tekton-chains-metrics': 'http://localhost:9093/metrics',
    'tekton-events-controller': 'http://localhost:9094/metrics',
    'tekton-pipelines-controller': 'http://localhost:9097/metrics',
    'tekton-pipelines-remote-resolvers': 'http://localhost:9100/metrics',
    'tekton-pipelines-webhook': 'http://localhost:9103/metrics',
    'tekton-results-api-service': 'http://localhost:9108/metrics',
    'tekton-results-watcher': 'http://localhost:9110/metrics'
}

# OpenShift cluster Prometheus endpoints (for mcp__lumino__prometheus_query)
OPENSHIFT_PROMETHEUS_ENDPOINTS = {
    # Add known cluster endpoints here as fallback
    # Format: "cluster-name": {"url": "https://prometheus-endpoint-url"}
}


# ============================================================================
# PROMETHEUS ENDPOINT DISCOVERY HELPERS
# ============================================================================

class PrometheusEndpointCache:
    """Cache for discovered Prometheus/Thanos endpoints with TTL."""

    def __init__(self, ttl_seconds: int = 300):  # 5 minute default cache
        self._cache: Dict[str, tuple] = {}  # key -> (endpoint, endpoint_type, timestamp)
        self._ttl = ttl_seconds

    def get(self, cluster_key: str = "default") -> Optional[tuple]:
        """Get cached endpoint if valid. Returns (url, endpoint_type) or None."""
        if cluster_key in self._cache:
            endpoint, endpoint_type, timestamp = self._cache[cluster_key]
            if time.time() - timestamp < self._ttl:
                logger.debug(f"Cache hit for {endpoint_type} endpoint: {endpoint}")
                return (endpoint, endpoint_type)
            else:
                del self._cache[cluster_key]
        return None

    def set(self, endpoint: str, cluster_key: str = "default", endpoint_type: str = "prometheus") -> None:
        """Cache endpoint with its type."""
        self._cache[cluster_key] = (endpoint, endpoint_type, time.time())
        logger.debug(f"Cached {endpoint_type} endpoint: {endpoint}")

    def invalidate(self, cluster_key: str = "default") -> None:
        """Invalidate cache entry."""
        if cluster_key in self._cache:
            del self._cache[cluster_key]


# Global cache instance for Prometheus endpoints
_prometheus_endpoint_cache = PrometheusEndpointCache()

# Namespace cache for avoiding repeated API calls
_namespace_cache = {"namespaces": None, "timestamp": 0}
_namespace_cache_lock = asyncio.Lock()
_NAMESPACE_CACHE_TTL = 86400  # 1 day in seconds


# ============================================================================
# SAFE REGEX COMPILATION (ReDoS mitigation)
# ============================================================================

# Maximum allowed length for user-supplied regex patterns (namespace filters, etc.)
_MAX_REGEX_PATTERN_LEN = 200

# Detects nested quantifiers that cause catastrophic backtracking (ReDoS).
# Catches patterns like (a+)+, (a*)+, (a+)*, ([^x]+)+, (?:a+)+, etc.
# Also catches overlapping-alternation quantifiers like (a|aa)+, (x|xx|xxx)+.
# Excludes bounded quantifiers like (\d{3})+ which cannot cause catastrophic
# backtracking because {n} and {n,m} with small m are fixed-width.
_NESTED_QUANTIFIER_RE = re.compile(
    r"\([^)]*[+*]\s*\)\s*[+*{]"  # (x+)+ or (x*){2,} -- inner unbounded
    r"|\([^)]*\{\d+,\}\s*\)\s*[+*{]"  # (x{2,})+ -- inner open-ended {n,}
    r"|\)\s*[+*{]\s*\)\s*[+*{]"  # nested groups: )+ )+
    r"|\([^)]*\|[^)]*\)\s*[+*{]"  # overlapping alternation: (a|aa)+
)


def _safe_compile_namespace_filter(pattern: str) -> re.Pattern:
    """Compile a namespace filter regex with ReDoS protections.

    Validates the pattern length and checks for nested quantifiers
    that cause catastrophic backtracking before compiling.

    Args:
        pattern: User-supplied regex pattern string.

    Returns:
        Compiled regex pattern.

    Raises:
        ValueError: If the pattern is too long or contains dangerous constructs.
        re.error: If the pattern is not valid regex syntax.
    """
    if len(pattern) > _MAX_REGEX_PATTERN_LEN:
        raise ValueError(
            f"Namespace filter pattern too long "
            f"({len(pattern)} chars, max {_MAX_REGEX_PATTERN_LEN})"
        )

    if _NESTED_QUANTIFIER_RE.search(pattern):
        raise ValueError(
            "Namespace filter contains nested quantifiers "
            "that may cause catastrophic backtracking (ReDoS)"
        )

    return re.compile(pattern)


def _is_running_in_cluster() -> bool:
    """Check if we're running inside a Kubernetes cluster."""
    return os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token")


# ============================================================================
# ADAPTIVE LOG PROCESSING HELPERS
# ============================================================================

class AdaptiveLogProcessor:
    """Helper class for adaptive log processing with token management."""

    def __init__(self, max_token_budget: int = 150000):
        self.max_token_budget = max_token_budget
        self.safety_buffer = 0.8  # Use 80% of budget for safety
        self.effective_budget = int(max_token_budget * self.safety_buffer)
        self.used_tokens = 0

    def can_process_more(self, estimated_tokens: int) -> bool:
        """Check if we can process more data within token budget."""
        return (self.used_tokens + estimated_tokens) <= self.effective_budget

    def record_usage(self, actual_tokens: int):
        """Record actual token usage."""
        self.used_tokens += actual_tokens

    def get_remaining_budget(self) -> int:
        """Get remaining token budget."""
        return max(0, self.effective_budget - self.used_tokens)

    def get_usage_percentage(self) -> float:
        """Get current token usage as percentage."""
        return (self.used_tokens / self.effective_budget) * 100


async def _estimate_pod_log_tokens(namespace: str, pod_name: str, tail_lines: int = 500, sample_ratio: float = 0.1) -> int:
    """
    Estimate token usage for a pod's logs using representative sampling.

    Args:
        namespace: Kubernetes namespace
        pod_name: Pod name to estimate
        tail_lines: The actual tail_lines that will be used for fetching
        sample_ratio: Fraction of tail_lines to sample (default: 10%)

    Returns:
        Estimated token count for the pod's logs (extrapolated from sample)
    """
    try:
        # Sample a fraction of the logs to estimate token density
        sample_lines = max(50, int(tail_lines * sample_ratio))

        sample = await get_all_pod_logs(
            pod_name=pod_name,
            namespace=namespace,
            k8s_core_api=k8s_core_api,
            tail_lines=sample_lines
        )

        if sample:
            sample_text = ""
            for container_logs in sample.values():
                if isinstance(container_logs, str):
                    sample_text += container_logs

            sample_tokens = calculate_context_tokens(sample_text)

            # Extrapolate to full tail_lines with capped multiplier to avoid over-estimation
            # Cap at 3x to handle cases where sample has unusually high token density
            raw_factor = tail_lines / sample_lines
            extrapolation_factor = min(raw_factor * 1.1, 3.0)  # Cap at 3x, use 1.1x safety margin
            estimated_tokens = int(sample_tokens * extrapolation_factor)

            logger.debug(f"Token estimate for {pod_name}: ~{estimated_tokens} tokens (sampled {sample_lines} lines, factor {extrapolation_factor:.2f}x)")
            return estimated_tokens

    except Exception as e:
        logger.debug(f"Token estimation failed for {pod_name}: {e}")

    # Conservative default: assume ~30 tokens per line
    return tail_lines * 30


async def _prioritize_pipeline_pods(pod_names: List[str], namespace: str) -> List[str]:
    """
    Prioritize pods for processing - failed pods first, recent pods next.

    Args:
        pod_names: List of pod names to prioritize
        namespace: Kubernetes namespace

    Returns:
        List of pod names in priority order
    """
    try:
        pod_priorities = []

        for pod_name in pod_names:
            try:
                pod = k8s_core_api.read_namespaced_pod(name=pod_name, namespace=namespace)

                priority_score = 0

                # Failed pods get highest priority
                if pod.status.phase in ['Failed', 'Error']:
                    priority_score += 1000

                # Recent pods get higher priority
                if pod.metadata.creation_timestamp:
                    age_hours = (datetime.now(pod.metadata.creation_timestamp.tzinfo) - pod.metadata.creation_timestamp).total_seconds() / 3600
                    priority_score += max(0, 100 - age_hours)

                # Pods with restart counts (indicating issues) get priority
                if pod.status.container_statuses:
                    for container_status in pod.status.container_statuses:
                        if container_status.restart_count and container_status.restart_count > 0:
                            priority_score += 50 + container_status.restart_count * 10

                pod_priorities.append((pod_name, priority_score))

            except Exception as e:
                logger.debug(f"Could not get details for pod {pod_name}: {e}")
                pod_priorities.append((pod_name, 1))

        # Sort by priority (highest first) and return pod names
        pod_priorities.sort(key=lambda x: x[1], reverse=True)
        prioritized_names = [pod_name for pod_name, _ in pod_priorities]

        logger.info(f"Pod prioritization: {prioritized_names[:3]}... (showing top 3)")
        return prioritized_names

    except Exception as e:
        logger.warning(f"Pod prioritization failed: {e}")
        return pod_names  # Return original order as fallback


def _calculate_adaptive_tail_lines(total_pods: int, processed_pods: int, remaining_budget: int) -> int:
    """
    Calculate adaptive tail_lines based on pipeline size and remaining token budget.

    Args:
        total_pods: Total number of pods in pipeline
        processed_pods: Number of pods already processed
        remaining_budget: Remaining token budget

    Returns:
        Optimal tail_lines for current pod
    """
    remaining_pods = total_pods - processed_pods
    tokens_per_pod = remaining_budget // max(remaining_pods, 1)

    # Convert tokens to approximate lines (assuming ~25 tokens per line)
    estimated_lines = tokens_per_pod // 25

    # Apply pipeline size strategy
    if total_pods <= 5:  # Small pipeline
        base_lines = min(2000, estimated_lines)
    elif total_pods <= 15:  # Medium pipeline
        base_lines = min(1000, estimated_lines)
    else:  # Large pipeline
        base_lines = min(500, estimated_lines)

    # Ensure minimum viable lines
    adaptive_lines = max(100, base_lines)

    logger.debug(f"Adaptive tail_lines: {adaptive_lines} (budget: {remaining_budget}, pods left: {remaining_pods})")
    return adaptive_lines


def _truncate_logs_to_token_limit(logs: str, max_tokens: int, pod_name: str) -> tuple[str, bool]:
    """
    Truncate logs if they exceed the token limit.

    Args:
        logs: Log content to potentially truncate
        max_tokens: Maximum allowed tokens
        pod_name: Pod name for logging

    Returns:
        Tuple of (truncated_logs, was_truncated)
    """
    current_tokens = calculate_context_tokens(logs)
    if current_tokens <= max_tokens:
        return logs, False

    # Estimate characters per token from current content
    chars_per_token = len(logs) / current_tokens if current_tokens > 0 else 4
    target_chars = int(max_tokens * chars_per_token * 0.9)  # 90% to be safe

    # Truncate and add notice
    truncated = logs[:target_chars]
    # Find last newline to avoid cutting mid-line
    last_newline = truncated.rfind('\n')
    if last_newline > target_chars * 0.8:  # Only use if we're not losing too much
        truncated = truncated[:last_newline]

    truncation_notice = f"\n\n[... TRUNCATED: {current_tokens:,} tokens exceeded budget of {max_tokens:,} tokens for pod {pod_name} ...]"
    truncated += truncation_notice

    logger.warning(f"Truncated logs for {pod_name}: {current_tokens:,} -> ~{max_tokens:,} tokens")
    return truncated, True


# ============================================================================
# MCP TOOLS
# ============================================================================


@mcp.tool()
async def list_namespaces() -> List[str]:
    """
    List all namespaces in the Kubernetes cluster.

    Returns:
        List[str]: Alphabetically sorted namespace names. Empty list if access denied or cluster unreachable.
    """
    current_time = time.time()
    if (_namespace_cache["namespaces"] is not None and
            current_time - _namespace_cache["timestamp"] < _NAMESPACE_CACHE_TTL):
        logger.debug("Returning cached namespace list")
        return _namespace_cache["namespaces"]

    async with _namespace_cache_lock:
        current_time = time.time()
        if (_namespace_cache["namespaces"] is not None and
                current_time - _namespace_cache["timestamp"] < _NAMESPACE_CACHE_TTL):
            return _namespace_cache["namespaces"]

        try:
            logger.info("Retrieving all namespaces from Kubernetes cluster")
            namespaces = k8s_core_api.list_namespace()
            ns_names = sorted([ns.metadata.name for ns in namespaces.items if ns.metadata and ns.metadata.name])

            _namespace_cache["namespaces"] = ns_names
            _namespace_cache["timestamp"] = current_time

            logger.info(f"Successfully retrieved {len(ns_names)} namespaces")
            return ns_names

        except ApiException as e:
            if e.status == 403:
                logger.warning(f"Insufficient permissions to list namespaces: {e.reason}. Check RBAC configuration.")
            elif e.status == 401:
                logger.error(f"Authentication failed while listing namespaces: {e.reason}. Check kubeconfig.")
            else:
                logger.error(f"API error while listing namespaces: {e.status} - {e.reason}")
            return []

        except Exception as e:
            logger.error(f"Unexpected error while listing namespaces: {str(e)}", exc_info=True)
            return []


async def detect_tekton_namespaces() -> Dict[str, List[str]]:
    """
    Intelligently identifies and categorizes namespaces related to Tekton/CI-CD ecosystems.

    This tool performs advanced pattern matching to detect and classify namespaces that are part of
    or related to Tekton-based CI/CD systems. It uses a hierarchical classification
    system to organize namespaces by their functional role within the CI/CD pipeline infrastructure.

    The detection algorithm uses pattern matching against namespace names to identify:
    - Core Tekton components and services
    - Tekton pipeline and task execution environments
    - Build and compilation workspaces
    - Integration and deployment namespaces
    - Supporting infrastructure and tooling

    Returns:
        Dict[str, List[str]]: Categorized namespace collections with the following structure:
            - "core_tekton": Namespaces containing "tekton" (primary system components)
            - "tekton_related": Namespaces containing tekton-related patterns
            - "pipeline_related": Namespaces containing "pipeline" (CI/CD workflows)
            - "build_related": Namespaces containing "build" (compilation and packaging)
            - "other_relevant": Namespaces matching other CI/CD ecosystem patterns
    """
    try:
        logger.info("Starting Tekton/CI-CD namespace detection and classification")
        all_namespaces = await list_namespaces()

        if not all_namespaces:
            logger.warning("No namespaces retrieved from cluster - returning empty classification")
            return {
                "core_tekton": [],
                "tekton_related": [],
                "pipeline_related": [],
                "build_related": [],
                "other_relevant": []
            }

        # Define comprehensive patterns for CI/CD ecosystem detection
        cicd_patterns = [
            "tekton", "pipeline", "build", "ci", "cd",
            "openshift-pipelines", "build-service", "release-service",
            "image-controller", "integration-service", "namespace-lister",
            "pipelines-as-code", "smee-client", "tekton-operator",
            "user-ns", "tekton-chains", "tekton-results", "tekton-triggers"
        ]

        result = {
            "core_tekton": [],
            "tekton_related": [],
            "pipeline_related": [],
            "build_related": [],
            "other_relevant": []
        }

        # Classification counters for logging
        classification_stats = {category: 0 for category in result.keys()}
        unclassified_count = 0

        logger.info(f"Classifying {len(all_namespaces)} namespaces using {len(cicd_patterns)} patterns")

        for ns in all_namespaces:
            ns_lower = ns.lower()
            classified = False

            # Priority-based classification (order matters)
            if "tekton" in ns_lower:
                result["core_tekton"].append(ns)
                classification_stats["core_tekton"] += 1
                classified = True
            elif "pipeline" in ns_lower:
                result["pipeline_related"].append(ns)
                classification_stats["pipeline_related"] += 1
                classified = True
            elif "build" in ns_lower:
                result["build_related"].append(ns)
                classification_stats["build_related"] += 1
                classified = True
            elif any(pattern in ns_lower for pattern in cicd_patterns):
                result["other_relevant"].append(ns)
                classification_stats["other_relevant"] += 1
                classified = True

            if not classified:
                unclassified_count += 1

        # Sort results within each category for consistent output
        for category in result:
            result[category].sort()

        # Log classification statistics
        total_classified = sum(classification_stats.values())
        logger.info(f"Namespace classification complete: {total_classified} CI/CD-related, "
                   f"{unclassified_count} other namespaces")

        for category, count in classification_stats.items():
            if count > 0:
                logger.info(f"  {category}: {count} namespaces")

        return result

    except Exception as e:
        logger.error(f"Unexpected error during Tekton namespace detection: {str(e)}", exc_info=True)
        # Return empty but consistent structure on error
        return {
            "core_tekton": [],
            "tekton_related": [],
            "pipeline_related": [],
            "build_related": [],
            "other_relevant": []
        }


@mcp.tool()
async def list_pipelineruns(namespace: str, limit: Optional[int] = 200) -> List[Dict[str, Any]]:
    """
    List Tekton PipelineRuns in a namespace with status and timing details.

    Args:
        namespace: Kubernetes namespace to query.
        limit: Maximum number of PipelineRuns to return (default: 200). Set to 0 for no limit.

    Returns:
        List[Dict]: PipelineRuns with keys: name, pipeline, status, started_at, completed_at, duration.
                    Empty list if none found. [{"error": "msg"}] on failure.
    """
    try:
        logger.info(f"Retrieving PipelineRuns from namespace: {namespace}")

        # Validate namespace parameter
        if not namespace or not isinstance(namespace, str):
            error_msg = f"Invalid namespace parameter: {namespace}. Must be a non-empty string."
            logger.error(error_msg)
            return [{"error": error_msg}]

        # Query Tekton PipelineRuns using Kubernetes Custom Resource API
        list_kwargs = {
            "group": "tekton.dev",
            "version": "v1",
            "namespace": namespace,
            "plural": "pipelineruns",
        }
        if limit:
            list_kwargs["limit"] = limit

        pipeline_runs = k8s_custom_api.list_namespaced_custom_object(**list_kwargs)

        pipeline_run_items = pipeline_runs.get("items", [])
        logger.info(f"Found {len(pipeline_run_items)} PipelineRuns in namespace '{namespace}'")

        if not pipeline_run_items:
            logger.info(f"No PipelineRuns found in namespace '{namespace}'")
            return []

        result = []
        processed_count = 0
        error_count = 0

        for pr in pipeline_run_items:
            try:
                # Extract metadata with null safety
                metadata = pr.get("metadata", {})
                spec = pr.get("spec", {})
                status = pr.get("status", {})

                # Get pipeline reference from multiple possible sources
                # Priority: pipelineRef.name > labels > pipelineSpec metadata > unknown
                pipeline_name = "unknown"

                # 1. Check spec.pipelineRef.name (direct reference to named Pipeline)
                pipeline_ref = spec.get("pipelineRef", {})
                if pipeline_ref and pipeline_ref.get("name"):
                    pipeline_name = pipeline_ref.get("name")

                # 2. Check common Tekton labels (used by Konflux and other platforms)
                if pipeline_name == "unknown":
                    labels = metadata.get("labels", {})
                    # Try multiple common label keys
                    pipeline_name = (
                        labels.get("tekton.dev/pipeline") or
                        labels.get("pipelines.tekton.dev/pipeline") or
                        labels.get("pipelines.openshift.io/pipeline") or
                        "unknown"
                    )

                # 3. Check inline pipelineSpec for name/displayName
                if pipeline_name == "unknown":
                    pipeline_spec = spec.get("pipelineSpec", {})
                    if pipeline_spec:
                        # Some inline specs may have displayName or name metadata
                        pipeline_name = (
                            pipeline_spec.get("displayName") or
                            pipeline_spec.get("name") or
                            "inline-pipeline"
                        )

                # Extract status information
                conditions = status.get("conditions", [])
                current_status = "Unknown"
                if conditions:
                    # Get the latest condition (Tekton uses last condition as current status)
                    latest_condition = conditions[-1]
                    current_status = latest_condition.get("reason", "Unknown")

                # Extract timing information
                start_time = status.get("startTime")
                completion_time = status.get("completionTime")

                # Determine if pipeline is still running
                is_running = current_status in ("Running", "Started", "Pending", "PipelineRunPending")

                # Calculate duration using helper function
                # For running pipelines, calculate elapsed time from start
                duration = "unknown"
                duration_seconds = None
                try:
                    duration = calculate_duration(start_time, completion_time, use_current_if_missing=is_running)
                    duration_seconds = calculate_duration_seconds(start_time, completion_time, use_current_if_missing=is_running)
                except Exception as e:
                    logger.debug(f"Duration calculation failed for PipelineRun {metadata.get('name', 'unknown')}: {e}")
                    duration = "calculation_error"

                pipeline_run_info = {
                    "name": metadata.get("name", "unknown"),
                    "pipeline": pipeline_name,
                    "status": current_status,
                    "started_at": start_time,
                    "completed_at": completion_time,
                    "duration": duration,
                    "duration_seconds": duration_seconds,
                }

                result.append(pipeline_run_info)
                processed_count += 1

            except Exception as e:
                error_count += 1
                logger.warning(f"Error processing individual PipelineRun: {e}")
                # Continue processing other PipelineRuns instead of failing completely
                continue

        logger.info(f"Successfully processed {processed_count} PipelineRuns from namespace '{namespace}' "
                   f"({error_count} errors encountered)")
        result.sort(key=lambda x: x.get("started_at") or "", reverse=True)
        return result

    except ApiException as e:
        if e.status == 404:
            logger.warning(f"Namespace '{namespace}' not found or no PipelineRuns accessible")
            return []
        elif e.status == 403:
            error_msg = (f"Insufficient permissions to list PipelineRuns in namespace '{namespace}'. "
                        f"Required RBAC: pipelineruns.tekton.dev/list")
            logger.error(error_msg)
            return [{"error": error_msg}]
        elif e.status == 401:
            error_msg = f"Authentication failed while accessing namespace '{namespace}'. Check kubeconfig."
            logger.error(error_msg)
            return [{"error": error_msg}]
        else:
            error_msg = f"API error listing PipelineRuns in namespace '{namespace}': {e.status} - {e.reason}"
            logger.error(error_msg)
            return [{"error": error_msg}]

    except Exception as e:
        error_msg = f"Unexpected error listing PipelineRuns in namespace '{namespace}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [{"error": error_msg}]


@mcp.tool()
async def list_taskruns(namespace: str, pipeline_run: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    List Tekton TaskRuns in a namespace, optionally filtered by a specific PipelineRun.

    Args:
        namespace: Kubernetes namespace to query.
        pipeline_run: Optional PipelineRun name to filter by.

    Returns:
        List[Dict]: TaskRuns with keys: name, task, pipeline_run, status, started_at, completed_at, duration.
    """
    try:
        logger.info(f"Retrieving TaskRuns from namespace: {namespace}" +
                   (f" (filtered by PipelineRun: {pipeline_run})" if pipeline_run else ""))

        label_selector = f"tekton.dev/pipelineRun={pipeline_run}" if pipeline_run else None

        # When filtering by pipeline_run, the label_selector narrows the result set
        # so no limit is needed. Without a filter, limit to prevent fetching all
        # TaskRuns in large namespaces (can be 2000+ objects, ~97MB response).
        list_kwargs = {
            "group": "tekton.dev",
            "version": "v1",
            "namespace": namespace,
            "plural": "taskruns",
        }
        if label_selector:
            list_kwargs["label_selector"] = label_selector
        else:
            list_kwargs["limit"] = 200

        task_runs = k8s_custom_api.list_namespaced_custom_object(**list_kwargs)

        result = []
        for tr in task_runs.get("items", []):
            # Skip if filtering by pipeline_run and this task doesn't match
            if pipeline_run and tr.get("metadata", {}).get("labels", {}).get("tekton.dev/pipelineRun") != pipeline_run:
                continue

            metadata = tr.get("metadata", {})
            spec = tr.get("spec", {})
            status = tr.get("status", {})
            labels = metadata.get("labels", {})

            conditions = status.get("conditions", [])
            current_status = conditions[0].get("reason", "Unknown") if conditions else "Unknown"

            # Determine if task is still running
            is_running = current_status in ("Running", "Started", "Pending", "TaskRunPending")

            start_time = status.get("startTime")
            completion_time = status.get("completionTime")

            # Get task name from multiple possible sources
            # Priority: taskRef.name > labels > pipelineTask label > extract from taskrun name
            task_name = None

            # 1. Check spec.taskRef.name (direct reference to named Task)
            task_ref = spec.get("taskRef", {})
            if task_ref and task_ref.get("name"):
                task_name = task_ref.get("name")

            # 2. Check common Tekton labels
            if not task_name:
                task_name = (
                    labels.get("tekton.dev/task") or
                    labels.get("tekton.dev/pipelineTask") or
                    labels.get("pipelines.tekton.dev/task")
                )

            # 3. Try to extract from TaskRun name (format: pipelinerun-taskname-suffix)
            if not task_name:
                tr_name = metadata.get("name", "")
                pr_name = labels.get("tekton.dev/pipelineRun", "")
                if pr_name and tr_name.startswith(pr_name + "-"):
                    # Remove pipelinerun prefix and random suffix
                    remaining = tr_name[len(pr_name) + 1:]
                    # Task name is everything except the last random suffix (usually 5-6 chars)
                    parts = remaining.rsplit("-", 1)
                    if len(parts) > 1 and len(parts[-1]) <= 6:
                        task_name = parts[0]

            result.append({
                "name": metadata.get("name"),
                "task": task_name,
                "pipeline_run": labels.get("tekton.dev/pipelineRun"),
                "status": current_status,
                "started_at": start_time,
                "completed_at": completion_time,
                "duration": calculate_duration(start_time, completion_time, use_current_if_missing=is_running),
                "duration_seconds": calculate_duration_seconds(start_time, completion_time, use_current_if_missing=is_running),
            })

        logger.info(f"Found {len(result)} TaskRuns in namespace '{namespace}'")
        return result

    except ApiException as e:
        logger.error(f"Error listing TaskRuns in namespace {namespace}: {e}")
        return [{"error": str(e)}]


@mcp.tool()
async def list_pods_in_namespace(namespace: str) -> List[Dict[str, Any]]:
    """
    List all pods in a Kubernetes namespace with status and placement info.

    Args:
        namespace: Kubernetes namespace to query.

    Returns:
        List[Dict]: Pods with keys: name, status, ip, node_name, creation_timestamp,
                    restart_count, container_states (list of waiting/terminated reasons).
    """
    if not k8s_core_api:
        return [{"error": "Kubernetes client not available."}]

    pods_info = []
    try:
        logger.info(f"Listing pods in namespace: {namespace}")
        pod_list_resp = await asyncio.to_thread(k8s_core_api.list_namespaced_pod, namespace=namespace)
        pod_list = pod_list_resp.items
        for pod in pod_list:
            # Extract container status information for better prioritization
            total_restart_count = 0
            container_states = []

            # Guard against pod.status being None (pods in early creation)
            if pod.status and pod.status.container_statuses:
                for cs in pod.status.container_statuses:
                    if cs.restart_count:
                        total_restart_count += cs.restart_count

                    # Capture waiting state reasons (CrashLoopBackOff, ImagePullBackOff, etc.)
                    if cs.state:
                        if cs.state.waiting and cs.state.waiting.reason:
                            container_states.append(cs.state.waiting.reason)
                        elif cs.state.terminated and cs.state.terminated.reason:
                            container_states.append(cs.state.terminated.reason)

            # Check init container statuses (common failure point in Tekton)
            if pod.status and pod.status.init_container_statuses:
                for ics in pod.status.init_container_statuses:
                    if ics.restart_count:
                        total_restart_count += ics.restart_count
                    if ics.state:
                        if ics.state.waiting and ics.state.waiting.reason:
                            container_states.append(f"Init:{ics.state.waiting.reason}")
                        elif ics.state.terminated and ics.state.terminated.reason and ics.state.terminated.reason != "Completed":
                            container_states.append(f"Init:{ics.state.terminated.reason}")

            pods_info.append({
                "name": pod.metadata.name,
                "status": pod.status.phase if pod.status else "Unknown",
                "ip": pod.status.pod_ip if pod.status else None,
                "node_name": pod.spec.node_name if pod.spec else "N/A",
                "creation_timestamp": pod.metadata.creation_timestamp.isoformat() if pod.metadata.creation_timestamp else "N/A",
                "restart_count": total_restart_count,
                "container_states": container_states
            })
        logger.info(f"Found {len(pods_info)} pods in namespace '{namespace}'.")
        return pods_info
    except ApiException as e:
        logger.error(f"API error listing pods in namespace '{namespace}': {e}")
        return [{"error": f"API Error: {e.reason}", "namespace": namespace}]
    except Exception as e:
        logger.error(f"Unexpected error listing pods in namespace '{namespace}': {e}", exc_info=True)
        return [{"error": f"Unexpected Error: {str(e)}", "namespace": namespace}]


@mcp.tool()
async def get_kubernetes_resource(
    resource_type: str,
    name: str,
    namespace: str = "default",
    output_format: str = "summary"
) -> str:
    """
    Retrieve details about a Kubernetes/Tekton resource.

    Args:
        resource_type: Resource type. Supported: pod, service, configmap, secret, pvc, namespace, node,
                       serviceaccount, endpoints, event, persistentvolume, resourcequota, limitrange,
                       deployment, replicaset, daemonset, statefulset, job, cronjob, ingress,
                       storageclass, hpa (horizontalpodautoscaler),
                       pipelinerun, taskrun, pipeline, task, clustertask,
                       triggertemplate, triggerbinding, eventlistener,
                       podmonitor, servicemonitor, prometheusrule, alertmanager,
                       application, component, snapshot, release, releaseplan,
                       releaseplanadmission, integrationtestscenario.
        name: Resource name.
        namespace: Namespace (default: "default").
        output_format: "summary", "detailed", or "yaml" (default: "summary").

    Returns:
        str: Formatted resource information.
    """
    try:
        resource_type = resource_type.lower().strip()

        # Define resource mappings
        core_resources = {
            'pod': ('pods', 'v1'),
            'service': ('services', 'v1'),
            'configmap': ('config_maps', 'v1'),
            'secret': ('secrets', 'v1'),
            'pvc': ('persistent_volume_claims', 'v1'),
            'persistentvolumeclaim': ('persistent_volume_claims', 'v1'),
            'namespace': ('namespaces', 'v1'),
            'node': ('nodes', 'v1'),
            'serviceaccount': ('service_accounts', 'v1'),
            'endpoints': ('endpoints', 'v1'),
            'event': ('events', 'v1'),
            'persistentvolume': ('persistent_volumes', 'v1'),
            'pv': ('persistent_volumes', 'v1'),
            'resourcequota': ('resource_quotas', 'v1'),
            'limitrange': ('limit_ranges', 'v1')
        }

        apps_resources = {
            'deployment': ('deployments', 'apps/v1'),
            'replicaset': ('replica_sets', 'apps/v1'),
            'daemonset': ('daemon_sets', 'apps/v1'),
            'statefulset': ('stateful_sets', 'apps/v1')
        }

        batch_resources = {
            'job': ('jobs', 'batch/v1'),
            'cronjob': ('cron_jobs', 'batch/v1')
        }

        networking_resources = {
            'ingress': ('ingresses', 'networking.k8s.io/v1')
        }

        storage_resources = {
            'storageclass': ('storage_classes', 'storage.k8s.io/v1'),
            'sc': ('storage_classes', 'storage.k8s.io/v1')
        }

        autoscaling_resources = {
            'horizontalpodautoscaler': ('horizontal_pod_autoscalers', 'autoscaling/v2'),
            'hpa': ('horizontal_pod_autoscalers', 'autoscaling/v2')
        }

        tekton_resources = {
            'pipelinerun': ('pipelineruns', 'tekton.dev/v1'),
            'taskrun': ('taskruns', 'tekton.dev/v1'),
            'pipeline': ('pipelines', 'tekton.dev/v1'),
            'task': ('tasks', 'tekton.dev/v1'),
            'clustertask': ('clustertasks', 'tekton.dev/v1beta1')  # ClusterTask deprecated, stays v1beta1
        }

        tekton_triggers_resources = {
            'triggertemplate': ('triggertemplates', 'triggers.tekton.dev/v1beta1'),
            'triggerbinding': ('triggerbindings', 'triggers.tekton.dev/v1beta1'),
            'eventlistener': ('eventlisteners', 'triggers.tekton.dev/v1beta1')
        }

        monitoring_resources = {
            'podmonitor': ('podmonitors', 'monitoring.coreos.com/v1'),
            'servicemonitor': ('servicemonitors', 'monitoring.coreos.com/v1'),
            'prometheusrule': ('prometheusrules', 'monitoring.coreos.com/v1'),
            'alertmanager': ('alertmanagers', 'monitoring.coreos.com/v1')
        }

        admission_resources = {
            'validatingadmissionwebhook': ('validatingadmissionwebhooks', 'admissionregistration.k8s.io/v1'),
            'mutatingadmissionwebhook': ('mutatingadmissionwebhooks', 'admissionregistration.k8s.io/v1')
        }

        konflux_resources = {
            'application': ('applications', 'appstudio.redhat.com/v1alpha1'),
            'component': ('components', 'appstudio.redhat.com/v1alpha1'),
            'snapshot': ('snapshots', 'appstudio.redhat.com/v1alpha1'),
            'release': ('releases', 'appstudio.redhat.com/v1alpha1'),
            'releaseplan': ('releaseplans', 'appstudio.redhat.com/v1alpha1'),
            'releaseplanadmission': ('releaseplanadmissions', 'appstudio.redhat.com/v1alpha1'),
            'integrationtestscenario': ('integrationtestscenarios', 'appstudio.redhat.com/v1beta2'),
        }

        resource_obj = None
        api_version = None

        # Fetch resource based on type
        if resource_type in core_resources:
            method_name, api_version = core_resources[resource_type]
            if resource_type in ['namespace', 'node', 'persistentvolume', 'pv']:
                # Cluster-scoped resources
                method = getattr(k8s_core_api, f'read_{method_name[:-1]}')
                resource_obj = await asyncio.to_thread(method, name=name)
            elif resource_type == 'endpoints':
                # Endpoints uses plural form in method name
                resource_obj = await asyncio.to_thread(
                    k8s_core_api.read_namespaced_endpoints,
                    name=name, namespace=namespace
                )
            else:
                # Namespaced resources
                method = getattr(k8s_core_api, f'read_namespaced_{method_name[:-1]}')
                resource_obj = await asyncio.to_thread(
                    method, name=name, namespace=namespace
                )

        elif resource_type in storage_resources:
            # Cluster-scoped storage resources
            # StorageV1Api uses read_storage_class (singular) while the dict
            # stores the plural 'storage_classes' for the supported-types list.
            # Dynamic dispatch via method_name[:-1] doesn't work here because
            # 'storage_classes'[:-1] == 'storage_classe', not 'storage_class'.
            resource_obj = await asyncio.to_thread(
                k8s_storage_api.read_storage_class, name=name
            )

        elif resource_type in autoscaling_resources:
            method_name, api_version = autoscaling_resources[resource_type]
            method = getattr(k8s_autoscaling_api, f'read_namespaced_{method_name[:-1]}')
            resource_obj = await asyncio.to_thread(
                method, name=name, namespace=namespace
            )

        elif resource_type in apps_resources:
            method_name, api_version = apps_resources[resource_type]
            method = getattr(k8s_apps_api, f'read_namespaced_{method_name[:-1]}')
            resource_obj = await asyncio.to_thread(
                method, name=name, namespace=namespace
            )

        elif resource_type in batch_resources:
            method_name, api_version = batch_resources[resource_type]
            method = getattr(k8s_batch_api, f'read_namespaced_{method_name[:-1]}')
            resource_obj = await asyncio.to_thread(
                method, name=name, namespace=namespace
            )

        elif resource_type in networking_resources:
            method_name, api_version = networking_resources[resource_type]
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group="networking.k8s.io",
                version="v1",
                namespace=namespace,
                plural="ingresses",
                name=name
            )

        elif resource_type in monitoring_resources:
            method_name, api_version = monitoring_resources[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group,
                version=version,
                namespace=namespace,
                plural=method_name,
                name=name
            )

        elif resource_type in admission_resources:
            method_name, api_version = admission_resources[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_cluster_custom_object,
                group=group,
                version=version,
                plural=method_name,
                name=name
            )

        elif resource_type in tekton_resources:
            method_name, api_version = tekton_resources[resource_type]
            group, version = api_version.split('/')

            if resource_type == 'clustertask':
                # Cluster-scoped Tekton resource
                resource_obj = await asyncio.to_thread(
                    k8s_custom_api.get_cluster_custom_object,
                    group=group,
                    version=version,
                    plural=method_name,
                    name=name
                )
            else:
                # Namespaced Tekton resource
                resource_obj = await asyncio.to_thread(
                    k8s_custom_api.get_namespaced_custom_object,
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=method_name,
                    name=name
                )

        elif resource_type in tekton_triggers_resources:
            method_name, api_version = tekton_triggers_resources[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group,
                version=version,
                namespace=namespace,
                plural=method_name,
                name=name
            )

        elif resource_type in konflux_resources:
            method_name, api_version = konflux_resources[resource_type]
            group, version = api_version.split('/')
            resource_obj = await asyncio.to_thread(
                k8s_custom_api.get_namespaced_custom_object,
                group=group,
                version=version,
                namespace=namespace,
                plural=method_name,
                name=name
            )

        else:
            supported_types = (
                list(core_resources.keys()) + list(apps_resources.keys()) +
                list(batch_resources.keys()) + list(networking_resources.keys()) +
                list(storage_resources.keys()) + list(autoscaling_resources.keys()) +
                list(tekton_resources.keys()) + list(tekton_triggers_resources.keys()) +
                list(monitoring_resources.keys()) + list(admission_resources.keys()) +
                list(konflux_resources.keys())
            )
            return f"Error: Unsupported resource type '{resource_type}'. Supported types: {', '.join(sorted(supported_types))}"

        if not resource_obj:
            return f"Error: Resource '{name}' of type '{resource_type}' not found in namespace '{namespace}'"

        # Format output based on requested format
        if output_format.lower() == "yaml":
            return format_yaml_output(resource_obj, resource_type, name, namespace)
        elif output_format.lower() == "detailed":
            return format_detailed_output(resource_obj, resource_type, name, namespace)
        else:  # summary
            return format_summary_output(resource_obj, resource_type, name, namespace)

    except ApiException as e:
        if e.status == 404:
            return f"Error: Resource '{name}' of type '{resource_type}' not found in namespace '{namespace}'"
        else:
            return f"Kubernetes API Error: {e.status} - {e.reason}"
    except Exception as e:
        return f"Error retrieving resource: {str(e)}"


@mcp.tool()
async def get_pipelinerun_logs(
    pipelinerun_name: str,
    namespace: str,
    clean_logs: bool = True,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
    max_token_budget: int = 120000
) -> Dict[str, Any]:
    """
    Fetch logs from all pods in a Tekton PipelineRun with adaptive volume management.

    Prioritizes failed pods and manages token budgets automatically when no time/line filters specified.

    Args:
        pipelinerun_name: PipelineRun name.
        namespace: Kubernetes namespace.
        clean_logs: Clean and format logs (default: True).
        tail_lines: Lines from end (optional).
        since_seconds: Logs newer than N seconds (optional).
        since_time: Logs newer than RFC3339 timestamp (optional).
        timestamps: Include timestamps (default: True).
        previous: Get logs from previous container instance (default: False).
        max_token_budget: Maximum tokens for output (default: 120000). Applies to both adaptive and manual modes.

    Returns:
        Dict[str, Any]: Pod names as keys, logs as values. Includes "_metadata" with processing info.
        Returns {"info": "No pods found..."} if pods are garbage collected - use query_kubearchive tool.
    """
    # Build log filtering info for logging
    filter_info = []
    if since_time:
        filter_info.append(f"since_time={since_time}")
    elif since_seconds:
        filter_info.append(f"since_seconds={since_seconds}")
    elif tail_lines:
        filter_info.append(f"tail_lines={tail_lines}")

    filter_str = f" with filters: {', '.join(filter_info)}" if filter_info else ""
    logger.info(f"Fetching logs for PipelineRun '{pipelinerun_name}' in ns '{namespace}'{filter_str}...")
    all_logs = {}

    try:
        # Find pods associated with the PipelineRun using Tekton labels
        # Tekton adds 'tekton.dev/pipelineRun' label to all pods in a PipelineRun
        label_selector = f"tekton.dev/pipelineRun={pipelinerun_name}"

        pod_list = await asyncio.to_thread(
            k8s_core_api.list_namespaced_pod,
            namespace=namespace,
            label_selector=label_selector,
        )

        if not pod_list.items:
            # Fallback: Try alternative label format used by some Tekton versions
            label_selector_alt = f"tekton.dev/pipeline={pipelinerun_name}"
            pod_list = await asyncio.to_thread(
                k8s_core_api.list_namespaced_pod,
                namespace=namespace,
                label_selector=label_selector_alt,
            )

        if not pod_list.items:
            return {"info": f"No pods found for PipelineRun '{pipelinerun_name}'. Check if the PipelineRun exists and has completed pods."}

        # Get all pod names
        pod_names = [pod.metadata.name for pod in pod_list.items]
        logger.info(f"Found {len(pod_names)} pods for PipelineRun '{pipelinerun_name}'")

        # Check if adaptive mode should be used
        use_adaptive_processing = (tail_lines is None and since_seconds is None and since_time is None)

        if use_adaptive_processing:
            logger.info(f"ADAPTIVE MODE activated for PipelineRun '{pipelinerun_name}' - {len(pod_names)} pods detected")

            # Initialize adaptive processor with configurable budget
            processor = AdaptiveLogProcessor(max_token_budget=max_token_budget)

            # Prioritize pods (failed pods first, recent pods next)
            prioritized_pods = await _prioritize_pipeline_pods(pod_names, namespace)

            # Process pods progressively with token management
            processed_pods = 0
            truncated_pods = 0  # Track how many pods had logs truncated
            for pod_name in prioritized_pods:
                # STEP 1: Calculate adaptive tail_lines FIRST based on pipeline size and remaining budget
                adaptive_tail_lines = _calculate_adaptive_tail_lines(
                    len(pod_names), processed_pods, processor.get_remaining_budget()
                )

                # STEP 2: Estimate tokens using the SAME tail_lines that will be used for fetching
                estimated_tokens = await _estimate_pod_log_tokens(namespace, pod_name, tail_lines=adaptive_tail_lines)

                # STEP 3: Check if we can process this pod within budget
                # GUARANTEE: Always process at least the first pod (highest priority - usually failed)
                is_first_pod = (processed_pods == 0)
                if not is_first_pod and not processor.can_process_more(estimated_tokens):
                    logger.info(f"Token budget reached ({processor.get_usage_percentage():.1f}% used) - processed {processed_pods}/{len(pod_names)} pods")
                    break

                try:
                    # STEP 4: Fetch logs with the calculated adaptive_tail_lines
                    pod_logs = await get_all_pod_logs(
                        pod_name=pod_name,
                        namespace=namespace,
                        k8s_core_api=k8s_core_api,
                        tail_lines=adaptive_tail_lines,
                        timestamps=timestamps,
                        previous=previous
                    )

                    # Format and clean logs
                    if len(pod_logs) == 1:
                        container_name, logs = next(iter(pod_logs.items()))
                        if clean_logs:
                            logs = clean_pipeline_logs(logs)
                        all_logs[pod_name] = logs
                    else:
                        formatted_logs = []
                        for container_name, logs in pod_logs.items():
                            if clean_logs:
                                logs = clean_pipeline_logs(logs)
                            formatted_logs.append(f"--- Container: {container_name} ---")
                            formatted_logs.append(logs)
                            formatted_logs.append(f"--- End Container: {container_name} ---")
                        all_logs[pod_name] = "\n".join(formatted_logs)

                    # HARD LIMIT ENFORCEMENT: Truncate if actual tokens exceed remaining budget
                    remaining_budget = processor.get_remaining_budget()
                    actual_tokens = calculate_context_tokens(str(all_logs[pod_name]))

                    if actual_tokens > remaining_budget:
                        # Truncate logs to fit within remaining budget
                        all_logs[pod_name], was_truncated = _truncate_logs_to_token_limit(
                            all_logs[pod_name], remaining_budget, pod_name
                        )
                        if was_truncated:
                            truncated_pods += 1
                        actual_tokens = calculate_context_tokens(str(all_logs[pod_name]))

                    processor.record_usage(actual_tokens)
                    processed_pods += 1

                    logger.info(f"Processed pod {processed_pods}/{len(pod_names)}: {pod_name} ({actual_tokens:,} tokens, {processor.get_usage_percentage():.1f}% budget used)")

                    # Brief pause for rate limiting
                    await asyncio.sleep(0.2)

                except Exception as e:
                    logger.error(f"Error fetching logs for pod {pod_name}: {e}")
                    all_logs[pod_name] = f"Error fetching logs for pod {pod_name}: {str(e)}"

            # Add adaptive processing metadata
            all_logs["_metadata"] = {
                "adaptive_mode": True,
                "pods_processed": processed_pods,
                "pods_truncated": truncated_pods,
                "pods_skipped": len(pod_names) - processed_pods,
                "total_pods_found": len(pod_names),
                "token_budget_used": f"{processor.get_usage_percentage():.1f}%",
                "token_budget_max": processor.max_token_budget,
                "processing_strategy": f"Pipeline size: {len(pod_names)} pods -> adaptive batching"
            }

        else:
            # MANUAL MODE: Use specified parameters with token budget enforcement
            logger.info(f"MANUAL MODE for PipelineRun '{pipelinerun_name}' - using specified constraints")

            # Initialize processor for token tracking in manual mode
            processor = AdaptiveLogProcessor(max_token_budget=max_token_budget)
            truncated_pods = 0

            async def fetch_pod_logs(pod_name):
                try:
                    pod_logs = await get_all_pod_logs(
                        pod_name=pod_name,
                        namespace=namespace,
                        k8s_core_api=k8s_core_api,
                        tail_lines=tail_lines,
                        since_seconds=since_seconds,
                        since_time=since_time,
                        timestamps=timestamps,
                        previous=previous
                    )
                    if len(pod_logs) == 1:
                        container_name, logs = next(iter(pod_logs.items()))
                        if clean_logs:
                            logs = clean_pipeline_logs(logs)
                        return pod_name, logs
                    else:
                        formatted_logs = []
                        for container_name, logs in pod_logs.items():
                            if clean_logs:
                                logs = clean_pipeline_logs(logs)
                            formatted_logs.append(f"--- Container: {container_name} ---")
                            formatted_logs.append(logs)
                            formatted_logs.append(f"--- End Container: {container_name} ---")
                        return pod_name, "\n".join(formatted_logs)
                except Exception as e:
                    logger.error(f"Error fetching logs for pod {pod_name}: {e}")
                    return pod_name, f"Error fetching logs for pod {pod_name}: {str(e)}"

            # Fetch logs concurrently for all pods
            log_tasks = [fetch_pod_logs(pod_name) for pod_name in pod_names]
            results = await asyncio.gather(*log_tasks)

            # Apply token budget limiting to collected logs
            for pod_name, logs in results:
                remaining_budget = processor.get_remaining_budget()
                actual_tokens = calculate_context_tokens(str(logs))

                if actual_tokens > remaining_budget and remaining_budget > 0:
                    # Truncate logs to fit within remaining budget
                    logs, was_truncated = _truncate_logs_to_token_limit(
                        logs, remaining_budget, pod_name
                    )
                    if was_truncated:
                        truncated_pods += 1
                    actual_tokens = calculate_context_tokens(str(logs))
                elif remaining_budget <= 0:
                    # Skip this pod entirely if budget exhausted
                    logs = f"[Skipped - token budget exhausted]"
                    actual_tokens = calculate_context_tokens(logs)

                all_logs[pod_name] = logs
                processor.record_usage(actual_tokens)

            # Add metadata for manual mode
            all_logs["_metadata"] = {
                "mode": "manual",
                "pods_processed": len(pod_names),
                "pods_truncated": truncated_pods,
                "token_budget_used": f"{processor.get_usage_percentage():.1f}%",
                "token_budget_max": max_token_budget,
                "filters_applied": filter_info if filter_info else ["none"]
            }

        return all_logs

    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        return {"error": str(e)}
    except ApiException as e:
        logger.error(f"K8s API error getting PipelineRun pods: {e.status} - {e.reason} - {e.body}")
        return {"error": f"Failed to find pods for PipelineRun: {e.reason}"}
    except Exception as e:
        logger.error(f"Unexpected error getting PipelineRun logs: {e}", exc_info=True)
        return {"error": f"An unexpected error occurred: {str(e)}"}


@mcp.tool()
async def check_resource_constraints(namespace: str) -> Dict[str, Any]:
    """
    Check for resource constraints in a namespace that may impact pipelines.

    Identifies: pending/unschedulable pods, OOMKilled containers, CrashLoopBackOff,
    ImagePullBackOff, high restart counts, and resource quota utilization.

    Args:
        namespace: Kubernetes namespace to inspect.

    Returns:
        Dict[str, Any]: Keys: status (Healthy/Warning/Critical/Error), summary, resource_quotas,
                        pending_pods_due_to_resources, oom_killed_containers, container_issues,
                        high_utilization_quotas, recommendations.
    """
    try:
        # Get pods in the namespace
        pods = await list_pods(namespace, k8s_core_api, logger)

        # Get resource quotas
        resource_quotas = k8s_core_api.list_namespaced_resource_quota(namespace)

        # Check for resource problems in pod status
        resource_issues = []
        pending_pods = []
        oom_killed_pods = []

        for pod in pods:
            pod_name = pod.get("name")
            pod_status = pod.get("status")

            # Fetch detailed pod info once per pod that needs inspection
            if pod_status in ["Failed", "Pending", "Running"]:
                detailed_pod = k8s_core_api.read_namespaced_pod(
                    name=pod_name, namespace=namespace)

                # Check for pending pods (potential scheduling issues)
                if pod_status == "Pending" and detailed_pod.status and detailed_pod.status.conditions:
                    for condition in detailed_pod.status.conditions:
                        if condition.type == "PodScheduled" and condition.status == "False":
                            pending_pods.append({
                                "pod": pod_name,
                                "issue": "Unschedulable",
                                "reason": condition.reason or "Unknown",
                                "message": condition.message or ""
                            })
                            break
                    else:
                        pending_pods.append({
                            "pod": pod_name,
                            "issue": "Pending",
                            "reason": "Unknown",
                            "message": "Pod is pending without specific reason"
                        })
                elif pod_status == "Pending":
                    pending_pods.append({
                        "pod": pod_name,
                        "issue": "Pending",
                        "reason": "Unknown",
                        "message": "Pod is pending without specific reason"
                    })

                # Check container statuses for issues
                def _check_container_statuses(statuses, prefix=""):
                    if not statuses:
                        return
                    for container_status in statuses:
                        cname = f"{prefix}{container_status.name}" if prefix else container_status.name
                        # Check current state for waiting issues
                        if hasattr(container_status, "state") and container_status.state:
                            if container_status.state.waiting:
                                reason = container_status.state.waiting.reason
                                if reason in ["CrashLoopBackOff", "OOMKilled", "ImagePullBackOff", "ErrImagePull", "CreateContainerError", "CreateContainerConfigError", "ContainerCreating"]:
                                    resource_issues.append({
                                        "pod": pod_name,
                                        "container": cname,
                                        "issue": reason,
                                        "message": container_status.state.waiting.message or ""
                                    })

                        # Check last_state for OOMKilled (container restarted after OOM)
                        if hasattr(container_status, "last_state") and container_status.last_state:
                            if container_status.last_state.terminated:
                                if container_status.last_state.terminated.reason == "OOMKilled":
                                    oom_killed_pods.append({
                                        "pod": pod_name,
                                        "container": cname,
                                        "issue": "OOMKilled",
                                        "restart_count": container_status.restart_count,
                                        "message": f"Container was OOMKilled and restarted {container_status.restart_count} times"
                                    })

                        # Check for high restart counts (potential resource issues)
                        if container_status.restart_count and container_status.restart_count > 5:
                            resource_issues.append({
                                "pod": pod_name,
                                "container": cname,
                                "issue": "HighRestartCount",
                                "restart_count": container_status.restart_count,
                                "message": f"Container has restarted {container_status.restart_count} times"
                            })

                if detailed_pod.status:
                    _check_container_statuses(detailed_pod.status.container_statuses)
                    _check_container_statuses(detailed_pod.status.init_container_statuses, prefix="init:")

        # Format resource quotas
        quota_data = []
        for quota in resource_quotas.items:
            if quota.status.hard and quota.status.used:
                quota_item = {
                    "name": quota.metadata.name,
                    "resources": {}
                }

                for resource, hard_limit in quota.status.hard.items():
                    used = quota.status.used.get(resource, "0")
                    quota_item["resources"][resource] = {
                        "limit": hard_limit,
                        "used": used,
                        "utilization": calculate_utilization(used, hard_limit)
                    }

                quota_data.append(quota_item)

        # Check for high utilization quotas
        high_utilization = [
            quota_item for quota_item in quota_data
            if any(
                resource.get("utilization", 0) > 80
                for resource in quota_item.get("resources", {}).values()
            )
        ]

        # Determine overall status
        status = "Healthy"
        summary_parts = []

        total_issues = len(resource_issues) + len(pending_pods) + len(oom_killed_pods)

        if oom_killed_pods:
            status = "Critical"
            summary_parts.append(f"{len(oom_killed_pods)} OOMKilled containers")
        if pending_pods:
            status = "Critical" if status != "Critical" else status
            summary_parts.append(f"{len(pending_pods)} pending/unschedulable pods")
        if resource_issues:
            status = "Warning" if status == "Healthy" else status
            summary_parts.append(f"{len(resource_issues)} container issues")
        if high_utilization:
            status = "Warning" if status == "Healthy" else status
            summary_parts.append(f"{len(high_utilization)} quotas with high utilization")

        if summary_parts:
            summary = f"Found: {', '.join(summary_parts)}"
        else:
            summary = "No significant resource constraints detected"

        # Generate recommendations
        recommendations = []
        if oom_killed_pods:
            recommendations.append("Increase memory limits for OOMKilled containers")
            recommendations.append("Review application memory usage patterns")
        if pending_pods:
            unschedulable = [p for p in pending_pods if p.get("issue") == "Unschedulable"]
            if unschedulable:
                recommendations.append("Check node resources - pods cannot be scheduled due to insufficient resources")
            recommendations.append("Review pending pods and their resource requests")
        if resource_issues:
            crash_loops = [i for i in resource_issues if i.get("issue") == "CrashLoopBackOff"]
            image_issues = [i for i in resource_issues if i.get("issue") in ["ImagePullBackOff", "ErrImagePull"]]
            config_errors = [i for i in resource_issues if i.get("issue") in ["CreateContainerError", "CreateContainerConfigError"]]
            high_restarts = [i for i in resource_issues if i.get("issue") == "HighRestartCount"]
            if crash_loops:
                recommendations.append("Investigate CrashLoopBackOff containers - check logs for errors")
            if image_issues:
                recommendations.append("Fix image pull issues - verify image names and registry access")
            if config_errors:
                recommendations.append("Fix container configuration errors - check secrets, configmaps, and volume mounts")
            if high_restarts:
                recommendations.append("Investigate containers with high restart counts")
        if high_utilization:
            recommendations.append("Monitor resource quota usage and consider increasing limits")

        return {
            "status": status,
            "summary": summary,
            "resource_quotas": quota_data,
            "pending_pods_due_to_resources": pending_pods,
            "oom_killed_containers": oom_killed_pods,
            "container_issues": resource_issues,
            "high_utilization_quotas": high_utilization,
            "recommendations": recommendations
        }

    except ApiException as e:
        logger.error(f"Kubernetes API error checking resource constraints in namespace {namespace}: {e}")
        return {
            "status": "Error",
            "summary": f"Kubernetes API error: {str(e)}",
            "resource_quotas": [],
            "pending_pods_due_to_resources": [],
            "oom_killed_containers": [],
            "container_issues": [],
            "high_utilization_quotas": [],
            "recommendations": ["Check cluster connectivity and permissions"],
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error checking resource constraints in namespace {namespace}: {e}")
        return {
            "status": "Error",
            "summary": f"Unexpected error: {str(e)}",
            "resource_quotas": [],
            "pending_pods_due_to_resources": [],
            "oom_killed_containers": [],
            "container_issues": [],
            "high_utilization_quotas": [],
            "recommendations": ["Review logs for detailed error information"],
            "error": str(e)
        }


@mcp.tool()
async def detect_anomalies(namespace: str, limit: int = 50) -> Dict[str, List[Dict[str, Any]]]:
    """
    Detect anomalies in Tekton PipelineRuns/TaskRuns using z-score statistical analysis.

    Identifies unusually long execution times (threshold: 2.5 standard deviations from mean).

    Args:
        namespace: Kubernetes namespace to analyze.
        limit: Max recent PipelineRuns to analyze (default: 50).

    Returns:
        Dict: Keys: pipeline_anomalies, task_anomalies (lists with anomaly details).
    """
    try:
        # Get pipeline runs
        pipeline_runs = await list_pipelineruns(namespace)

        # Limit to the most recent runs
        # Use 'or ""' to handle None values (not just missing keys)
        pipeline_runs = sorted(
            pipeline_runs,
            key=lambda x: x.get("started_at") or "",
            reverse=True
        )[:limit]

        # Get ALL task runs in one API call (bulk fetch instead of N+1)
        all_task_runs = await list_taskruns(namespace, pipeline_run=None)

        # Create a set of pipeline run names for fast lookup
        pr_names = {pr.get("name") for pr in pipeline_runs}

        # Collect durations for anomaly detection
        pipeline_data = []
        task_data = []

        # Process pipeline runs
        for pr in pipeline_runs:
            # Parse pipeline duration
            if pr.get("status") == "Succeeded" and pr.get("duration") and pr.get("duration") != "unknown":
                try:
                    value = pr.get("duration").split()[0]
                    if value.replace(".", "", 1).isdigit():
                        pipeline_data.append({
                            "name": pr.get("name"),
                            "duration": float(value)
                        })
                except (ValueError, IndexError):
                    continue

        # Process task runs (filter in memory - much faster than N API calls)
        for tr in all_task_runs:
            # Only include tasks belonging to our selected pipeline runs
            tr_pipeline = tr.get("pipeline_run")
            if tr_pipeline not in pr_names:
                continue

            if tr.get("status") == "Succeeded" and tr.get("duration") and tr.get("duration") != "unknown":
                try:
                    value = tr.get("duration").split()[0]
                    if value.replace(".", "", 1).isdigit():
                        task_data.append({
                            "name": tr.get("name"),
                            "duration": float(value),
                            "pipeline_run": tr_pipeline
                        })
                except (ValueError, IndexError):
                    continue

        # Detect anomalies
        pipeline_anomaly_result = detect_anomalies_in_data(
            [d["duration"] for d in pipeline_data], pipeline_data
        )
        task_anomaly_result = detect_anomalies_in_data(
            [d["duration"] for d in task_data], task_data
        )

        # Extract anomaly lists from helper function results
        pipeline_anomalies = []
        if pipeline_anomaly_result.get("anomalies_detected") and pipeline_anomaly_result.get("anomaly_details"):
            for anomaly in pipeline_anomaly_result["anomaly_details"].get("anomalies", []):
                original_data = anomaly.get("original_data", {})
                stats = pipeline_anomaly_result["anomaly_details"]["statistics"]
                pipeline_anomalies.append({
                    "name": original_data.get("name", "unknown"),
                    "reason": f"Unusually long duration (z-score: {anomaly.get('z_score', 0):.2f})",
                    "actual_value": anomaly.get("value"),
                    "expected_range": (
                        max(0, stats["mean"] - 2.5 * stats["std_dev"]),
                        stats["mean"] + 2.5 * stats["std_dev"]
                    )
                })

        task_anomalies = []
        if task_anomaly_result.get("anomalies_detected") and task_anomaly_result.get("anomaly_details"):
            for anomaly in task_anomaly_result["anomaly_details"].get("anomalies", []):
                original_data = anomaly.get("original_data", {})
                stats = task_anomaly_result["anomaly_details"]["statistics"]
                task_anomalies.append({
                    "name": original_data.get("name", "unknown"),
                    "pipeline_run": original_data.get("pipeline_run", "unknown"),
                    "reason": f"Unusually long duration (z-score: {anomaly.get('z_score', 0):.2f})",
                    "actual_value": anomaly.get("value"),
                    "expected_range": (
                        max(0, stats["mean"] - 2.5 * stats["std_dev"]),
                        stats["mean"] + 2.5 * stats["std_dev"]
                    )
                })

        return {
            "pipeline_anomalies": pipeline_anomalies,
            "task_anomalies": task_anomalies
        }

    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}")
        return {
            "pipeline_anomalies": [],
            "task_anomalies": [],
            "error": str(e)
        }


# ============================================================================
# INTERNAL HELPER FUNCTIONS
# ============================================================================


async def _get_namespace_events_internal(
    namespace: str,
    last_n_events: Optional[int] = None,
    time_period: Optional[str] = None,
    max_fetch_limit: int = 5000
) -> Dict[str, Any]:
    """
    Internal function to fetch Kubernetes events from a namespace with optional filtering.

    Uses pagination to handle large event volumes efficiently and prevent connection timeouts.

    Args:
        namespace: Kubernetes namespace to fetch events from
        last_n_events: Limit to last N events (optional)
        time_period: Time period like '1h', '30m', '2d' (optional)
        max_fetch_limit: Maximum events to fetch per page

    Returns:
        Dictionary with events list and metadata
    """
    from datetime import datetime, timedelta

    logger.info(f"Fetching events from namespace '{namespace}'")
    if last_n_events is not None:
        logger.info(f"Will filter to last {last_n_events} events")
    if time_period is not None:
        logger.info(f"Will filter to events from last {time_period}")

    output: Dict[str, Any] = {
        "namespace": namespace,
        "events": [],
        "errors": [],
        "applied_filters": {}
    }
    events_list: List[str] = []
    errors_list: List[str] = []

    try:
        # Calculate time filter if provided
        cutoff_time = None
        if time_period is not None:
            try:
                time_delta = parse_time_period(time_period)
                cutoff_time = datetime.now() - time_delta
                output["applied_filters"]["time_period"] = time_period
                output["applied_filters"]["cutoff_time"] = cutoff_time.isoformat()
            except Exception as e:
                errors_list.append(f"Error parsing time period: {str(e)}")
                logger.error(f"Error parsing time period: {e}")

        # Fetch events using pagination
        all_events = []
        continue_token = None
        page_count = 0
        MAX_PAGES = 20  # Safety limit

        logger.info(f"Fetching events with pagination (limit={max_fetch_limit} per page)")

        while page_count < MAX_PAGES:
            try:
                if continue_token:
                    event_list_response = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_event,
                        namespace=namespace,
                        watch=False,
                        limit=max_fetch_limit,
                        _continue=continue_token
                    )
                else:
                    event_list_response = await asyncio.to_thread(
                        k8s_core_api.list_namespaced_event,
                        namespace=namespace,
                        watch=False,
                        limit=max_fetch_limit
                    )

                page_count += 1
                page_events = len(event_list_response.items)
                all_events.extend(event_list_response.items)

                logger.info(f"Fetched page {page_count}: {page_events} events (total: {len(all_events)})")

                continue_token = event_list_response.metadata._continue

                if not continue_token:
                    logger.info(f"All events fetched ({len(all_events)} total)")
                    break

                if last_n_events and len(all_events) >= last_n_events * 2:
                    logger.info(f"Fetched sufficient events for filtering")
                    break

                if cutoff_time and event_list_response.items:
                    def get_event_time(event):
                        timestamp = event.last_timestamp or event.first_timestamp
                        if timestamp is None:
                            return datetime.max
                        if timestamp.tzinfo is not None:
                            return timestamp.replace(tzinfo=None)
                        return timestamp

                    oldest_in_page = min(event_list_response.items, key=get_event_time)
                    oldest_time = get_event_time(oldest_in_page)

                    if oldest_time < cutoff_time:
                        logger.info(f"Reached events older than cutoff time")
                        break

            except ApiException as e:
                if e.status == 410:
                    logger.warning(f"Continue token expired at page {page_count}")
                    break
                else:
                    raise

        if page_count >= MAX_PAGES and continue_token:
            logger.warning(f"Reached maximum page limit ({MAX_PAGES} pages)")
            errors_list.append(f"Event fetching limited to {len(all_events)} events due to volume.")

        original_count = len(all_events)
        logger.info(f"Found {original_count} events in namespace '{namespace}'")

        # Sort events by timestamp (most recent first)
        def get_comparable_timestamp(event):
            timestamp = event.last_timestamp or event.first_timestamp
            if timestamp is None:
                return datetime.min.replace(tzinfo=None)
            if timestamp.tzinfo is not None:
                return timestamp.replace(tzinfo=None)
            return timestamp

        events = sorted(all_events, key=get_comparable_timestamp, reverse=True)

        # Apply time period filtering
        if time_period is not None and cutoff_time is not None:
            filtered_events = []
            for event in events:
                event_time = get_comparable_timestamp(event)
                if event_time >= cutoff_time:
                    filtered_events.append(event)
            events = filtered_events
            logger.info(f"Filtered to {len(events)} events after time period filter")

        # Apply count filtering
        if last_n_events is not None and len(events) > last_n_events:
            events = events[:last_n_events]
            output["applied_filters"]["last_n_events"] = last_n_events
            logger.info(f"Limited to last {last_n_events} events")

        # Convert events to string format
        for event in events:
            try:
                timestamp = event.last_timestamp or event.first_timestamp or "Unknown"
                event_str = f"[{timestamp}] {event.type}: {event.reason} - {event.message}"
                if event.involved_object:
                    event_str += f" (Object: {event.involved_object.kind}/{event.involved_object.name})"
                events_list.append(event_str)
            except Exception as e:
                errors_list.append(f"Error formatting event: {str(e)}")
                logger.error(f"Error formatting event: {e}")

        output["events"] = events_list
        output["errors"] = errors_list
        output["original_events_count"] = original_count
        output["filtered_events_count"] = len(events_list)
        output["pagination_info"] = {
            "pages_fetched": page_count,
            "hit_page_limit": page_count >= MAX_PAGES and continue_token is not None
        }

        logger.info(f"Returning {len(events_list)} formatted events")
        return output

    except Exception as e:
        error_msg = f"Failed to fetch events from namespace '{namespace}': {str(e)}"
        logger.error(error_msg)
        return {
            "namespace": namespace,
            "events": [],
            "errors": [error_msg],
            "applied_filters": {}
        }


async def _get_namespace_events_as_dicts(
    namespace: str,
    limit: int = 100,
    time_period: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Fetch Kubernetes events as dictionaries for use with FailureEventCollector.

    Unlike _get_namespace_events_internal which returns formatted strings,
    this function returns raw event data as dictionaries.

    Args:
        namespace: Kubernetes namespace to fetch events from
        limit: Maximum number of events to fetch
        time_period: Optional time period like '1h', '30m', '2d'

    Returns:
        List of event dictionaries with keys: type, reason, message,
        involved_object, last_timestamp, first_timestamp, count, name
    """
    from datetime import datetime, timedelta

    events_as_dicts: List[Dict[str, Any]] = []

    try:
        # Calculate time filter if provided
        cutoff_time = None
        if time_period is not None:
            try:
                time_delta = parse_time_period(time_period)
                cutoff_time = datetime.now() - time_delta
            except Exception as e:
                logger.debug(f"Error parsing time period: {e}")

        # Fetch events
        event_list_response = await asyncio.to_thread(
            k8s_core_api.list_namespaced_event,
            namespace=namespace,
            watch=False,
            limit=limit
        )

        for event in event_list_response.items:
            try:
                # Apply time filter if specified
                if cutoff_time:
                    event_time = event.last_timestamp or event.first_timestamp
                    if event_time:
                        if event_time.tzinfo is not None:
                            event_time = event_time.replace(tzinfo=None)
                        if event_time < cutoff_time:
                            continue

                # Convert to dict format expected by FailureEventCollector
                event_dict = {
                    "type": event.type or "Normal",
                    "reason": event.reason or "",
                    "message": event.message or "",
                    "name": event.metadata.name if event.metadata else "",
                    "last_timestamp": event.last_timestamp.isoformat() if event.last_timestamp else None,
                    "first_timestamp": event.first_timestamp.isoformat() if event.first_timestamp else None,
                    "count": event.count or 1,
                    "involved_object": {}
                }

                # Extract involved object details
                if event.involved_object:
                    event_dict["involved_object"] = {
                        "name": event.involved_object.name or "",
                        "kind": event.involved_object.kind or "",
                        "namespace": event.involved_object.namespace or namespace,
                        "uid": event.involved_object.uid or ""
                    }

                events_as_dicts.append(event_dict)

            except Exception as e:
                logger.debug(f"Error converting event to dict: {e}")
                continue

        logger.debug(f"Fetched {len(events_as_dicts)} events as dicts from {namespace}")
        return events_as_dicts

    except Exception as e:
        logger.debug(f"Failed to fetch events as dicts from {namespace}: {e}")
        return []


@mcp.tool()
async def smart_get_namespace_events(
    namespace: str,
    last_n_events: Optional[int] = None,
    time_period: Optional[str] = None,
    strategy: str = "auto",
    focus_areas: Optional[List[str]] = None,
    max_context_tokens: int = 8000,
    include_summary: bool = True
) -> Dict[str, Any]:
    """
    Adaptive event analysis for a namespace with automatic volume management.

    When no constraints specified, automatically: estimates volume, applies smart time windows,
    prioritizes errors/warnings, samples within token limits.

    Args:
        namespace: Kubernetes namespace to analyze.
        last_n_events: Exact event count (only if user specifies).
        time_period: Exact time window (only if user specifies).
        strategy: "auto" for adaptive behavior (default).
        focus_areas: Areas to emphasize (default: ["errors", "warnings", "failures"]).
        max_context_tokens: Max output tokens (default: 8000).
        include_summary: Include summary and insights (default: True).

    Returns:
        Dict: Events with adaptive filtering, insights, and recommendations.
    """
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings", "failures"]

    tool_name = "smart_get_namespace_events"
    logger.info(f"[{tool_name}] Starting smart event analysis for namespace '{namespace}'")

    try:
        # Validate inputs
        if not namespace or not namespace.strip():
            return {"error": "Namespace cannot be empty"}

        if max_context_tokens < 1000:
            logger.warning(f"[{tool_name}] Low token limit ({max_context_tokens}), setting to 1000")
            max_context_tokens = 1000

        # Step 1: Determine strategy and apply defaults
        if strategy == "auto":
            strategy = "smart_summary"
            logger.info(f"[{tool_name}] Auto-selected strategy: {strategy}")

        # Step 2: Apply intelligent defaults if no parameters provided - ADAPTIVE MODE
        if last_n_events is None and time_period is None:
            logger.info(f"[{tool_name}] No filters provided - activating ADAPTIVE MODE")

            # Quick volume estimation using very recent events
            try:
                recent_sample = await _get_namespace_events_internal(
                    namespace=namespace,
                    time_period="10m"
                )

                sample_count = recent_sample.get("filtered_events_count", 0)
                estimated_hourly_events = sample_count * 6  # 10min * 6 = 1 hour

                if estimated_hourly_events > 500:
                    time_period = "30m"
                    logger.info(f"[{tool_name}] HIGH EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 30min window")
                    if "errors" not in focus_areas:
                        focus_areas = ["errors", "warnings"] + [f for f in focus_areas if f not in ["errors", "warnings"]]
                elif estimated_hourly_events > 50:
                    time_period = "2h"
                    logger.info(f"[{tool_name}] MEDIUM EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 2h window")
                else:
                    time_period = "6h"
                    logger.info(f"[{tool_name}] LOW EVENT VOLUME detected (~{estimated_hourly_events}/hour) - using 6h window")

            except Exception as e:
                logger.warning(f"[{tool_name}] Volume estimation failed, using safe default: {e}")
                time_period = SMART_EVENTS_CONFIG["defaults"]["default_time_window"]

            logger.info(f"[{tool_name}] ADAPTIVE STRATEGY selected: {time_period} time window")

        # Step 3: Fetch events using internal function
        logger.info(f"[{tool_name}] Fetching events with filters: last_n={last_n_events}, time_period={time_period}")

        raw_result = await _get_namespace_events_internal(
            namespace=namespace,
            last_n_events=last_n_events,
            time_period=time_period
        )

        if "errors" in raw_result and raw_result["errors"]:
            return {"error": f"Failed to fetch events: {raw_result['errors']}"}

        events_count = raw_result.get("filtered_events_count", 0)
        events_list = raw_result.get("events", [])

        logger.info(f"[{tool_name}] Retrieved {events_count} events, processing with strategy: {strategy}")

        # Step 4: Apply intelligent processing based on strategy
        if strategy == "smart_summary":

            if not events_list:
                return {
                    "namespace": namespace,
                    "strategy_used": "smart_summary",
                    "total_events": 0,
                    "processed_events": 0,
                    "events": [],
                    "summary": {"total_events": 0, "message": "No events found in the specified timeframe"},
                    "insights": ["No events found - this could indicate either a quiet period or issues with event generation"],
                    "recommendations": ["Verify that applications are generating events as expected"],
                    "token_usage": {"total_estimated": 200},
                    "applied_filters": raw_result.get("applied_filters", {}),
                    "smart_features": {
                        "intelligent_defaults": time_period if last_n_events is None else None,
                        "context_overflow_prevention": True,
                        "focus_areas": focus_areas
                    }
                }

            # Apply smart sampling and analysis
            selected_events = smart_sample_string_events(events_list, focus_areas, max_context_tokens)

            # Generate summary if requested
            summary = {}
            if include_summary:
                summary = generate_string_events_summary(selected_events, focus_areas)

            # Generate insights and recommendations
            insights = generate_string_events_insights(selected_events)
            recommendations = generate_string_events_recommendations(selected_events)

            # Calculate token usage
            total_tokens = sum(event["token_estimate"] for event in selected_events)
            summary_tokens = len(str(summary).split()) * 1.3 if summary else 0
            metadata_tokens = 200

            return {
                "namespace": namespace,
                "strategy_used": "smart_summary",
                "total_events": events_count,
                "processed_events": len(selected_events),
                "events": [
                    {
                        "event_string": event["event_string"],
                        "severity": event["severity"],
                        "category": event["category"],
                        "relevance_score": round(event["relevance_score"], 2),
                        "timestamp": event["timestamp"].isoformat(),
                        "token_estimate": event["token_estimate"]
                    }
                    for event in selected_events
                ],
                "summary": summary,
                "insights": insights,
                "recommendations": recommendations,
                "token_usage": {
                    "events_tokens": int(total_tokens),
                    "summary_tokens": int(summary_tokens),
                    "metadata_tokens": metadata_tokens,
                    "total_estimated": int(total_tokens + summary_tokens + metadata_tokens)
                },
                "applied_filters": raw_result.get("applied_filters", {}),
                "smart_features": {
                    "intelligent_defaults": time_period if last_n_events is None else None,
                    "context_overflow_prevention": True,
                    "focus_areas": focus_areas,
                    "classification_applied": True,
                    "smart_sampling": True
                },
                "classification_metadata": {
                    "severity_distribution": {
                        severity.value: len([e for e in selected_events if e["severity"] == severity.value])
                        for severity in EventSeverity
                    },
                    "category_distribution": {
                        category.value: len([e for e in selected_events if e["category"] == category.value])
                        for category in EventCategory
                    }
                }
            }

        elif strategy == "raw":
            # Limited raw processing
            max_raw = SMART_EVENTS_CONFIG["defaults"]["max_events_raw"]
            return {
                "namespace": namespace,
                "strategy_used": "raw_limited",
                "total_events": events_count,
                "processed_events": min(events_count, max_raw),
                "events": events_list[:max_raw] if events_list else [],
                "applied_limits": {
                    "max_raw_events": max_raw,
                    "truncated": events_count > max_raw
                },
                "token_usage": {
                    "total_estimated": min(events_count, max_raw) * 60
                },
                "note": "Raw strategy with safety limits applied to prevent context overflow"
            }

        else:  # progressive or fallback
            return {
                "namespace": namespace,
                "strategy_used": "progressive",
                "total_events": events_count,
                "note": "Progressive analysis strategy - showing overview",
                "events_overview": {
                    "total_found": events_count,
                    "time_period": time_period,
                    "preview": events_list[:5] if events_list else [],
                    "suggestion": "Use smart_summary strategy for detailed analysis"
                },
                "quick_insights": [
                    f"Found {events_count} events in namespace '{namespace}'",
                    "Use 'smart_summary' strategy for intelligent analysis",
                    "Progressive disclosure enables drilling down into specific issues"
                ]
            }

    except Exception as e:
        logger.error(f"[{tool_name}] Unexpected error: {str(e)}", exc_info=True)
        return {
            "error": f"Smart event analysis failed: {str(e)}",
            "fallback_suggestion": "Try using the original get_namespace_events tool with explicit filters"
        }


# @mcp.tool()  # Commented out - Konflux-specific tool
async def get_konflux_components_status() -> Dict[str, Any]:
    """
    Retrieves a comprehensive status overview of all Konflux components across all accessible Kubernetes namespaces.

    This asynchronous function provides a high-level health check and status report for the entire
    Konflux ecosystem deployed within the Kubernetes cluster. It performs:

    1. Discovery of Konflux-related namespaces using pattern matching
    2. Collection of deployment statuses (replicas, availability)
    3. Aggregation of PipelineRun statistics by status
    4. Resource quota usage analysis

    Returns:
        Dict[str, Any]: A dictionary containing comprehensive Konflux status:
            - namespaces: Categorized list of Konflux-related namespaces
            - components: Deployment statuses organized by namespace
            - pipeline_stats: PipelineRun counts and status breakdown per namespace
            - resource_usage: Resource quota utilization per namespace

    Example output structure:
        {
            "namespaces": {
                "core_konflux": ["konflux-ci"],
                "tekton_related": ["tekton-pipelines"],
                ...
            },
            "components": {
                "konflux-ci": {
                    "deployments": [
                        {"name": "controller", "ready": "2/2", ...}
                    ]
                }
            },
            "pipeline_stats": {
                "user-ns-1": {"total": 50, "status_counts": {"Succeeded": 45, "Failed": 5}}
            },
            "resource_usage": {...}
        }
    """
    try:
        logger.info("Retrieving Konflux components status across all namespaces")

        # First identify all Konflux namespaces
        tekton_namespaces = await detect_tekton_namespaces()

        # Initialize results
        results = {
            "namespaces": tekton_namespaces,
            "components": {},
            "pipeline_stats": {},
            "resource_usage": {}
        }

        # Count total namespaces for logging
        total_namespaces = sum(len(ns_list) for ns_list in tekton_namespaces.values())
        logger.info(f"Found {total_namespaces} Konflux-related namespaces to analyze")

        # For each Konflux namespace, get key resources
        for namespace_type, namespaces in tekton_namespaces.items():
            for namespace in namespaces:
                # Get deployments
                try:
                    deployments = k8s_apps_api.list_namespaced_deployment(namespace)
                    deployment_statuses = []

                    for deployment in deployments.items:
                        deployment_statuses.append({
                            "name": deployment.metadata.name,
                            "ready": f"{deployment.status.ready_replicas or 0}/{deployment.status.replicas}",
                            "up_to_date": deployment.status.updated_replicas,
                            "available": deployment.status.available_replicas
                        })

                    if deployment_statuses:
                        if namespace not in results["components"]:
                            results["components"][namespace] = {}
                        results["components"][namespace]["deployments"] = deployment_statuses
                        logger.debug(f"Found {len(deployment_statuses)} deployments in {namespace}")

                except ApiException as e:
                    logger.warning(f"Could not get deployments in namespace {namespace}: {e}")

                # Get pipeline runs stats
                try:
                    pipeline_runs = await list_pipelineruns(namespace)
                    if pipeline_runs and isinstance(pipeline_runs, list) and not any("error" in pr for pr in pipeline_runs if isinstance(pr, dict)):
                        # Count by status
                        status_counts = {}
                        for pr in pipeline_runs:
                            status = pr.get("status", "Unknown")
                            status_counts[status] = status_counts.get(status, 0) + 1

                        results["pipeline_stats"][namespace] = {
                            "total": len(pipeline_runs),
                            "status_counts": status_counts
                        }
                        logger.debug(f"Found {len(pipeline_runs)} pipeline runs in {namespace}")

                except Exception as e:
                    logger.warning(f"Could not get pipeline runs in namespace {namespace}: {e}")

                # Get resource quotas
                try:
                    resource_quotas = k8s_core_api.list_namespaced_resource_quota(namespace)
                    if resource_quotas.items:
                        results["resource_usage"][namespace] = []
                        for quota in resource_quotas.items:
                            quota_data = {
                                "name": quota.metadata.name,
                                "resources": {}
                            }

                            if quota.status.hard and quota.status.used:
                                for resource, hard_limit in quota.status.hard.items():
                                    used = quota.status.used.get(resource, "0")
                                    quota_data["resources"][resource] = {
                                        "limit": hard_limit,
                                        "used": used,
                                        "utilization": calculate_utilization(used, hard_limit)
                                    }

                            results["resource_usage"][namespace].append(quota_data)

                except ApiException as e:
                    logger.warning(f"Could not get resource quotas in namespace {namespace}: {e}")

        # Add summary statistics
        total_deployments = sum(
            len(ns_data.get("deployments", []))
            for ns_data in results["components"].values()
        )
        total_pipelines = sum(
            stats.get("total", 0)
            for stats in results["pipeline_stats"].values()
        )

        results["summary"] = {
            "total_namespaces_analyzed": total_namespaces,
            "namespaces_with_deployments": len(results["components"]),
            "total_deployments": total_deployments,
            "namespaces_with_pipelines": len(results["pipeline_stats"]),
            "total_pipeline_runs": total_pipelines
        }

        logger.info(f"Konflux status complete: {total_deployments} deployments, {total_pipelines} pipeline runs")
        return results

    except Exception as e:
        logger.error(f"Error getting Konflux components status: {e}", exc_info=True)
        return {"error": str(e)}


async def get_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False
) -> Dict[str, Any]:
    """
    Get logs from a pod using the same interface expected by analysis tools.

    This function wraps get_all_pod_logs to provide a consistent interface
    for pod log retrieval across all tools in the system.

    Args:
        namespace: Kubernetes namespace containing the pod
        pod_name: Name of the pod to get logs from
        container_name: Specific container name (optional)
        tail_lines: Number of lines to retrieve from end of logs
        since_seconds: Retrieve logs newer than this many seconds
        since_time: Retrieve logs newer than this timestamp
        timestamps: Include timestamps in log output
        previous: Retrieve logs from previous container instance

    Returns:
        Dict with either:
        - {"logs": {"container_name": "logs", ...}} on success
        - {"error": "error_message"} on failure
    """
    try:
        # Call the underlying get_all_pod_logs function
        pod_logs = await get_all_pod_logs(
            pod_name=pod_name,
            namespace=namespace,
            k8s_core_api=k8s_core_api,
            tail_lines=tail_lines,
            since_seconds=since_seconds,
            since_time=since_time,
            timestamps=timestamps,
            previous=previous
        )

        # Check if we got an error response
        if isinstance(pod_logs, dict):
            # Check for error indicators
            error_keys = [k for k in pod_logs.keys() if k.startswith(('error_', 'pod_error', 'no_'))]
            if error_keys:
                error_msg = pod_logs.get(error_keys[0], "Unknown error retrieving logs")
                return {"error": error_msg}

            # Filter by container if specified
            if container_name:
                if container_name in pod_logs:
                    return {"logs": {container_name: pod_logs[container_name]}}
                else:
                    return {"error": f"Container '{container_name}' not found in pod '{pod_name}'"}

            # Return all container logs
            return {"logs": pod_logs}

        # Handle unexpected response format
        return {"error": f"Unexpected response format from get_all_pod_logs: {type(pod_logs)}"}

    except Exception as e:
        logger.error(f"Error in get_pod_logs for pod {pod_name} in namespace {namespace}: {e}")
        return {"error": f"Failed to retrieve logs: {str(e)}"}


@mcp.tool()
async def analyze_logs(log_text: str) -> Dict[str, Any]:
    """
    Analyze log text to extract error patterns and insights.

    Args:
        log_text: Log content string (single entry, multiple lines, or full log file).

    Returns:
        Dict[str, Any]: Keys: error_count, error_patterns, categorized_errors, summary.
    """
    try:
        error_patterns = extract_error_patterns(log_text)
        error_categories = categorize_errors(log_text, error_patterns)

        return {
            "error_count": len(error_patterns),
            "error_patterns": error_patterns,
            "categorized_errors": error_categories,
            "summary": generate_log_summary(log_text, error_patterns, error_categories)
        }
    except Exception as e:
        logger.error(f"Error in analyze_logs: {e}", exc_info=True)
        return {
            "error_count": 0,
            "error_patterns": [],
            "categorized_errors": {},
            "summary": f"Analysis failed: {str(e)}"
        }


@mcp.tool()
async def analyze_failed_pipeline(namespace: str, pipeline_run: str) -> Dict[str, Any]:
    """
    Perform root cause analysis on a failed Tekton PipelineRun.

    Fetches pipeline/task details, analyzes logs for errors, and provides remediation recommendations.

    Args:
        namespace: Kubernetes namespace of the PipelineRun.
        pipeline_run: Name of the failed PipelineRun.

    Returns:
        Dict[str, Any]: Keys: pipeline_name, pipeline_status, overall_message, failed_task_count,
                        failed_tasks, probable_root_cause, recommended_actions.
    """
    try:
        logger.info(f"Analyzing failed pipeline '{pipeline_run}' in namespace '{namespace}'")

        # Get pipeline details
        pipeline_details = await get_pipeline_details(
            namespace, pipeline_run, k8s_custom_api, list_taskruns, calculate_duration, logger
        )

        if "error" in pipeline_details:
            return {"error": pipeline_details["error"]}

        # Check if the pipeline actually failed
        if pipeline_details.get("status") == "Succeeded":
            return {
                "error": "Pipeline did not fail, it succeeded",
                "pipeline_status": pipeline_details.get("status")
            }

        # Find failed tasks
        failed_tasks = [
            task for task in pipeline_details.get("task_runs", [])
            if task.get("status") not in ("Succeeded", "Running", "Started", "Pending", "TaskRunPending", None)
        ]

        results = {
            "pipeline_name": pipeline_details.get("pipeline"),
            "pipeline_status": pipeline_details.get("status"),
            "overall_message": pipeline_details.get("message"),
            "failed_task_count": len(failed_tasks),
            "failed_tasks": []
        }

        logger.info(f"Found {len(failed_tasks)} failed tasks in pipeline '{pipeline_run}'")

        # Detailed analysis of each failed task
        for task in failed_tasks:
            task_name = task.get("name")
            task_details = await get_task_details(
                namespace, task_name, k8s_custom_api, calculate_duration, logger
            )

            # Get logs for the pod associated with this task
            pod_name = task_details.get("pod", "unknown")
            pod_logs_available = True
            log_content = ""
            logs_unavailable_reason = None

            if pod_name == "unknown":
                pod_logs_available = False
                logs_unavailable_reason = "No pod associated with this task"
            else:
                pod_logs = await get_pod_logs(namespace, pod_name)

                # Extract log content as string for analysis
                if isinstance(pod_logs, dict) and "logs" in pod_logs:
                    for container, logs in pod_logs["logs"].items():
                        if isinstance(logs, list):
                            log_content += "\n".join(logs)
                        else:
                            log_content += str(logs)
                elif isinstance(pod_logs, dict) and "error" in pod_logs:
                    pod_logs_available = False
                    error_msg = pod_logs.get("error", "")
                    if "Not Found" in error_msg:
                        logs_unavailable_reason = "Pod was deleted (normal for completed pipelines)"
                    else:
                        logs_unavailable_reason = error_msg

            # Build failed step info from TaskRun status as fallback/supplement
            failed_steps = []
            for step in task_details.get("steps", []):
                if step.get("exit_code") is not None and step.get("exit_code") != 0:
                    failed_steps.append({
                        "step_name": step.get("name"),
                        "exit_code": step.get("exit_code"),
                        "reason": step.get("reason")
                    })

            # Analyze logs if available, otherwise use step info for context
            if pod_logs_available and log_content.strip():
                log_analysis = await analyze_logs(log_content)
                error_patterns = log_analysis.get("error_patterns", [])
                error_categories = log_analysis.get("categorized_errors", {})

                # If log analysis found nothing but steps failed, supplement with step info
                if not error_patterns and failed_steps:
                    task_message = task_details.get("message", "")
                    for step in failed_steps:
                        step_msg = f"Step '{step['step_name']}' failed with exit code {step['exit_code']}"
                        if step.get("reason"):
                            step_msg += f" (reason: {step['reason']})"
                        error_patterns.append(step_msg)
                    if task_message:
                        error_patterns.append(f"Task message: {task_message}")
                    error_categories["step_failures"] = len(failed_steps)
            else:
                # Use step failure info when logs unavailable
                error_patterns = []
                error_categories = {}
                for step in failed_steps:
                    error_patterns.append(f"Step '{step['step_name']}' failed with exit code {step['exit_code']}")
                if failed_steps:
                    error_categories["step_failures"] = len(failed_steps)

            # Build task result
            task_result = {
                "task_name": task.get("task"),
                "task_run": task_name,
                "status": task_details.get("status"),
                "message": task_details.get("message"),
                "error_patterns": error_patterns,
                "error_categories": error_categories,
                "pod": pod_name,
                "failed_steps": failed_steps
            }

            # Add note if logs were unavailable
            if not pod_logs_available:
                task_result["logs_unavailable"] = True
                task_result["logs_unavailable_reason"] = logs_unavailable_reason

            results["failed_tasks"].append(task_result)

        # Determine root cause and recommend actions
        results["probable_root_cause"] = determine_root_cause(results)
        actions = recommend_actions(results)
        seen = set()
        results["recommended_actions"] = [a for a in actions if a not in seen and not seen.add(a)]

        logger.info(f"Pipeline analysis complete. Root cause: {results['probable_root_cause'][:50]}...")
        return results

    except Exception as e:
        logger.error(f"Error analyzing failed pipeline {pipeline_run}: {e}", exc_info=True)
        return {"error": str(e)}


@mcp.tool()
async def list_recent_pipeline_runs(limit: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    List recent Tekton PipelineRuns across all accessible namespaces, sorted by start time.

    Args:
        limit: Max PipelineRuns to retrieve (default: 10).

    Returns:
        Dict[str, List[Dict]]: Namespace to PipelineRun list. Each run has: namespace, name,
                               start_time, status, pipeline, labels.
    """
    results: Dict[str, List[Dict[str, Any]]] = {}

    try:
        logger.info(f"Listing recent pipeline runs across all namespaces (limit: {limit})")

        # Use cluster-wide query with limit for performance (single API call)
        # Use a fixed fetch limit for consistent results regardless of requested limit
        # The API doesn't sort, so we need to fetch enough to ensure we get the most recent
        fetch_limit = 200  # Fixed limit for consistent results

        pipeline_runs = k8s_custom_api.list_cluster_custom_object(
            group="tekton.dev",
            version="v1",
            plural="pipelineruns",
            limit=fetch_limit
        )

        # Collect all pipeline runs
        all_runs: List[Dict[str, Any]] = []

        for pr in pipeline_runs.get("items", []):
            status = pr.get("status", {})
            metadata = pr.get("metadata", {})
            namespace = metadata.get("namespace", "unknown")

            # Get the start time for sorting
            start_time = status.get("startTime")
            if not start_time:
                # If no start time, use creation time
                start_time = metadata.get("creationTimestamp")

            if start_time:
                # Get status from conditions
                conditions = status.get("conditions", [])
                current_status = "Unknown"
                if conditions:
                    current_status = conditions[-1].get("reason", "Unknown")

                # Get pipeline name from multiple sources (same logic as list_pipelineruns)
                spec = pr.get("spec", {})
                labels = metadata.get("labels", {})
                pipeline_name = "unknown"

                # 1. Check spec.pipelineRef.name (direct reference)
                pipeline_ref = spec.get("pipelineRef", {})
                if pipeline_ref and pipeline_ref.get("name"):
                    pipeline_name = pipeline_ref.get("name")

                # 2. Check common Tekton labels (used by Konflux)
                if pipeline_name == "unknown":
                    pipeline_name = (
                        labels.get("tekton.dev/pipeline") or
                        labels.get("pipelines.tekton.dev/pipeline") or
                        labels.get("pipelines.openshift.io/pipeline") or
                        "unknown"
                    )

                # 3. Check inline pipelineSpec
                if pipeline_name == "unknown":
                    pipeline_spec = spec.get("pipelineSpec", {})
                    if pipeline_spec:
                        pipeline_name = (
                            pipeline_spec.get("displayName") or
                            pipeline_spec.get("name") or
                            "inline-pipeline"
                        )

                all_runs.append({
                    "namespace": namespace,
                    "name": metadata.get("name", "unknown"),
                    "start_time": start_time,
                    "status": current_status,
                    "pipeline": pipeline_name,
                    "labels": labels
                })

        logger.info(f"Found {len(all_runs)} pipeline runs from cluster-wide query")

        # Sort by start time (most recent first)
        # Use 'or ""' to handle None values
        all_runs.sort(key=lambda x: x.get("start_time") or "", reverse=True)

        # Group by namespace (limited to top N)
        for run in all_runs[:limit]:
            namespace = run["namespace"]
            if namespace not in results:
                results[namespace] = []
            results[namespace].append(run)

        return results

    except Exception as e:
        logger.error(f"Error listing recent pipeline runs: {e}", exc_info=True)
        return {"error": str(e)}


# @mcp.tool()  # Commented out - Konflux-specific tool
async def track_pipeline_across_namespaces(pipeline_id: str) -> Dict[str, Any]:
    """
    Tracks a specific Konflux pipeline and its associated components across all accessible namespaces.

    This tool provides a comprehensive, holistic view of a Konflux pipeline identified by a unique
    pipeline_id, regardless of which namespace its various execution components reside in.
    Konflux pipelines can span multiple namespaces in multi-tenant or complex deployment scenarios.

    The tracking process involves:
    1. Iterating through all accessible Konflux-related namespaces
    2. Searching for Tekton resources (PipelineRuns, TaskRuns) associated with the pipeline_id
    3. Aggregating status, logs, and metadata of all found components
    4. Constructing a coherent view of the pipeline's execution flow across namespaces

    Args:
        pipeline_id: The unique identifier for the Konflux pipeline to track.
                    This could be a PipelineRun name, Application name, or other identifier
                    that links related resources via labels or naming conventions.

    Returns:
        Dict[str, Any]: Aggregated status and details containing:
            - pipeline_id: The identifier being tracked
            - pipeline_runs: List of associated PipelineRun details with namespace info
            - task_runs: List of associated TaskRun details with namespace info
            - pods: List of related pods with log summaries
            - related_resources: Other resources linked to this pipeline
    """
    try:
        logger.info(f"Tracking pipeline '{pipeline_id}' across all namespaces")

        # Get all relevant namespaces
        tekton_namespaces = await detect_tekton_namespaces()
        all_namespaces = []
        for ns_list in tekton_namespaces.values():
            all_namespaces.extend(ns_list)

        logger.info(f"Searching {len(all_namespaces)} namespaces for pipeline '{pipeline_id}'")

        # Track pipeline components
        results = {
            "pipeline_id": pipeline_id,
            "pipeline_runs": [],
            "task_runs": [],
            "pods": [],
            "related_resources": []
        }

        # Look for pipeline runs in all namespaces
        for namespace in all_namespaces:
            # Look for exact pipeline run by name
            try:
                pipeline_run = await get_pipeline_details(
                    namespace, pipeline_id, k8s_custom_api, list_taskruns, calculate_duration, logger
                )
                if "error" not in pipeline_run:
                    results["pipeline_runs"].append({
                        "namespace": namespace,
                        "details": pipeline_run
                    })

                    # Get related task runs
                    task_runs = await list_taskruns(namespace, pipeline_id)
                    for task_run in task_runs:
                        task_details = await get_task_details(
                            namespace, task_run["name"], k8s_custom_api, calculate_duration, logger
                        )
                        results["task_runs"].append({
                            "namespace": namespace,
                            "details": task_details
                        })

                        # Get related pod
                        pod_name = task_details.get("pod")
                        if pod_name and pod_name != "unknown":
                            pod_logs_result = await get_pod_logs(namespace, pod_name)

                            # Extract log content as string for analysis
                            if isinstance(pod_logs_result, dict) and "logs" in pod_logs_result:
                                log_content = ""
                                for pod, logs in pod_logs_result["logs"].items():
                                    if isinstance(logs, list):
                                        log_content += "\n".join(logs)
                                    else:
                                        log_content += str(logs)
                            else:
                                log_content = str(pod_logs_result) if pod_logs_result else "No pod logs available"

                            log_analysis = await analyze_logs(log_content)

                            results["pods"].append({
                                "namespace": namespace,
                                "name": pod_name,
                                "log_summary": generate_log_summary(
                                    log_content,
                                    log_analysis.get("error_patterns", []),
                                    log_analysis.get("categorized_errors", {})
                                )
                            })
            except Exception as e:
                logger.warning(f"Error tracking pipeline in namespace {namespace}: {e}")

        # Check for pipeline related resources by labels
        for namespace in all_namespaces:
            try:
                # Look for resources with labels related to this pipeline
                pods = await list_pods(namespace, k8s_core_api, logger)
                for pod in pods:
                    labels = pod.get("labels", {})
                    # Check if this pod is related to our pipeline
                    if labels and (
                        labels.get("tekton.dev/pipelineRun") == pipeline_id or
                        labels.get("konflux.pipeline") == pipeline_id or
                        pipeline_id in labels.get("tekton.dev/pipelineRun", "") or
                        pipeline_id in pod.get("name", "")
                    ):
                        results["related_resources"].append({
                            "kind": "Pod",
                            "namespace": namespace,
                            "name": pod.get("name"),
                            "status": pod.get("status")
                        })
            except Exception as e:
                logger.warning(f"Error finding related resources in namespace {namespace}: {e}")

        # Add summary
        results["summary"] = {
            "pipeline_runs_found": len(results["pipeline_runs"]),
            "task_runs_found": len(results["task_runs"]),
            "pods_found": len(results["pods"]),
            "related_resources_found": len(results["related_resources"]),
            "namespaces_searched": len(all_namespaces)
        }

        logger.info(f"Pipeline tracking complete: {results['summary']}")
        return results

    except Exception as e:
        logger.error(f"Error tracking pipeline across namespaces: {e}", exc_info=True)
        return {"error": str(e)}


@mcp.tool()
async def find_pipeline(
    pipeline_id_pattern: str,
    include_taskruns: bool = False,
    max_results: int = 100,
    namespaces: Optional[List[str]] = None,
    pipeline_runs_limit: int = 1000,
    task_runs_limit: int = 500
) -> Dict[str, Any]:
    """
    Find Tekton pipelines matching a pattern across all accessible namespaces.

    Searches PipelineRuns/TaskRuns by name, labels, or annotations using cluster-wide queries.

    Args:
        pipeline_id_pattern: Pattern to match (partial name, label value, or substring).
        include_taskruns: Include TaskRuns in search results (default: False for performance).
        max_results: Maximum matching results to return per resource type (default: 100).
        namespaces: Optional list of namespaces to search (default: all namespaces).
        pipeline_runs_limit: Max PipelineRuns to fetch from API (default: 1000).
        task_runs_limit: Max TaskRuns to fetch from API if include_taskruns=True (default: 500).

    Returns:
        Dict[str, Any]: Keys: pipeline_runs, task_runs, pipelines_as_code, all_namespaces_checked,
                        diagnostic_info, substring_matches.
    """
    from concurrent.futures import ThreadPoolExecutor

    results = {
        "pipeline_runs": [],
        "task_runs": [],
        "all_namespaces_checked": [],
        "diagnostic_info": {}
    }

    try:
        logger.info(f"Searching for pipeline pattern '{pipeline_id_pattern}' (include_taskruns={include_taskruns}, max_results={max_results})")
        pattern_lower = pipeline_id_pattern.lower()

        # Use ThreadPoolExecutor for parallel API calls
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=3)

        def fetch_pipelineruns_namespaced(ns: str):
            try:
                return k8s_custom_api.list_namespaced_custom_object(
                    group="tekton.dev",
                    version="v1",
                    namespace=ns,
                    plural="pipelineruns",
                    limit=pipeline_runs_limit
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        def fetch_pipelineruns_cluster():
            try:
                # Cap at 200 to avoid multi-MB responses causing IncompleteRead
                safe_limit = min(pipeline_runs_limit, 200)
                return k8s_custom_api.list_cluster_custom_object(
                    group="tekton.dev",
                    version="v1",
                    plural="pipelineruns",
                    limit=safe_limit
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        def fetch_taskruns_namespaced(ns: str):
            try:
                return k8s_custom_api.list_namespaced_custom_object(
                    group="tekton.dev",
                    version="v1",
                    namespace=ns,
                    plural="taskruns",
                    limit=task_runs_limit
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        def fetch_taskruns_cluster():
            try:
                # Cap at 100 -- cluster-wide TaskRun LIST is the most expensive
                # call (~97MB response). Prefer namespace-scoped queries instead.
                safe_limit = min(task_runs_limit, 100)
                return k8s_custom_api.list_cluster_custom_object(
                    group="tekton.dev",
                    version="v1",
                    plural="taskruns",
                    limit=safe_limit
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        def fetch_repositories():
            try:
                return k8s_custom_api.list_cluster_custom_object(
                    group="pipelinesascode.tekton.dev",
                    version="v1alpha1",
                    plural="repositories",
                    limit=500
                )
            except ApiException as e:
                return {"error": str(e), "items": []}

        # Fetch based on namespace targeting
        if namespaces:
            # Targeted namespace search - fetch from specific namespaces in parallel
            logger.info(f"Searching in {len(namespaces)} specified namespaces")
            pr_futures = [loop.run_in_executor(executor, fetch_pipelineruns_namespaced, ns) for ns in namespaces]
            pipeline_runs_resps = await asyncio.gather(*pr_futures)
            pipeline_runs_resp = {"items": []}
            for resp in pipeline_runs_resps:
                if "error" not in resp:
                    pipeline_runs_resp["items"].extend(resp.get("items", []))
                else:
                    pipeline_runs_resp["error"] = resp.get("error")

            if include_taskruns:
                tr_futures = [loop.run_in_executor(executor, fetch_taskruns_namespaced, ns) for ns in namespaces]
                task_runs_resps = await asyncio.gather(*tr_futures)
                task_runs_resp = {"items": []}
                for resp in task_runs_resps:
                    if "error" not in resp:
                        task_runs_resp["items"].extend(resp.get("items", []))
                    else:
                        task_runs_resp["error"] = resp.get("error")
            else:
                task_runs_resp = {"items": [], "skipped": True}

            repo_future = loop.run_in_executor(executor, fetch_repositories)
            repositories_resp = await repo_future
        else:
            # Cluster-wide search with limits
            pr_future = loop.run_in_executor(executor, fetch_pipelineruns_cluster)
            repo_future = loop.run_in_executor(executor, fetch_repositories)

            if include_taskruns:
                tr_future = loop.run_in_executor(executor, fetch_taskruns_cluster)
                pipeline_runs_resp, task_runs_resp, repositories_resp = await asyncio.gather(
                    pr_future, tr_future, repo_future
                )
            else:
                pipeline_runs_resp, repositories_resp = await asyncio.gather(pr_future, repo_future)
                task_runs_resp = {"items": [], "skipped": True}

        # Track namespaces found and counts for sampling info
        namespaces_seen = set()
        pr_total_scanned = 0
        tr_total_scanned = 0
        pr_matches_truncated = False
        tr_matches_truncated = False

        # Process PipelineRuns with max_results limit
        if "error" in pipeline_runs_resp:
            results["diagnostic_info"]["pipelineruns_error"] = pipeline_runs_resp["error"]

        pr_items = pipeline_runs_resp.get("items", [])
        for pr in pr_items:
            pr_total_scanned += 1
            namespace = pr.get("metadata", {}).get("namespace", "")
            namespaces_seen.add(namespace)
            pr_name = pr.get("metadata", {}).get("name", "")
            labels = pr.get("metadata", {}).get("labels", {})

            if (pattern_lower in pr_name.lower() or
                    any(pattern_lower in str(v).lower() for v in labels.values())):
                if len(results["pipeline_runs"]) >= max_results:
                    pr_matches_truncated = True
                    break  # Stop processing once max_results reached
                status = pr.get("status", {})
                conditions = status.get("conditions", [{}])
                condition = conditions[-1] if conditions else {}

                results["pipeline_runs"].append({
                    "namespace": namespace,
                    "name": pr_name,
                    "status": condition.get("reason", "Unknown"),
                    "message": condition.get("message", ""),
                    "started_at": status.get("startTime", "unknown"),
                    "completion_time": status.get("completionTime", "unknown"),
                    "labels": labels
                })

        # Process TaskRuns only if include_taskruns is True
        if task_runs_resp.get("skipped"):
            results["diagnostic_info"]["taskruns_skipped"] = "Set include_taskruns=True to search TaskRuns"
        else:
            if "error" in task_runs_resp:
                results["diagnostic_info"]["taskruns_error"] = task_runs_resp["error"]

            tr_items = task_runs_resp.get("items", [])
            for tr in tr_items:
                tr_total_scanned += 1
                namespace = tr.get("metadata", {}).get("namespace", "")
                namespaces_seen.add(namespace)
                tr_name = tr.get("metadata", {}).get("name", "")
                labels = tr.get("metadata", {}).get("labels", {})
                pipeline_run = labels.get("tekton.dev/pipelineRun", "")

                if (pattern_lower in tr_name.lower() or
                    pattern_lower in pipeline_run.lower() or
                        any(pattern_lower in str(v).lower() for v in labels.values())):
                    if len(results["task_runs"]) >= max_results:
                        tr_matches_truncated = True
                        break  # Stop processing once max_results reached
                    status = tr.get("status", {})
                    conditions = status.get("conditions", [{}])
                    condition = conditions[-1] if conditions else {}

                    results["task_runs"].append({
                        "namespace": namespace,
                        "name": tr_name,
                        "pipeline_run": pipeline_run,
                        "status": condition.get("reason", "Unknown"),
                        "message": condition.get("message", ""),
                        "pod_name": status.get("podName", "unknown"),
                        "labels": labels
                    })

        # Process Repositories
        # When namespaces filter is specified, only include repositories from those namespaces
        if "error" in repositories_resp:
            results["diagnostic_info"]["repositories_error"] = repositories_resp["error"]
        for repo in repositories_resp.get("items", []):
            namespace = repo.get("metadata", {}).get("namespace", "")
            repo_name = repo.get("metadata", {}).get("name", "")

            # Skip repositories not in the specified namespaces filter
            if namespaces and namespace not in namespaces:
                continue

            # Only add to namespaces_seen if we're actually considering this repository
            namespaces_seen.add(namespace)

            if pattern_lower in repo_name.lower():
                spec = repo.get("spec", {})
                status = repo.get("status", {})
                results.setdefault("pipelines_as_code", []).append({
                    "namespace": namespace,
                    "name": repo_name,
                    "url": spec.get("url", "unknown"),
                    "runs": status.get("runs", [])
                })

        # Set all_namespaces_checked based on what was actually searched
        # If namespaces filter was provided, show those; otherwise show discovered namespaces
        if namespaces:
            results["all_namespaces_checked"] = sorted(namespaces)
        else:
            results["all_namespaces_checked"] = sorted(namespaces_seen)

        # Add summary with sampling info
        results["summary"] = {
            "pipeline_runs_found": len(results["pipeline_runs"]),
            "task_runs_found": len(results["task_runs"]),
            "namespaces_with_tekton_resources": len(namespaces_seen),
            "pipeline_runs_scanned": pr_total_scanned,
            "task_runs_scanned": tr_total_scanned,
            "pipeline_runs_truncated": pr_matches_truncated,
            "task_runs_truncated": tr_matches_truncated,
            "include_taskruns": include_taskruns,
            "max_results_limit": max_results
        }

        logger.info(f"Pipeline search complete: {results['summary']}")
        return results

    except Exception as e:
        logger.error(f"Error finding pipeline {pipeline_id_pattern}: {e}", exc_info=True)
        return {"error": str(e), "diagnostic_info": results.get("diagnostic_info", {})}


@mcp.tool()
async def get_tekton_pipeline_runs_status(
    pipeline_runs_limit: int = 500,
    task_runs_limit_per_namespace: int = 100,
    max_namespaces: int = 20,
    recent_failures_limit: int = 10,
    long_running_limit: int = 5
) -> Dict[str, Any]:
    """
    Get cluster-wide status summary of all Tekton PipelineRuns and TaskRuns.

    Shows running/succeeded/failed counts, recent failures, and long-running pipelines (>1 hour).

    Args:
        pipeline_runs_limit: Max PipelineRuns to fetch cluster-wide (default: 500).
        task_runs_limit_per_namespace: Max TaskRuns to fetch per namespace (default: 100).
        max_namespaces: Max namespaces to scan for TaskRuns (default: 20).
        recent_failures_limit: Max recent failures to include in output (default: 10).
        long_running_limit: Max long-running pipelines to include (default: 5).

    Returns:
        Dict[str, Any]: Keys: timestamp, sampling_info, pipeline_runs (total, by_status,
                        recent_failures [top N], failures_by_namespace, long_running [top N]),
                        task_runs (total, by_status, recent_failures [top N], failures_by_namespace),
                        insights.
    """
    try:
        logger.info("Fetching cluster-wide Tekton PipelineRuns and TaskRuns status")

        # Cap the limit to avoid massive responses that cause IncompleteRead errors.
        # Cluster-wide LIST on pipelineruns can return multi-MB responses.
        safe_pr_limit = min(pipeline_runs_limit, 200)

        # First, get namespaces with Tekton activity to scope queries
        # Fetch PipelineRuns per-namespace for reliability on large clusters
        all_namespaces = []
        try:
            ns_list = k8s_core_api.list_namespace(label_selector="toolchain.dev.openshift.com/type=tenant")
            all_namespaces = [ns.metadata.name for ns in ns_list.items]
            logger.info(f"Found {len(all_namespaces)} tenant namespaces")
        except Exception:
            # Fallback: cluster-wide query with safe limit
            logger.info("Namespace label selector failed, falling back to cluster-wide query")

        pipeline_runs_items = []
        active_namespaces = set()

        if all_namespaces:
            # Per-namespace fetch with limit -- avoids 97MB cluster-wide responses
            per_ns_limit = max(5, safe_pr_limit // min(len(all_namespaces), max_namespaces))
            for ns in all_namespaces[:max_namespaces * 2]:
                try:
                    ns_prs = k8s_custom_api.list_namespaced_custom_object(
                        group="tekton.dev", version="v1",
                        namespace=ns, plural="pipelineruns",
                        limit=per_ns_limit
                    )
                    items = ns_prs.get('items', [])
                    if items:
                        pipeline_runs_items.extend(items)
                        active_namespaces.add(ns)
                except Exception as e:
                    logger.debug(f"Error fetching PipelineRuns from {ns}: {e}")
                    continue
                if len(pipeline_runs_items) >= safe_pr_limit:
                    break
            pipeline_runs_items = pipeline_runs_items[:safe_pr_limit]
        else:
            # Fallback: cluster-wide with safe limit
            pipeline_runs = k8s_custom_api.list_cluster_custom_object(
                group="tekton.dev", version="v1",
                plural="pipelineruns", limit=safe_pr_limit
            )
            pipeline_runs_items = pipeline_runs.get('items', [])
            for pr in pipeline_runs_items:
                ns = pr.get('metadata', {}).get('namespace')
                if ns:
                    active_namespaces.add(ns)

        pipeline_runs = {'items': pipeline_runs_items}

        # Fetch TaskRuns only from active namespaces with limits
        task_runs_items = []
        for ns in list(active_namespaces)[:max_namespaces]:
            try:
                ns_task_runs = k8s_custom_api.list_namespaced_custom_object(
                    group="tekton.dev", version="v1",
                    namespace=ns, plural="taskruns",
                    limit=task_runs_limit_per_namespace
                )
                task_runs_items.extend(ns_task_runs.get('items', []))
            except Exception as e:
                logger.debug(f"Error fetching TaskRuns from {ns}: {e}")
                continue

        task_runs = {'items': task_runs_items}

        analysis = {
            'timestamp': datetime.now().isoformat(),
            'sampling_info': {
                'pipeline_runs_limit': pipeline_runs_limit,
                'task_runs_limit_per_namespace': task_runs_limit_per_namespace,
                'max_namespaces': max_namespaces,
                'namespaces_sampled': min(len(active_namespaces), max_namespaces),
                'recent_failures_limit': recent_failures_limit,
                'long_running_limit': long_running_limit,
                'note': 'Results are sampled to prevent timeout on large clusters'
            },
            'pipeline_runs': {
                'total': len(pipeline_runs.get('items', [])),
                'by_status': {},
                'recent_failures': [],
                'long_running': []
            },
            'task_runs': {
                'total': len(task_runs.get('items', [])),
                'by_status': {},
                'recent_failures': []
            },
            'insights': []
        }

        logger.info(f"Analyzing {analysis['pipeline_runs']['total']} PipelineRuns and {analysis['task_runs']['total']} TaskRuns")

        # Analyze PipelineRuns
        for pr in pipeline_runs.get('items', []):
            status = pr.get('status', {})
            conditions = status.get('conditions', [])

            # Get latest condition
            if conditions:
                latest_condition = conditions[-1]
                condition_type = latest_condition.get('type', 'Unknown')
                condition_status = latest_condition.get('status', 'Unknown')

                status_key = f"{condition_type}_{condition_status}"
                analysis['pipeline_runs']['by_status'][status_key] = \
                    analysis['pipeline_runs']['by_status'].get(status_key, 0) + 1

                # Check for failures
                if condition_type == 'Succeeded' and condition_status == 'False':
                    failure_info = {
                        'name': pr.get('metadata', {}).get('name', 'unknown'),
                        'namespace': pr.get('metadata', {}).get('namespace', 'unknown'),
                        'reason': latest_condition.get('reason', 'Unknown'),
                        'message': latest_condition.get('message', 'No message')[:200],  # Truncate long messages
                        'start_time': status.get('startTime', 'Unknown')
                    }
                    analysis['pipeline_runs']['recent_failures'].append(failure_info)

                # Check for long-running pipelines
                start_time_str = status.get('startTime')
                if start_time_str and not status.get('completionTime'):
                    try:
                        start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                        runtime = datetime.now(start_time.tzinfo) - start_time
                        if runtime.total_seconds() > 3600:  # 1 hour
                            long_running_info = {
                                'name': pr.get('metadata', {}).get('name', 'unknown'),
                                'namespace': pr.get('metadata', {}).get('namespace', 'unknown'),
                                'runtime_hours': round(runtime.total_seconds() / 3600, 2),
                                'start_time': start_time_str
                            }
                            analysis['pipeline_runs']['long_running'].append(long_running_info)
                    except Exception as e:
                        logger.debug(f"Error parsing start time for PipelineRun: {e}")

        # Analyze TaskRuns
        for tr in task_runs.get('items', []):
            status = tr.get('status', {})
            conditions = status.get('conditions', [])

            # Get latest condition
            if conditions:
                latest_condition = conditions[-1]
                condition_type = latest_condition.get('type', 'Unknown')
                condition_status = latest_condition.get('status', 'Unknown')

                status_key = f"{condition_type}_{condition_status}"
                analysis['task_runs']['by_status'][status_key] = \
                    analysis['task_runs']['by_status'].get(status_key, 0) + 1

                # Check for failures
                if condition_type == 'Succeeded' and condition_status == 'False':
                    failure_info = {
                        'name': tr.get('metadata', {}).get('name', 'unknown'),
                        'namespace': tr.get('metadata', {}).get('namespace', 'unknown'),
                        'reason': latest_condition.get('reason', 'Unknown'),
                        'message': latest_condition.get('message', 'No message')[:200],
                        'start_time': status.get('startTime', 'Unknown')
                    }
                    analysis['task_runs']['recent_failures'].append(failure_info)

        # Aggregate failures by namespace for summary
        pr_failures_by_namespace: Dict[str, int] = {}
        for f in analysis['pipeline_runs']['recent_failures']:
            ns = f.get('namespace', 'unknown')
            pr_failures_by_namespace[ns] = pr_failures_by_namespace.get(ns, 0) + 1

        tr_failures_by_namespace: Dict[str, int] = {}
        for f in analysis['task_runs']['recent_failures']:
            ns = f.get('namespace', 'unknown')
            tr_failures_by_namespace[ns] = tr_failures_by_namespace.get(ns, 0) + 1

        # Store total counts before truncating
        total_pr_failures = len(analysis['pipeline_runs']['recent_failures'])
        total_tr_failures = len(analysis['task_runs']['recent_failures'])
        total_long_running = len(analysis['pipeline_runs']['long_running'])

        # Sort failures by start_time (most recent first) and apply limit
        # Use 'or ""' to handle None values (not just missing keys)
        analysis['pipeline_runs']['recent_failures'].sort(
            key=lambda x: x.get('start_time') or '', reverse=True
        )
        analysis['pipeline_runs']['recent_failures'] = analysis['pipeline_runs']['recent_failures'][:recent_failures_limit]

        analysis['task_runs']['recent_failures'].sort(
            key=lambda x: x.get('start_time') or '', reverse=True
        )
        analysis['task_runs']['recent_failures'] = analysis['task_runs']['recent_failures'][:recent_failures_limit]

        # Sort long_running by runtime (longest first) and apply limit
        analysis['pipeline_runs']['long_running'].sort(
            key=lambda x: x.get('runtime_hours', 0), reverse=True
        )
        analysis['pipeline_runs']['long_running'] = analysis['pipeline_runs']['long_running'][:long_running_limit]

        # Add counts and aggregations
        analysis['pipeline_runs']['total_failures'] = total_pr_failures
        analysis['pipeline_runs']['failures_by_namespace'] = pr_failures_by_namespace
        analysis['pipeline_runs']['total_long_running'] = total_long_running

        analysis['task_runs']['total_failures'] = total_tr_failures
        analysis['task_runs']['failures_by_namespace'] = tr_failures_by_namespace

        # Generate insights
        if total_pr_failures > 0:
            shown = min(total_pr_failures, recent_failures_limit)
            analysis['insights'].append(f"Found {total_pr_failures} failed PipelineRuns (showing top {shown} most recent)")

        if total_tr_failures > 0:
            shown = min(total_tr_failures, recent_failures_limit)
            analysis['insights'].append(f"Found {total_tr_failures} failed TaskRuns (showing top {shown} most recent)")

        if total_long_running > 0:
            shown = min(total_long_running, long_running_limit)
            analysis['insights'].append(
                f"Found {total_long_running} long-running pipelines >1 hour (showing top {shown} longest)"
            )

        # Add summary insight — exclude running pipelines from success rate
        succeeded_prs = analysis['pipeline_runs']['by_status'].get('Succeeded_True', 0)
        running_prs = analysis['pipeline_runs']['by_status'].get('Succeeded_Unknown', 0)
        completed_prs = analysis['pipeline_runs']['total'] - running_prs
        if completed_prs > 0:
            success_rate = (succeeded_prs / completed_prs) * 100
            analysis['insights'].append(f"Pipeline success rate: {success_rate:.1f}% ({running_prs} still running)")
        elif running_prs > 0:
            analysis['insights'].append(f"All {running_prs} pipelines still running — no completed runs to measure")

        logger.info(f"Tekton status analysis complete: {len(analysis['insights'])} insights generated")
        return analysis

    except ApiException as e:
        logger.error(f"API error fetching Tekton resources: {e}")
        return {
            'error': f"Kubernetes API error: {e.reason}",
            'status': e.status,
            'timestamp': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error fetching Tekton resources: {e}", exc_info=True)
        return {
            'error': f"Failed to fetch Tekton resources: {str(e)}",
            'timestamp': datetime.now().isoformat()
        }


@mcp.tool()
async def detect_log_anomalies(
    logs: str,
    baseline_patterns: Optional[List[str]] = None,
    severity_threshold: str = "medium"
) -> Dict[str, Any]:
    """
    Detect anomalies in log data using error frequency, pattern repetition, and timestamp analysis.

    Args:
        logs: Raw log content (newline-separated entries).
        baseline_patterns: Optional expected error patterns for comparison.
        severity_threshold: "low" (most sensitive), "medium", or "high" (least sensitive).

    Returns:
        Dict[str, Any]: Keys: anomaly_detected (bool), anomaly_details, analysis_summary.
    """
    logger.info(f"Starting log anomaly detection with severity threshold: {severity_threshold}")

    if not logs or logs.strip() == "":
        logger.warning("Empty or null logs provided for anomaly detection")
        return {
            "anomaly_detected": False,
            "anomaly_details": None,
            "analysis_summary": "No logs provided for analysis"
        }

    try:
        # Normalize escaped newlines from MCP/JSON transport (literal \n -> actual newline)
        import re as _re
        normalized_logs = _re.sub(
            r'\\n(?=\d{4}-\d{2}-\d{2}|level=|"level"|time=|"ts"|msg=|http:)',
            '\n', logs
        )
        if '\n' not in normalized_logs and '\\n' in normalized_logs:
            normalized_logs = normalized_logs.replace('\\n', '\n')

        # Parse logs into lines
        log_lines = [line.strip() for line in normalized_logs.split('\n') if line.strip()]
        total_lines = len(log_lines)

        if total_lines == 0:
            return {
                "anomaly_detected": False,
                "anomaly_details": None,
                "analysis_summary": "No valid log lines found"
            }

        logger.info(f"Analyzing {total_lines} log lines for anomalies")

        # Initialize anomaly detection results
        anomalies = []

        # Define severity thresholds
        thresholds = {
            "low": {"error_rate": 0.05, "warn_rate": 0.30, "repetition_rate": 0.3, "time_gap": 300},
            "medium": {"error_rate": 0.1, "warn_rate": 0.60, "repetition_rate": 0.5, "time_gap": 180},
            "high": {"error_rate": 0.2, "warn_rate": 0.90, "repetition_rate": 0.7, "time_gap": 60}
        }

        threshold_config = thresholds.get(severity_threshold, thresholds["medium"])

        # 1. Analyze error frequency patterns
        # First-match-wins: more specific patterns before generic ones
        error_pattern_map = {
            r"(?i)\b(out\s+of\s+memory|memory\s+limit|oom(?:killed)?)\b": "memory",
            r"(?i)\b(timeout)\b": "timeout",
            r"(?i)\b(connection\s+refused|connection\s+reset)\b": "connection",
            r"(?i)\b(permission\s+denied|access\s+denied|unauthorized)\b": "permission",
            r"(?i)\b(not\s+found|missing|invalid|corrupt)\b": "not_found",
            r"(?i)\b(error|exception|failed|fatal|panic|critical)\b": "error",
        }

        error_counts = {}
        error_lines = []
        warn_lines = 0

        for i, line in enumerate(log_lines):
            matched = False
            for pattern, label in error_pattern_map.items():
                if re.search(pattern, line):
                    error_lines.append((i, line))
                    error_counts[label] = error_counts.get(label, 0) + 1
                    matched = True
                    break
            if not matched and re.search(r"(?i)\bwarn(?:ing)?\b", line):
                warn_lines += 1

        unique_error_line_indices = set(line[0] for line in error_lines)
        error_rate = len(unique_error_line_indices) / total_lines
        if error_rate > threshold_config["error_rate"]:
            anomalies.append({
                "type": "high_error_rate",
                "severity": "high" if error_rate > 0.3 else "medium",
                "description": f"High error rate detected: {error_rate:.2%} ({len(unique_error_line_indices)}/{total_lines} lines)",
                "details": {
                    "error_rate": error_rate,
                    "error_patterns": error_counts,
                    "sample_errors": [line[1][:200] for line in error_lines[:5]]  # Truncate long lines
                }
            })

        warn_rate = warn_lines / total_lines if total_lines else 0
        if warn_rate > threshold_config["warn_rate"]:
            anomalies.append({
                "type": "high_warn_rate",
                "severity": "medium" if warn_rate > 0.5 else "low",
                "description": f"High warning rate detected: {warn_rate:.2%} ({warn_lines}/{total_lines} lines)",
                "details": {"warn_rate": warn_rate, "warn_count": warn_lines}
            })

        # 2. Detect repetitive log patterns (potential loops or spam)
        line_frequency = {}
        for line in log_lines:
            # Normalize line by removing timestamps and variable data
            normalized = re.sub(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}', 'TIMESTAMP', line)
            normalized = re.sub(r'\b\d+\b', 'NUMBER', normalized)
            normalized = re.sub(r'\b[a-f0-9]{8,}\b', 'HASH', normalized)

            line_frequency[normalized] = line_frequency.get(normalized, 0) + 1

        # Find highly repetitive patterns
        for pattern, count in line_frequency.items():
            repetition_rate = count / total_lines
            if repetition_rate > threshold_config["repetition_rate"] and count > 10:
                anomalies.append({
                    "type": "repetitive_pattern",
                    "severity": "high" if repetition_rate > 0.8 else "medium",
                    "description": f"Highly repetitive log pattern detected: {repetition_rate:.2%} of logs ({count} occurrences)",
                    "details": {
                        "pattern": pattern[:200],
                        "occurrence_count": count,
                        "repetition_rate": repetition_rate
                    }
                })

        # 3. Analyze timestamp patterns for gaps or bursts
        timestamps = []
        for line in log_lines:
            # Extract timestamps
            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})', line)
            if timestamp_match:
                try:
                    ts = datetime.fromisoformat(timestamp_match.group(1).replace('T', ' '))
                    timestamps.append(ts)
                except Exception:
                    continue

        if len(timestamps) > 2:
            # Calculate time gaps between consecutive log entries
            time_gaps = []
            for i in range(1, len(timestamps)):
                gap = (timestamps[i] - timestamps[i-1]).total_seconds()
                time_gaps.append(gap)

            # Detect unusual time gaps
            if time_gaps:
                avg_gap = sum(time_gaps) / len(time_gaps)
                max_gap = max(time_gaps)

                if max_gap > threshold_config["time_gap"] and max_gap > avg_gap * 10:
                    anomalies.append({
                        "type": "time_gap_anomaly",
                        "severity": "medium",
                        "description": f"Unusual time gap detected: {max_gap:.0f} seconds (avg: {avg_gap:.1f}s)",
                        "details": {
                            "max_gap_seconds": max_gap,
                            "average_gap_seconds": avg_gap,
                            "total_timestamps": len(timestamps)
                        }
                    })

                # Detect log bursts (many logs in short time)
                burst_threshold = 50  # logs per minute
                one_minute_windows = {}
                for ts in timestamps:
                    minute_key = ts.replace(second=0, microsecond=0)
                    one_minute_windows[minute_key] = one_minute_windows.get(minute_key, 0) + 1

                max_burst = max(one_minute_windows.values()) if one_minute_windows else 0
                if max_burst > burst_threshold:
                    anomalies.append({
                        "type": "log_burst",
                        "severity": "medium",
                        "description": f"Log burst detected: {max_burst} logs in one minute",
                        "details": {
                            "max_logs_per_minute": max_burst,
                            "burst_threshold": burst_threshold
                        }
                    })

        # 4. Check against baseline patterns if provided
        if baseline_patterns:
            baseline_set = set(baseline_patterns)
            current_patterns = set(error_counts.keys())

            new_patterns = current_patterns - baseline_set
            missing_patterns = baseline_set - current_patterns

            if new_patterns:
                anomalies.append({
                    "type": "new_error_patterns",
                    "severity": "medium",
                    "description": f"New error patterns not seen in baseline: {', '.join(list(new_patterns)[:5])}",
                    "details": {
                        "new_patterns": list(new_patterns),
                        "baseline_patterns": baseline_patterns
                    }
                })

            if missing_patterns and len(missing_patterns) > len(baseline_patterns) * 0.5:
                anomalies.append({
                    "type": "missing_expected_patterns",
                    "severity": "low",
                    "description": f"Expected patterns missing from logs: {', '.join(list(missing_patterns)[:5])}",
                    "details": {
                        "missing_patterns": list(missing_patterns)
                    }
                })

        # 5. Detect unusual log levels distribution
        log_levels = {"debug": 0, "info": 0, "warn": 0, "error": 0, "fatal": 0}
        for line in log_lines:
            line_lower = line.lower()
            if re.search(r'\b(debug|trace)\b', line_lower):
                log_levels["debug"] += 1
            elif re.search(r'\binfo\b', line_lower):
                log_levels["info"] += 1
            elif re.search(r'\b(warn|warning)\b', line_lower):
                log_levels["warn"] += 1
            elif re.search(r'\b(error|err)\b', line_lower):
                log_levels["error"] += 1
            elif re.search(r'\b(fatal|critical|panic)\b', line_lower):
                log_levels["fatal"] += 1

        total_leveled = sum(log_levels.values())
        if total_leveled > 0:
            error_plus_fatal = log_levels["error"] + log_levels["fatal"]
            severe_ratio = error_plus_fatal / total_leveled

            if severe_ratio > 0.5:  # More than 50% severe logs
                anomalies.append({
                    "type": "unusual_log_level_distribution",
                    "severity": "high",
                    "description": f"High proportion of severe logs: {severe_ratio:.2%}",
                    "details": {
                        "log_level_distribution": log_levels,
                        "severe_log_ratio": severe_ratio
                    }
                })

        # Compile results
        anomaly_detected = len(anomalies) > 0

        if anomaly_detected:
            # Sort anomalies by severity
            severity_order = {"high": 3, "medium": 2, "low": 1}
            anomalies.sort(key=lambda x: severity_order.get(x["severity"], 0), reverse=True)

            anomaly_details = {
                "total_anomalies": len(anomalies),
                "anomalies": anomalies,
                "log_statistics": {
                    "total_lines": total_lines,
                    "error_rate": error_rate,
                    "unique_patterns": len(line_frequency),
                    "timestamp_coverage": len(timestamps) / total_lines if total_lines > 0 else 0
                }
            }

            analysis_summary = f"Detected {len(anomalies)} anomalies in {total_lines} log lines. "
            analysis_summary += f"Highest severity: {anomalies[0]['severity']}. "
            analysis_summary += f"Primary issues: {', '.join([a['type'] for a in anomalies[:3]])}"
        else:
            anomaly_details = None
            analysis_summary = f"No anomalies detected in {total_lines} log lines. Log patterns appear normal."

        logger.info(f"Anomaly detection completed. Found {len(anomalies)} anomalies")

        return {
            "anomaly_detected": anomaly_detected,
            "anomaly_details": anomaly_details,
            "analysis_summary": analysis_summary
        }

    except Exception as e:
        logger.error(f"Error during log anomaly detection: {str(e)}", exc_info=True)
        return {
            "anomaly_detected": False,
            "anomaly_details": None,
            "analysis_summary": f"Analysis failed due to error: {str(e)}"
        }


@mcp.tool()
async def search_resources_by_labels(
    resource_types: List[str],
    label_selectors: List[Dict[str, Any]],
    namespaces: Optional[List[str]] = None,
    limit_per_type: int = 100,
    include_metadata_only: bool = False,
    include_status: bool = True,
    sort_by: str = "creation_time",
    sort_order: str = "desc"
) -> Dict[str, Any]:
    """
    Search Kubernetes resources by labels across multiple resource types and namespaces.

    Args:
        resource_types: Types to search (e.g., ["pods", "services", "deployments"]).
        label_selectors: Criteria list [{"key": str, "value": str, "operator": "equals|exists|not_equals|in|not_in"}].
        namespaces: Namespaces to search (default: all).
        limit_per_type: Max results per type (default: 100).
        include_metadata_only: Return only metadata (default: False).
        include_status: Include status info (default: True).
        sort_by: "name", "namespace", "creation_time", or "labels" (default: "creation_time").
        sort_order: "asc" or "desc" (default: "desc").

    Returns:
        Dict: Search results with resource details, analysis, and recommendations.
    """
    start_time = time.time()
    logger.info(f"Starting Kubernetes resource search by labels for types: {resource_types}")

    try:
        # Build label selector string
        label_selector = build_advanced_label_selector(label_selectors)
        logger.info(f"Built label selector: {label_selector}")

        # Get accessible namespaces if not specified
        if namespaces is None:
            try:
                ns_response = k8s_core_api.list_namespace()
                accessible_namespaces = [ns.metadata.name for ns in ns_response.items]
                logger.info(f"Found {len(accessible_namespaces)} accessible namespaces")
            except ApiException as e:
                logger.warning(f"Could not list namespaces: {e.reason}. Using default namespace")
                accessible_namespaces = ["default"]
        else:
            accessible_namespaces = namespaces

        all_resources = []
        resource_type_counts = {}
        error_details = []

        # Search each resource type
        for resource_type in resource_types:
            logger.info(f"Searching {resource_type} resources")
            type_count = 0

            try:
                api_info = get_resource_api_info(resource_type)
                if not api_info:
                    error_details.append({
                        "resource_type": resource_type,
                        "namespace": "all",
                        "error_message": f"Unsupported resource type: {resource_type}",
                        "error_code": "UNSUPPORTED_RESOURCE_TYPE"
                    })
                    continue

                resources_found = []

                if api_info.get("namespaced", True):
                    # Search namespaced resources
                    for namespace in accessible_namespaces:
                        try:
                            if api_info["api"] == "core_v1":
                                api_client = k8s_core_api
                                method = getattr(api_client, api_info["method"])
                                response = method(
                                    namespace=namespace,
                                    label_selector=label_selector,
                                    limit=limit_per_type
                                )
                            elif api_info["api"] == "apps_v1":
                                api_client = k8s_apps_api
                                method = getattr(api_client, api_info["method"])
                                response = method(
                                    namespace=namespace,
                                    label_selector=label_selector,
                                    limit=limit_per_type
                                )
                            elif api_info["api"] == "batch_v1":
                                api_client = k8s_batch_api
                                method = getattr(api_client, api_info["method"])
                                response = method(
                                    namespace=namespace,
                                    label_selector=label_selector,
                                    limit=limit_per_type
                                )
                            elif api_info["api"] == "custom":
                                response = k8s_custom_api.list_namespaced_custom_object(
                                    group=api_info["group"],
                                    version=api_info["version"],
                                    namespace=namespace,
                                    plural=api_info["plural"],
                                    label_selector=label_selector,
                                    limit=limit_per_type
                                )

                            # Custom objects return dicts, native K8s objects have items attribute
                            if isinstance(response, dict):
                                items = response.get('items', [])
                            elif hasattr(response, 'items'):
                                items = response.items
                            else:
                                items = []

                            for item in items:
                                if hasattr(item, 'to_dict'):
                                    resource_dict = item.to_dict()
                                else:
                                    resource_dict = item

                                processed_resource = extract_resource_info(
                                    resource_dict,
                                    not include_metadata_only,
                                    include_status,
                                    resource_type_hint=resource_type
                                )
                                resources_found.append(processed_resource)
                                type_count += 1

                        except ApiException as e:
                            if e.status not in [403, 404]:
                                error_details.append({
                                    "resource_type": resource_type,
                                    "namespace": namespace,
                                    "error_message": f"API error: {e.reason}",
                                    "error_code": str(e.status)
                                })
                        except Exception as e:
                            error_details.append({
                                "resource_type": resource_type,
                                "namespace": namespace,
                                "error_message": str(e),
                                "error_code": "UNEXPECTED_ERROR"
                            })
                else:
                    # Search cluster-scoped resources
                    try:
                        if api_info["api"] == "core_v1":
                            api_client = k8s_core_api
                            method = getattr(api_client, api_info["method"])
                            response = method(
                                label_selector=label_selector,
                                limit=limit_per_type
                            )

                        # Custom objects return dicts, native K8s objects have items attribute
                        if isinstance(response, dict):
                            items = response.get('items', [])
                        elif hasattr(response, 'items'):
                            items = response.items
                        else:
                            items = []

                        for item in items:
                            if hasattr(item, 'to_dict'):
                                resource_dict = item.to_dict()
                            else:
                                resource_dict = item

                            processed_resource = extract_resource_info(
                                resource_dict,
                                not include_metadata_only,
                                include_status,
                                resource_type_hint=resource_type
                            )
                            resources_found.append(processed_resource)
                            type_count += 1

                    except ApiException as e:
                        error_details.append({
                            "resource_type": resource_type,
                            "namespace": "cluster-scoped",
                            "error_message": f"API error: {e.reason}",
                            "error_code": str(e.status)
                        })
                    except Exception as e:
                        error_details.append({
                            "resource_type": resource_type,
                            "namespace": "cluster-scoped",
                            "error_message": str(e),
                            "error_code": "UNEXPECTED_ERROR"
                        })

                all_resources.extend(resources_found)
                resource_type_counts[resource_type] = type_count
                logger.info(f"Found {type_count} {resource_type} resources")

            except Exception as e:
                logger.error(f"Error searching {resource_type}: {str(e)}")
                error_details.append({
                    "resource_type": resource_type,
                    "namespace": "all",
                    "error_message": str(e),
                    "error_code": "SEARCH_ERROR"
                })
                resource_type_counts[resource_type] = 0

        # Sort resources
        sorted_resources = sort_resources(all_resources, sort_by, sort_order)

        # Perform analysis
        label_analysis = analyze_labels(sorted_resources)
        namespace_distribution = calculate_namespace_distribution(sorted_resources)

        # Generate recommendations
        recommendations = []
        if len(error_details) > 0:
            recommendations.append({
                "type": "permission_check",
                "description": "Some resources could not be accessed due to permission errors",
                "affected_resources": [err["resource_type"] for err in error_details],
                "suggested_actions": ["Check RBAC permissions", "Verify cluster connectivity", "Confirm resource types exist"]
            })

        if len(sorted_resources) == 0:
            recommendations.append({
                "type": "no_results",
                "description": "No resources found matching the specified label selectors",
                "affected_resources": resource_types,
                "suggested_actions": ["Verify label selector syntax", "Check if resources exist with different labels", "Try broader search criteria"]
            })

        # Calculate duration
        duration_ms = round((time.time() - start_time) * 1000, 2)

        # Build response
        response = {
            "search_summary": {
                "total_resources_found": len(sorted_resources),
                "resource_type_counts": resource_type_counts,
                "namespaces_searched": accessible_namespaces,
                "search_criteria": {
                    "label_selectors": label_selectors,
                    "resource_types": resource_types
                },
                "search_duration_ms": duration_ms
            },
            "resources": sorted_resources,
            "label_analysis": label_analysis,
            "namespace_distribution": namespace_distribution,
            "error_details": error_details,
            "recommendations": recommendations
        }

        logger.info(f"Resource search completed. Found {len(sorted_resources)} resources in {duration_ms}ms")
        return response

    except Exception as e:
        error_msg = f"Unexpected error during resource search: {str(e)}"
        logger.error(error_msg, exc_info=True)

        return {
            "search_summary": {
                "total_resources_found": 0,
                "resource_type_counts": {},
                "namespaces_searched": [],
                "search_criteria": {
                    "label_selectors": label_selectors,
                    "resource_types": resource_types
                },
                "search_duration_ms": round((time.time() - start_time) * 1000, 2)
            },
            "resources": [],
            "label_analysis": {
                "common_labels": [],
                "unique_labels": [],
                "label_patterns": []
            },
            "namespace_distribution": [],
            "error_details": [{
                "resource_type": "system",
                "namespace": "all",
                "error_message": error_msg,
                "error_code": "SYSTEM_ERROR"
            }],
            "recommendations": [{
                "type": "system_error",
                "description": "A system error occurred during the search",
                "affected_resources": resource_types,
                "suggested_actions": ["Check system logs", "Verify cluster connectivity", "Retry the search"]
            }]
        }


# ============================================================================
# PROMETHEUS QUERY HELPER FUNCTIONS
# ============================================================================


async def _get_k8s_bearer_token() -> Optional[str]:
    """
    Get a fresh bearer token for Prometheus authentication.

    Fallback chain:
    1. Run `oc whoami -t` for a fresh OpenShift token (handles token refresh)
    2. Re-read kubeconfig for the current token (covers non-oc environments)
    3. Extract from in-memory Kubernetes client config (last resort)
    4. Read from ServiceAccount token file (in-cluster)
    5. Environment variable (PROMETHEUS_TOKEN, OPENSHIFT_TOKEN, OC_TOKEN)
    """
    # Method 1: Get fresh token via `oc whoami -t` (most reliable for OpenShift)
    try:
        import subprocess
        result = subprocess.run(
            ["oc", "whoami", "-t"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0 and result.stdout.strip():
            token = result.stdout.strip()
            logger.debug("Obtained fresh bearer token via 'oc whoami -t'")
            return token
    except (FileNotFoundError, subprocess.TimeoutExpired):
        logger.debug("oc CLI not available or timed out")
    except Exception as e:
        logger.debug(f"Could not get token via oc: {e}")

    # Method 2: Re-read kubeconfig file for current token
    try:
        from kubernetes import config as k8s_config_module
        from kubernetes.client import Configuration, ApiClient

        loader = k8s_config_module.kube_config._get_kube_config_loader()
        loader.load_and_set(Configuration())
        fresh_config = Configuration.get_default_copy()
        if fresh_config.api_key and fresh_config.api_key.get('authorization'):
            auth_header = fresh_config.api_key['authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header[7:]
                logger.debug("Obtained bearer token from re-read kubeconfig")
                return token
    except Exception as e:
        logger.debug(f"Could not re-read kubeconfig: {e}")

    # Method 3: Extract from in-memory Kubernetes client config (may be stale)
    try:
        from kubernetes.client import Configuration

        k8s_config = Configuration.get_default_copy()

        if k8s_config.api_key and k8s_config.api_key.get('authorization'):
            auth_header = k8s_config.api_key['authorization']
            if auth_header.startswith('Bearer '):
                token = auth_header[7:]
                logger.debug("Using bearer token from in-memory k8s client config (may be stale)")
                return token

    except Exception as e:
        logger.debug(f"Could not extract token from k8s client config: {e}")

    # Method 4: Read from ServiceAccount token file (in-cluster scenario)
    SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    try:
        if os.path.exists(SA_TOKEN_PATH):
            with open(SA_TOKEN_PATH, 'r') as f:
                token = f.read().strip()
                if token:
                    logger.info("Successfully obtained token from ServiceAccount token file")
                    return token
    except Exception as e:
        logger.debug(f"Could not read ServiceAccount token: {e}")

    # Method 5: Environment variable fallback
    token = os.getenv("PROMETHEUS_TOKEN") or os.getenv("OPENSHIFT_TOKEN") or os.getenv("OC_TOKEN")
    if token:
        logger.info("Using token from environment variable")
        return token

    logger.error("Could not obtain authentication token from any source")
    return None


async def _discover_prometheus_via_routes() -> Optional[str]:
    """
    Discover Prometheus endpoint via OpenShift Routes.

    Looks for routes in openshift-monitoring namespace:
    - prometheus-k8s (primary Prometheus)
    - thanos-querier (Thanos frontend)
    """
    if not k8s_custom_api:
        logger.debug("CustomObjectsApi not available for route discovery")
        return None

    try:
        # Query routes in openshift-monitoring namespace
        routes = k8s_custom_api.list_namespaced_custom_object(
            group="route.openshift.io",
            version="v1",
            namespace="openshift-monitoring",
            plural="routes"
        )

        # Priority order: prefer Thanos (unified, deduplicated view) over direct Prometheus
        preferred_routes = ["thanos-querier", "prometheus-k8s"]

        route_items = routes.get("items", [])
        route_map = {r.get("metadata", {}).get("name"): r for r in route_items}

        for route_name in preferred_routes:
            if route_name in route_map:
                route = route_map[route_name]
                spec = route.get("spec", {})
                host = spec.get("host")

                if host:
                    # Determine protocol (check TLS termination)
                    tls = spec.get("tls")
                    protocol = "https" if tls else "http"
                    endpoint = f"{protocol}://{host}"

                    logger.info(f"Discovered Prometheus via OpenShift route '{route_name}': {endpoint}")
                    return endpoint

        # Fallback: any route with 'prometheus' in the name
        for route in route_items:
            route_name = route.get("metadata", {}).get("name", "")
            if "prometheus" in route_name.lower():
                host = route.get("spec", {}).get("host")
                if host:
                    tls = route.get("spec", {}).get("tls")
                    protocol = "https" if tls else "http"
                    endpoint = f"{protocol}://{host}"
                    logger.info(f"Discovered Prometheus via route '{route_name}': {endpoint}")
                    return endpoint

    except client.rest.ApiException as e:
        if e.status == 404:
            logger.debug("OpenShift routes API not available (not an OpenShift cluster)")
        else:
            logger.warning(f"Error querying OpenShift routes: {e}")
    except Exception as e:
        logger.warning(f"Error discovering Prometheus via routes: {e}")

    return None


async def _discover_prometheus_via_operator_crd() -> Optional[str]:
    """
    Discover Prometheus via Prometheus Operator CRDs.

    Looks for:
    - Prometheus custom resources (monitoring.coreos.com/v1)
    - Associated services
    """
    if not k8s_custom_api or not k8s_core_api:
        logger.debug("API clients not available for Prometheus Operator CRD discovery")
        return None

    try:
        # List all Prometheus custom resources cluster-wide
        prometheus_resources = k8s_custom_api.list_cluster_custom_object(
            group="monitoring.coreos.com",
            version="v1",
            plural="prometheuses"
        )

        for prom in prometheus_resources.get("items", []):
            metadata = prom.get("metadata", {})
            name = metadata.get("name")
            namespace = metadata.get("namespace")

            if not name or not namespace:
                continue

            # The Prometheus Operator creates a service with pattern: prometheus-<name>
            service_name = f"prometheus-{name}"

            try:
                service = k8s_core_api.read_namespaced_service(
                    name=service_name,
                    namespace=namespace
                )

                # Get service port (default Prometheus port is 9090)
                ports = service.spec.ports or []
                port = 9090
                for p in ports:
                    if p.name in ["web", "http", "prometheus"] or p.port == 9090:
                        port = p.port
                        break

                # Construct in-cluster service URL
                endpoint = f"http://{service_name}.{namespace}.svc.cluster.local:{port}"
                logger.info(f"Discovered Prometheus via Operator CRD: {endpoint}")
                return endpoint

            except client.rest.ApiException as e:
                logger.debug(f"Could not find service for Prometheus '{name}': {e}")
                continue

    except client.rest.ApiException as e:
        if e.status == 404:
            logger.debug("Prometheus Operator CRDs not available")
        else:
            logger.warning(f"Error querying Prometheus CRDs: {e}")
    except Exception as e:
        logger.warning(f"Error discovering Prometheus via Operator CRD: {e}")

    return None


async def _discover_prometheus_via_services() -> Optional[str]:
    """
    Discover Prometheus by searching for services with prometheus-related labels/names.

    Search criteria:
    - Services with 'prometheus' in name
    - Services with label 'app=prometheus' or 'app.kubernetes.io/name=prometheus'
    - Services exposing port 9090
    """
    if not k8s_core_api:
        logger.debug("CoreV1Api not available for service discovery")
        return None

    try:
        # Search common monitoring namespaces first
        monitoring_namespaces = [
            "openshift-monitoring",
            "monitoring",
            "prometheus",
            "kube-prometheus",
            "observability"
        ]

        # First, try specific namespaces
        for namespace in monitoring_namespaces:
            try:
                services = k8s_core_api.list_namespaced_service(namespace=namespace)

                # Prioritize actual Prometheus server services (not alertmanager, pushgateway, etc.)
                # Priority: prometheus-server > prometheus-k8s > prometheus > any with prometheus in name
                priority_names = ["prometheus-server", "prometheus-k8s", "prometheus"]
                excluded_suffixes = ["-alertmanager", "-pushgateway", "-node-exporter",
                                   "-kube-state-metrics", "-headless", "-operated"]

                # First pass: look for priority names
                for priority_name in priority_names:
                    for service in services.items:
                        name = service.metadata.name
                        if name == priority_name:
                            ports = service.spec.ports or []
                            port = 9090
                            for p in ports:
                                # Accept common Prometheus ports: 9090, 80, 443
                                if p.port in [9090, 80, 443] or (p.name and p.name in ["web", "http", "https"]):
                                    port = p.port
                                    break
                            endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                            logger.info(f"Discovered Prometheus service (priority match): {endpoint}")
                            return endpoint

                # Second pass: look for services with 'prometheus' but exclude non-server services
                for service in services.items:
                    name = service.metadata.name
                    if "prometheus" in name.lower():
                        # Skip non-server services
                        if any(name.lower().endswith(suffix) for suffix in excluded_suffixes):
                            continue

                        ports = service.spec.ports or []
                        port = 9090
                        for p in ports:
                            if p.port in [9090, 80, 443] or (p.name and p.name in ["web", "http", "https"]):
                                port = p.port
                                break

                        endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                        logger.info(f"Discovered Prometheus service: {endpoint}")
                        return endpoint

            except client.rest.ApiException as e:
                if e.status != 404:
                    logger.debug(f"Namespace '{namespace}' not accessible: {e}")
                continue

        # Try cluster-wide search with label selectors
        label_selectors = [
            "app=prometheus",
            "app.kubernetes.io/name=prometheus",
            "app.kubernetes.io/component=prometheus"
        ]

        for label_selector in label_selectors:
            try:
                services = k8s_core_api.list_service_for_all_namespaces(
                    label_selector=label_selector
                )

                if services.items:
                    service = services.items[0]  # Take first match
                    name = service.metadata.name
                    namespace = service.metadata.namespace

                    ports = service.spec.ports or []
                    port = 9090
                    for p in ports:
                        if p.port == 9090 or (p.name and p.name in ["web", "http"]):
                            port = p.port
                            break

                    endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                    logger.info(f"Discovered Prometheus via label selector '{label_selector}': {endpoint}")
                    return endpoint

            except client.rest.ApiException as e:
                logger.debug(f"Error with label selector '{label_selector}': {e}")
                continue

    except Exception as e:
        logger.warning(f"Error discovering Prometheus via services: {e}")

    return None


async def _discover_thanos_via_services() -> Optional[str]:
    """
    Discover Thanos Query endpoint by searching for Thanos services.

    Thanos Query implements the Prometheus HTTP API, so once discovered
    it can be used interchangeably with Prometheus for PromQL queries.

    Search criteria:
    - Services with 'thanos-query' or 'thanos-querier' in name
    - Services with Thanos-related labels
    - Common Thanos Query ports: 9090, 10902 (HTTP), 9091
    """
    if not k8s_core_api:
        logger.debug("CoreV1Api not available for Thanos service discovery")
        return None

    try:
        monitoring_namespaces = [
            "openshift-monitoring",
            "monitoring",
            "thanos",
            "observability",
            "kube-prometheus",
        ]

        priority_names = ["thanos-query-frontend", "thanos-querier", "thanos-query"]
        thanos_http_ports = [9090, 9091, 80, 443]

        # First pass: check known monitoring namespaces for priority service names
        for namespace in monitoring_namespaces:
            try:
                services = k8s_core_api.list_namespaced_service(namespace=namespace)

                for priority_name in priority_names:
                    for service in services.items:
                        if service.metadata.name == priority_name:
                            ports = service.spec.ports or []
                            port = 9090
                            for p in ports:
                                if p.port in thanos_http_ports or (p.name and p.name in ["http", "web", "https"]):
                                    port = p.port
                                    break
                            endpoint = f"http://{priority_name}.{namespace}.svc.cluster.local:{port}"
                            logger.info(f"Discovered Thanos Query service (priority match): {endpoint}")
                            return endpoint

                # Second pass: any service with 'thanos' and 'query' in the name
                for service in services.items:
                    name = service.metadata.name.lower()
                    if "thanos" in name and ("query" in name or "querier" in name):
                        ports = service.spec.ports or []
                        port = 9090
                        for p in ports:
                            if p.port in thanos_http_ports or (p.name and p.name in ["http", "web", "https"]):
                                port = p.port
                                break
                        endpoint = f"http://{service.metadata.name}.{namespace}.svc.cluster.local:{port}"
                        logger.info(f"Discovered Thanos Query service: {endpoint}")
                        return endpoint

            except client.rest.ApiException as e:
                if e.status != 404:
                    logger.debug(f"Namespace '{namespace}' not accessible for Thanos discovery: {e}")
                continue

        # Cluster-wide label-based search
        label_selectors = [
            "app.kubernetes.io/name=thanos-query",
            "app.kubernetes.io/component=query,app.kubernetes.io/name=thanos",
            "app=thanos-query",
            "app=thanos-querier",
        ]

        for label_selector in label_selectors:
            try:
                services = k8s_core_api.list_service_for_all_namespaces(
                    label_selector=label_selector
                )
                if services.items:
                    service = services.items[0]
                    name = service.metadata.name
                    namespace = service.metadata.namespace
                    ports = service.spec.ports or []
                    port = 9090
                    for p in ports:
                        if p.port in thanos_http_ports or (p.name and p.name in ["http", "web"]):
                            port = p.port
                            break
                    endpoint = f"http://{name}.{namespace}.svc.cluster.local:{port}"
                    logger.info(f"Discovered Thanos Query via label selector '{label_selector}': {endpoint}")
                    return endpoint

            except client.rest.ApiException as e:
                logger.debug(f"Error with Thanos label selector '{label_selector}': {e}")
                continue

    except Exception as e:
        logger.warning(f"Error discovering Thanos Query via services: {e}")

    return None


async def _discover_prometheus_endpoint(cluster_override: Optional[str] = None) -> tuple:
    """
    Discover Prometheus or Thanos Query endpoint using multiple strategies.

    Returns a (url, endpoint_type) tuple where endpoint_type is "thanos" or "prometheus".
    Thanos Query is preferred when available since it provides a unified, deduplicated view.

    Priority order:
    0. THANOS_URL env var (explicit Thanos override)
    1. PROMETHEUS_URL env var (explicit Prometheus override)
    2. Predefined cluster endpoints
    3. Cache
    4. Auto-discovery (Thanos first, then Prometheus)
    5. Predefined fallback endpoints

    Args:
        cluster_override: Optional cluster name for predefined endpoint lookup

    Returns:
        (endpoint_url, endpoint_type) tuple, or (None, None) if not found
    """
    # 0. Check for THANOS_URL environment variable (highest priority)
    env_thanos_url = os.getenv("THANOS_URL")
    if env_thanos_url:
        logger.info(f"Using Thanos endpoint from THANOS_URL environment variable: {env_thanos_url}")
        return (env_thanos_url, "thanos")

    # 1. Check for PROMETHEUS_URL environment variable
    env_prometheus_url = os.getenv("PROMETHEUS_URL")
    if env_prometheus_url:
        logger.info(f"Using Prometheus endpoint from PROMETHEUS_URL environment variable: {env_prometheus_url}")
        return (env_prometheus_url, "prometheus")

    # 2. Check for cluster override in predefined endpoints
    if cluster_override and cluster_override in OPENSHIFT_PROMETHEUS_ENDPOINTS:
        endpoint = OPENSHIFT_PROMETHEUS_ENDPOINTS[cluster_override].get("url")
        if endpoint:
            endpoint_type = OPENSHIFT_PROMETHEUS_ENDPOINTS[cluster_override].get("type", "prometheus")
            logger.info(f"Using predefined {endpoint_type} endpoint for cluster '{cluster_override}': {endpoint}")
            return (endpoint, endpoint_type)

    # 3. Check cache (now returns (url, type) tuple or None)
    cache_key = cluster_override or "default"
    cached = _prometheus_endpoint_cache.get(cache_key)
    if cached:
        logger.info(f"Using cached {cached[1]} endpoint: {cached[0]}")
        return cached

    # 4. Discovery chain - order depends on runtime environment
    # Thanos discovery comes first: if Thanos is deployed, it's the intended query interface.
    # Each entry: (method_name, discovery_func, endpoint_type)
    if _is_running_in_cluster():
        discovery_methods = [
            ("Thanos Query Services", _discover_thanos_via_services, "thanos"),
            ("Prometheus Services", _discover_prometheus_via_services, "prometheus"),
            ("Prometheus Operator CRD", _discover_prometheus_via_operator_crd, "prometheus"),
            ("OpenShift Routes", _discover_prometheus_via_routes, None),  # type detected from route name
        ]
    else:
        discovery_methods = [
            ("OpenShift Routes", _discover_prometheus_via_routes, None),  # type detected from route name
            ("Thanos Query Services", _discover_thanos_via_services, "thanos"),
            ("Prometheus Operator CRD", _discover_prometheus_via_operator_crd, "prometheus"),
            ("Prometheus Services", _discover_prometheus_via_services, "prometheus"),
        ]

    for method_name, discovery_func, method_type in discovery_methods:
        try:
            logger.debug(f"Attempting discovery via: {method_name}")
            endpoint = await discovery_func()
            if endpoint:
                # For OpenShift Routes, detect type from the discovered endpoint/route name
                if method_type is None:
                    endpoint_type = "thanos" if "thanos" in endpoint.lower() else "prometheus"
                else:
                    endpoint_type = method_type
                _prometheus_endpoint_cache.set(endpoint, cache_key, endpoint_type=endpoint_type)
                return (endpoint, endpoint_type)
        except Exception as e:
            logger.warning(f"Discovery method '{method_name}' failed: {e}")
            continue

    # 5. Fallback to predefined endpoints (try all except 'local')
    for cluster_name, config in OPENSHIFT_PROMETHEUS_ENDPOINTS.items():
        if cluster_name != "local":
            endpoint = config.get("url")
            if endpoint:
                endpoint_type = config.get("type", "prometheus")
                logger.info(f"Using fallback {endpoint_type} endpoint: {endpoint}")
                _prometheus_endpoint_cache.set(endpoint, cache_key, endpoint_type=endpoint_type)
                return (endpoint, endpoint_type)

    logger.error("Could not discover Prometheus/Thanos endpoint via any method")
    return (None, None)


def _parse_time_parameter(time_param: str) -> str:
    """Parse time parameter to Unix timestamp for Prometheus API."""
    try:
        # If it's already a Unix timestamp
        if time_param.isdigit():
            return time_param

        # Try to parse ISO 8601 format
        if "T" in time_param:
            dt = datetime.fromisoformat(time_param.replace("Z", "+00:00"))
            return str(int(dt.timestamp()))

        # Try to parse other common formats
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                dt = datetime.strptime(time_param, fmt)
                return str(int(dt.timestamp()))
            except ValueError:
                continue

        # If all else fails, return as-is
        return time_param

    except Exception as e:
        logger.warning(f"Error parsing time parameter '{time_param}': {e}")
        return time_param


async def _execute_prometheus_query_internal(
    query: str,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Internal helper to execute Prometheus/Thanos queries from within other tools.

    Args:
        query: PromQL query string
        timeout: Query timeout in seconds

    Returns:
        Dict with 'success', 'data' (list of results), 'endpoint_type', and 'error' if failed
    """
    try:
        prometheus_url, endpoint_type = await _discover_prometheus_endpoint()
        if not prometheus_url:
            return {"success": False, "data": [], "error": "Could not discover Prometheus/Thanos endpoint"}

        # Get authentication token
        auth_token = await _get_k8s_bearer_token()

        api_path = "/api/v1/query"
        params = {"query": query, "timeout": f"{timeout}s"}

        # Add Thanos-specific parameters for deduplicated results
        if endpoint_type == "thanos":
            params["dedup"] = "true"

        query_url = f"{prometheus_url}{api_path}"

        headers = {
            "Accept": "application/json",
            "User-Agent": "LUMINO-MCP/1.0"
        }

        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout + 10)) as session:
            async with session.get(query_url, params=params, headers=headers, ssl=False) as response:
                if response.status == 200:
                    response_data = await response.json()
                    result_data = response_data.get("data", {})
                    raw_results = result_data.get("result", [])
                    return {"success": True, "data": raw_results, "endpoint_type": endpoint_type, "error": None}
                else:
                    error_text = await response.text()
                    logger.warning(f"Prometheus query failed with status {response.status}: {error_text}")
                    return {"success": False, "data": [], "endpoint_type": endpoint_type, "error": f"HTTP {response.status}: {error_text}"}

    except Exception as e:
        logger.error(f"Error executing internal Prometheus query: {e}")
        return {"success": False, "data": [], "error": str(e)}


async def _process_prometheus_results(
    response_data: Dict[str, Any],
    format_type: str,
    namespace_filter: Optional[str],
    limit: Optional[int],
    original_query: str,
    query_type: str
) -> Dict[str, Any]:
    """Process and format Prometheus query results."""
    try:
        result_data = response_data.get("data", {})
        result_type = result_data.get("resultType", "")
        raw_results = result_data.get("result", [])

        # Apply namespace filtering if specified
        if namespace_filter:
            try:
                namespace_pattern = _safe_compile_namespace_filter(namespace_filter)
                filtered_results = []

                for result in raw_results:
                    metric = result.get("metric", {})
                    namespace = metric.get("namespace", "")
                    if namespace and namespace_pattern.search(namespace):
                        filtered_results.append(result)

                raw_results = filtered_results
                logger.info(f"Applied namespace filter '{namespace_filter}', {len(raw_results)} results remain")

            except (re.error, ValueError) as e:
                logger.warning(f"Invalid namespace filter regex '{namespace_filter}': {e}")

        # Apply limit if specified
        if limit and len(raw_results) > limit:
            raw_results = raw_results[:limit]
            logger.info(f"Limited results to {limit} items")

        # Apply safety limit to prevent excessive response sizes (max 500 series)
        MAX_SERIES_LIMIT = 500
        if len(raw_results) > MAX_SERIES_LIMIT:
            logger.warning(f"Truncating {len(raw_results)} series to {MAX_SERIES_LIMIT} to prevent excessive response size")
            raw_results = raw_results[:MAX_SERIES_LIMIT]

        # Format results based on requested format
        if format_type == "table":
            formatted_data = _format_as_table(raw_results, result_type)
        elif format_type == "csv":
            formatted_data = _format_as_csv(raw_results, result_type)
        else:  # json format (default)
            formatted_data = _format_as_json(raw_results, result_type)

        # Generate summary and analysis
        summary = _generate_result_summary(raw_results, result_type, original_query)
        suggestions = _generate_related_query_suggestions(original_query, raw_results)

        return {
            "result_count": len(raw_results),
            "result_type": result_type,
            "data": formatted_data,
            "summary": summary,
            "suggestions": suggestions,
            "errors": [],
            "metadata": {
                "namespace_filter": namespace_filter,
                "limit": limit,
                "format": format_type,
                "query_type": query_type
            }
        }

    except Exception as e:
        logger.error(f"Error processing Prometheus results: {e}")
        return {
            "result_count": 0,
            "result_type": "unknown",
            "data": [],
            "summary": "Error processing results",
            "suggestions": ["Check query syntax", "Try simpler query"],
            "errors": [str(e)]
        }


def _format_as_table(results: List[Dict], result_type: str) -> str:
    """Format results as a human-readable table."""
    if not results:
        return "No data returned"

    try:
        if result_type == "vector":
            # Instant query results
            headers = ["Metric"] + list(results[0].get("metric", {}).keys()) + ["Value"]
            rows = []

            for result in results:
                metric = result.get("metric", {})
                value = result.get("value", ["", ""])[1] if result.get("value") else "N/A"

                metric_name = metric.get("__name__", "")
                row = [metric_name] + [metric.get(key, "") for key in headers[1:-1]] + [value]
                rows.append(row)

        elif result_type == "matrix":
            # Range query results
            headers = ["Metric", "Namespace", "Values (timestamp:value)"]
            rows = []

            for result in results:
                metric = result.get("metric", {})
                values = result.get("values", [])

                metric_name = metric.get("__name__", "")
                namespace = metric.get("namespace", "")

                # Format values as timestamp:value pairs (limit to first 5 for readability)
                value_pairs = [f"{ts}:{val}" for ts, val in values[:5]]
                if len(values) > 5:
                    value_pairs.append(f"... ({len(values) - 5} more)")

                rows.append([metric_name, namespace, ", ".join(value_pairs)])

        else:
            return f"Unsupported result type for table format: {result_type}"

        if not rows:
            return "No data to display"

        # Calculate column widths
        col_widths = [max(len(str(header)), max(len(str(row[i])) for row in rows)) for i, header in enumerate(headers)]

        # Build table
        table_lines = []

        # Header
        header_line = " | ".join(header.ljust(col_widths[i]) for i, header in enumerate(headers))
        table_lines.append(header_line)
        table_lines.append("-" * len(header_line))

        # Rows
        for row in rows:
            row_line = " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(headers)))
            table_lines.append(row_line)

        return "\n".join(table_lines)

    except Exception as e:
        logger.error(f"Error formatting table: {e}")
        return f"Error formatting table: {e}"


def _format_as_csv(results: List[Dict], result_type: str) -> str:
    """Format results as CSV."""
    if not results:
        return "No data returned"

    try:
        import csv
        import io

        output = io.StringIO()

        if result_type == "vector":
            # Instant query results
            fieldnames = ["metric_name"] + list(results[0].get("metric", {}).keys()) + ["value", "timestamp"]
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            for result in results:
                metric = result.get("metric", {})
                value_data = result.get("value", ["", ""])

                row = {
                    "metric_name": metric.get("__name__", ""),
                    "value": value_data[1] if len(value_data) > 1 else "",
                    "timestamp": value_data[0] if len(value_data) > 0 else ""
                }
                row.update({k: v for k, v in metric.items() if k != "__name__"})
                writer.writerow(row)

        elif result_type == "matrix":
            # Range query results - flatten time series
            fieldnames = ["metric_name", "namespace", "timestamp", "value"]
            if results:
                additional_labels = set()
                for result in results:
                    metric = result.get("metric", {})
                    additional_labels.update(k for k in metric.keys() if k not in ["__name__", "namespace"])
                fieldnames.extend(sorted(additional_labels))

            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()

            for result in results:
                metric = result.get("metric", {})
                values = result.get("values", [])

                base_row = {
                    "metric_name": metric.get("__name__", ""),
                    "namespace": metric.get("namespace", "")
                }
                base_row.update({k: v for k, v in metric.items() if k not in ["__name__", "namespace"]})

                for timestamp, value in values:
                    row = base_row.copy()
                    row.update({"timestamp": timestamp, "value": value})
                    writer.writerow(row)

        return output.getvalue()

    except Exception as e:
        logger.error(f"Error formatting CSV: {e}")
        return f"Error formatting CSV: {e}"


def _format_as_json(results: List[Dict], result_type: str) -> List[Dict]:
    """Format results as structured JSON."""
    try:
        formatted_results = []

        for result in results:
            metric = result.get("metric", {})

            if result_type == "vector":
                # Instant query
                value_data = result.get("value", [])
                formatted_result = {
                    "metric": metric,
                    "value": value_data[1] if len(value_data) > 1 else None,
                    "timestamp": value_data[0] if len(value_data) > 0 else None,
                    "formatted_value": _format_metric_value(metric.get("__name__", ""), value_data[1] if len(value_data) > 1 else None)
                }

            elif result_type == "matrix":
                # Range query - downsample to avoid excessive response size
                values = result.get("values", [])
                total_count = len(values)

                # Calculate statistical summary instead of returning all raw data
                numeric_values = []
                for v in values:
                    try:
                        numeric_values.append(float(v[1]))
                    except (ValueError, TypeError, IndexError):
                        pass

                stats = {}
                if numeric_values:
                    sorted_vals = sorted(numeric_values)
                    stats = {
                        "min": round(min(numeric_values), 4),
                        "max": round(max(numeric_values), 4),
                        "avg": round(sum(numeric_values) / len(numeric_values), 4),
                        "latest": round(numeric_values[-1], 4),
                        "first": round(numeric_values[0], 4),
                        "p50": round(sorted_vals[len(sorted_vals) // 2], 4),
                        "p95": round(sorted_vals[int(len(sorted_vals) * 0.95)], 4) if len(sorted_vals) > 1 else round(sorted_vals[0], 4),
                    }

                # Downsample values to max 50 points for trend visualization
                MAX_DATAPOINTS = 50
                sampled_values = []
                if total_count > MAX_DATAPOINTS:
                    step = total_count / MAX_DATAPOINTS
                    for i in range(MAX_DATAPOINTS):
                        idx = int(i * step)
                        sampled_values.append(values[idx])
                else:
                    sampled_values = values

                formatted_result = {
                    "metric": metric,
                    "statistics": stats,
                    "values": sampled_values,  # Keep as "values" for backward compatibility
                    "value_count": total_count,
                    "sampled_count": len(sampled_values),
                    "downsampled": total_count > MAX_DATAPOINTS,
                    "time_range": {
                        "start": values[0][0] if values else None,
                        "end": values[-1][0] if values else None
                    }
                }

            else:
                # Fallback
                formatted_result = result

            formatted_results.append(formatted_result)

        return formatted_results

    except Exception as e:
        logger.error(f"Error formatting JSON: {e}")
        return [{"error": f"Error formatting results: {e}"}]


def _format_metric_value(metric_name: str, value: Optional[str]) -> str:
    """Format metric value with appropriate units."""
    if value is None:
        return "N/A"

    try:
        numeric_value = float(value)

        # Format based on metric name patterns
        if "cpu" in metric_name.lower():
            if "seconds" in metric_name.lower():
                return f"{numeric_value:.3f} CPU seconds"
            else:
                return f"{numeric_value:.3f} CPU cores"
        elif "memory" in metric_name.lower() or "bytes" in metric_name.lower():
            # Convert bytes to human readable
            if numeric_value >= 1024**3:
                return f"{numeric_value / (1024**3):.2f} GB"
            elif numeric_value >= 1024**2:
                return f"{numeric_value / (1024**2):.2f} MB"
            elif numeric_value >= 1024:
                return f"{numeric_value / 1024:.2f} KB"
            else:
                return f"{numeric_value:.0f} bytes"
        elif "percentage" in metric_name.lower() or "percent" in metric_name.lower():
            return f"{numeric_value:.1f}%"
        else:
            return f"{numeric_value:.3f}"

    except (ValueError, TypeError):
        return str(value)


def _generate_result_summary(results: List[Dict], result_type: str, query: str) -> str:
    """Generate human-readable summary of query results."""
    if not results:
        return f"No data returned for query: {query}"

    try:
        summary_parts = []

        # Basic count
        summary_parts.append(f"Found {len(results)} metric series")

        # Analyze namespaces
        namespaces = set()
        for result in results:
            metric = result.get("metric", {})
            if "namespace" in metric:
                namespaces.add(metric["namespace"])

        if namespaces:
            summary_parts.append(f"across {len(namespaces)} namespaces: {', '.join(sorted(list(namespaces))[:5])}")
            if len(namespaces) > 5:
                summary_parts[-1] += f" and {len(namespaces) - 5} more"

        # Analyze metric types
        metric_names = set()
        for result in results:
            metric = result.get("metric", {})
            if "__name__" in metric:
                metric_names.add(metric["__name__"])

        if metric_names:
            summary_parts.append(f"Metric types: {', '.join(sorted(list(metric_names))[:3])}")
            if len(metric_names) > 3:
                summary_parts[-1] += f" and {len(metric_names) - 3} more"

        return ". ".join(summary_parts) + "."

    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        return f"Query returned {len(results)} results"


def _generate_query_suggestions(query: str, error_message: str) -> List[str]:
    """Generate helpful suggestions based on query and error."""
    suggestions = []

    # Common PromQL syntax errors
    if "parse error" in error_message.lower():
        suggestions.extend([
            "Check PromQL syntax - ensure proper use of operators and functions",
            "Verify metric names and label selectors are correctly formatted",
            "Example: up{job=\"node-exporter\"} or rate(http_requests_total[5m])"
        ])

    if "unknown metric" in error_message.lower() or "not found" in error_message.lower():
        suggestions.extend([
            "Check if the metric name is spelled correctly",
            "Try querying available metrics with: {__name__=~\".*\"}",
            "Verify the metric is actually being scraped by Prometheus"
        ])

    if "timeout" in error_message.lower():
        suggestions.extend([
            "Try a shorter time range for range queries",
            "Use more specific label selectors to reduce data volume",
            "Consider using recording rules for complex queries"
        ])

    # Query-specific suggestions
    if "rate(" in query and "[" not in query:
        suggestions.append("rate() function requires a time range: rate(metric[5m])")

    if "{" in query and "}" in query:
        if "=~" in query:
            suggestions.append("Ensure regex patterns are valid and properly escaped")

    # Default suggestions if no specific ones
    if not suggestions:
        suggestions.extend([
            "Check Prometheus documentation for correct PromQL syntax",
            "Try a simpler query first to test connectivity",
            "Verify you have access to the metrics you're querying"
        ])

    return suggestions


def _generate_related_query_suggestions(original_query: str, results: List[Dict]) -> List[str]:
    """Generate suggestions for related queries based on results."""
    suggestions = []

    try:
        if not results:
            suggestions.extend([
                "Try expanding the time range if using a range query",
                "Check if the metric exists: {__name__=~\".*metric_name.*\"}",
                "List all available metrics: {__name__=~\".*\"}"
            ])
            return suggestions

        # Extract metric names from results
        metric_names = set()
        namespaces = set()

        for result in results:
            metric = result.get("metric", {})
            if "__name__" in metric:
                metric_names.add(metric["__name__"])
            if "namespace" in metric:
                namespaces.add(metric["namespace"])

        # Suggest related queries
        if metric_names:
            example_metric = list(metric_names)[0]
            if "cpu" in example_metric:
                suggestions.append("Related memory usage: sum(container_memory_working_set_bytes) by (namespace)")
            elif "memory" in example_metric:
                suggestions.append("Related CPU usage: sum(rate(container_cpu_usage_seconds_total[5m])) by (namespace)")

            if "rate(" not in original_query and "_total" in example_metric:
                suggestions.append(f"Rate calculation: rate({example_metric}[5m])")

        if namespaces and len(namespaces) > 1:
            suggestions.append(f"Filter by specific namespace: {{namespace=\"{list(namespaces)[0]}\"}}")

        if "topk(" not in original_query:
            suggestions.append(f"Top 10 results: topk(10, {original_query})")

        # Time-based suggestions
        if "range" not in original_query:
            suggestions.append(f"Historical data: {original_query} over time range")

    except Exception as e:
        logger.error(f"Error generating related suggestions: {e}")

    return suggestions[:5]  # Limit to 5 suggestions


@mcp.tool()
async def prometheus_query(
    query: str,
    query_type: str = "instant",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    step: str = "300s",
    cluster: Optional[str] = None,
    format: str = "json",
    namespace_filter: Optional[str] = None,
    limit: Optional[int] = None,
    timeout: int = 30
) -> Dict[str, Any]:
    """
    Execute PromQL queries against Prometheus for cluster metrics.

    Supports instant and range queries with automatic endpoint discovery and authentication.

    Args:
        query: PromQL query string.
        query_type: "instant" or "range" (default: "instant").
        start_time: Start for range queries (ISO 8601 or Unix timestamp).
        end_time: End for range queries (ISO 8601 or Unix timestamp).
        step: Step interval for range queries (default: "300s").
        cluster: Cluster domain override.
        format: "json", "table", or "csv" (default: "json").
        namespace_filter: Regex to filter by namespace.
        limit: Max results to return.
        timeout: Query timeout in seconds (default: 30).

    Returns:
        Dict: Query results, metadata, execution info, and analysis.
    """
    start_execution_time = time.time()
    tool_name = "mcp__lumino__prometheus_query"

    logger.info(f"[{tool_name}] Starting Prometheus query execution")
    logger.info(f"[{tool_name}] Query: {query}")
    logger.info(f"[{tool_name}] Type: {query_type}, Format: {format}")

    try:
        # Validate required parameters
        if not query or not query.strip():
            return {
                "status": "error",
                "error_type": "invalid_query",
                "message": "Query parameter is required and cannot be empty",
                "query_executed": "",
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": ["Provide a valid PromQL query", "Example: up{job=\"node-exporter\"}"],
                "errors": ["Empty query provided"]
            }

        # Validate query type
        if query_type not in ["instant", "range"]:
            return {
                "status": "error",
                "error_type": "invalid_query_type",
                "message": f"Invalid query_type '{query_type}'. Must be 'instant' or 'range'",
                "query_executed": query,
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": ["Use query_type='instant' for current values", "Use query_type='range' for time series"],
                "errors": [f"Invalid query_type: {query_type}"]
            }

        # Validate range query parameters
        if query_type == "range":
            if not start_time or not end_time:
                return {
                    "status": "error",
                    "error_type": "missing_time_range",
                    "message": "Range queries require both start_time and end_time parameters",
                    "query_executed": query,
                    "execution_time": 0,
                    "result_count": 0,
                    "data": [],
                    "suggestions": [
                        "Provide start_time and end_time for range queries",
                        "Use ISO 8601 format: '2024-01-01T00:00:00Z'",
                        "Or Unix timestamps: '1704067200'"
                    ],
                    "errors": ["Missing time range parameters for range query"]
                }

        # Get Kubernetes authentication token (optional for vanilla K8s Prometheus)
        auth_token = await _get_k8s_bearer_token()
        if not auth_token:
            logger.info(f"[{tool_name}] No bearer token available - will attempt unauthenticated request (common for vanilla Kubernetes Prometheus)")

        # Discover or use Prometheus/Thanos endpoint
        prometheus_url, endpoint_type = await _discover_prometheus_endpoint(cluster)
        if not prometheus_url:
            return {
                "status": "error",
                "error_type": "endpoint_discovery_failed",
                "message": "Could not discover Prometheus endpoint",
                "query_executed": query,
                "execution_time": 0,
                "result_count": 0,
                "data": [],
                "suggestions": [
                    "Check if Prometheus or Thanos Query is deployed (openshift-monitoring, monitoring, thanos, or observability namespace)",
                    "Verify Prometheus Operator CRDs are installed if using Prometheus Operator",
                    "Ensure OpenShift Routes are accessible if on OpenShift",
                    "Set THANOS_URL or PROMETHEUS_URL environment variable to specify endpoint directly",
                    "Try adding a predefined endpoint in OPENSHIFT_PROMETHEUS_ENDPOINTS config"
                ],
                "errors": ["Prometheus/Thanos endpoint not found"]
            }

        logger.info(f"[{tool_name}] Using {endpoint_type} endpoint: {prometheus_url}")

        # Build query URL and parameters
        if query_type == "instant":
            api_path = "/api/v1/query"
            params = {"query": query}
            if timeout:
                params["timeout"] = f"{timeout}s"
        else:  # range query
            api_path = "/api/v1/query_range"
            params = {
                "query": query,
                "start": _parse_time_parameter(start_time),
                "end": _parse_time_parameter(end_time),
                "step": step
            }
            if timeout:
                params["timeout"] = f"{timeout}s"

        # Add Thanos-specific parameters for deduplicated, consistent results
        if endpoint_type == "thanos":
            params["dedup"] = "true"

        query_url = f"{prometheus_url}{api_path}"
        headers = {
            "Accept": "application/json",
            "User-Agent": "LUMINO-MCP/1.0"
        }
        # Only add Authorization header if token is available
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        logger.info(f"[{tool_name}] Executing query against: {query_url}")

        # Execute Prometheus query
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout + 10)) as session:
            async with session.get(query_url, params=params, headers=headers, ssl=False) as response:
                execution_time = round((time.time() - start_execution_time) * 1000, 2)

                if response.status == 200:
                    response_data = await response.json()
                    logger.info(f"[{tool_name}] Query executed successfully in {execution_time}ms")

                    # Process results
                    processed_results = await _process_prometheus_results(
                        response_data, format, namespace_filter, limit, query, query_type
                    )

                    # Add execution metadata
                    processed_results.update({
                        "status": "success",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "prometheus_endpoint": prometheus_url,
                        "endpoint_type": endpoint_type,
                        "query_type": query_type,
                        "parameters": params
                    })

                    return processed_results

                elif response.status == 400:
                    error_text = await response.text()
                    logger.warning(f"[{tool_name}] Bad request (400): {error_text}")

                    # Try to parse Prometheus error for better suggestions
                    suggestions = _generate_query_suggestions(query, error_text)

                    return {
                        "status": "error",
                        "error_type": "invalid_query",
                        "message": f"PromQL query error: {error_text}",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": suggestions,
                        "errors": [error_text]
                    }

                elif response.status == 401:
                    logger.error(f"[{tool_name}] Authentication failed (401)")
                    return {
                        "status": "error",
                        "error_type": "authentication_failed",
                        "message": "Authentication failed - invalid or expired token",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": [
                            "Refresh your Kubernetes credentials (kubeconfig or ServiceAccount)",
                            "Check if token has expired",
                            "Set PROMETHEUS_TOKEN environment variable with a valid token",
                            "Verify cluster access permissions"
                        ],
                        "errors": ["Authentication failed"]
                    }

                elif response.status == 403:
                    logger.error(f"[{tool_name}] Access forbidden (403)")
                    return {
                        "status": "error",
                        "error_type": "permission_denied",
                        "message": "Access denied - insufficient permissions",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": [
                            "Check RBAC permissions for metrics access",
                            "Verify cluster-monitoring-view role binding",
                            "Contact cluster administrator for monitoring access"
                        ],
                        "errors": ["Permission denied"]
                    }

                else:
                    error_text = await response.text()
                    logger.error(f"[{tool_name}] HTTP error {response.status}: {error_text}")
                    return {
                        "status": "error",
                        "error_type": "http_error",
                        "message": f"HTTP {response.status}: {error_text}",
                        "query_executed": query,
                        "execution_time": execution_time,
                        "result_count": 0,
                        "data": [],
                        "suggestions": [
                            "Check Prometheus service availability",
                            "Verify cluster connectivity",
                            "Try again in a few minutes"
                        ],
                        "errors": [f"HTTP {response.status}: {error_text}"]
                    }

    except asyncio.TimeoutError:
        execution_time = round((time.time() - start_execution_time) * 1000, 2)
        logger.error(f"[{tool_name}] Query timeout after {timeout}s")
        return {
            "status": "error",
            "error_type": "timeout",
            "message": f"Query timed out after {timeout} seconds",
            "query_executed": query,
            "execution_time": execution_time,
            "result_count": 0,
            "data": [],
            "suggestions": [
                "Try a simpler query with shorter time range",
                "Increase timeout parameter",
                "Use more specific label selectors to reduce data"
            ],
            "errors": [f"Timeout after {timeout}s"]
        }

    except Exception as e:
        execution_time = round((time.time() - start_execution_time) * 1000, 2)
        error_msg = f"Unexpected error during query execution: {str(e)}"
        logger.error(f"[{tool_name}] {error_msg}", exc_info=True)

        return {
            "status": "error",
            "error_type": "unexpected_error",
            "message": error_msg,
            "query_executed": query,
            "execution_time": execution_time,
            "result_count": 0,
            "data": [],
            "suggestions": [
                "Check system logs for details",
                "Verify cluster connectivity",
                "Try a simpler query first"
            ],
            "errors": [str(e)]
        }


# ============================================================================
# SMART LOG ANALYSIS HELPER FUNCTIONS
# ============================================================================
# Note: AdaptiveLogProcessor class is defined earlier in the file (around line 366)


def _filter_analysis_for_synthesis(pod_analysis: Dict[str, Any], focus_areas: List[str]) -> Dict[str, Any]:
    """
    Filter pod analysis results to keep only essential data for synthesis, preventing token overflow.

    Args:
        pod_analysis: Full pod analysis results
        focus_areas: Areas to focus on for filtering

    Returns:
        Filtered analysis with only essential data
    """
    try:
        # Keep only essential fields to prevent token overflow
        filtered = {
            "summary": pod_analysis.get("summary", {}),
            "metadata": {
                "total_log_lines": pod_analysis.get("metadata", {}).get("processing_metrics", {}).get("total_log_lines", 0),
                "patterns_extracted": pod_analysis.get("metadata", {}).get("processing_metrics", {}).get("patterns_extracted", 0),
                "processing_time_seconds": pod_analysis.get("metadata", {}).get("processing_metrics", {}).get("processing_time_seconds", 0)
            }
        }

        # Keep only focused patterns (top 3 items per focus area)
        if "patterns" in pod_analysis:
            filtered["patterns"] = {}
            for area in focus_areas:
                if area in pod_analysis["patterns"] and pod_analysis["patterns"][area]:
                    # Keep only top 3 most important items per area
                    filtered["patterns"][area] = pod_analysis["patterns"][area][:3]

        # Keep only essential representative samples (top 2 per area)
        if "representative_samples" in pod_analysis:
            filtered["representative_samples"] = {}
            for area in focus_areas:
                if area in pod_analysis["representative_samples"]:
                    # Keep only top 2 samples per area
                    filtered["representative_samples"][area] = pod_analysis["representative_samples"][area][:2]

        return filtered

    except Exception as e:
        logger.warning(f"Error filtering analysis: {e}")
        # Fallback: return minimal data
        return {
            "summary": pod_analysis.get("summary", "Analysis available but filtered due to size"),
            "metadata": {"filtered": True, "reason": "token_overflow_prevention"}
        }


def _compress_events_for_synthesis(events_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compress event analysis results to essential information for synthesis.

    Args:
        events_result: Full event analysis results

    Returns:
        Compressed events data with only essential information
    """
    try:
        if not events_result or "error" in events_result:
            return events_result

        # Keep only essential event information
        compressed = {
            "namespace": events_result.get("namespace"),
            "strategy_used": events_result.get("strategy_used"),
            "total_events": events_result.get("total_events", 0),
            "processed_events": events_result.get("processed_events", 0)
        }

        # Keep only top 5 most critical events
        if "events" in events_result and events_result["events"]:
            # Sort by severity and relevance, keep top 5
            sorted_events = sorted(
                events_result["events"],
                key=lambda e: (e.get("severity") == "CRITICAL", e.get("relevance_score", 0)),
                reverse=True
            )
            compressed["critical_events"] = sorted_events[:5]

        # Keep summary and insights
        if "summary" in events_result:
            compressed["summary"] = events_result["summary"]

        if "insights" in events_result:
            compressed["insights"] = events_result["insights"][:3]  # Top 3 insights

        if "recommendations" in events_result:
            compressed["recommendations"] = events_result["recommendations"][:3]  # Top 3 recommendations

        return compressed

    except Exception as e:
        logger.warning(f"Error compressing events: {e}")
        return {"compressed": True, "total_events": events_result.get("total_events", 0)}


async def _quick_volume_estimate(namespace: str, pod_name: str) -> int:
    """
    Quick estimate of log volume using minimal token budget.

    Args:
        namespace: Kubernetes namespace
        pod_name: Pod name to estimate

    Returns:
        Estimated total log lines for the pod
    """
    try:
        # Sample last 5 minutes to estimate volume
        sample = await get_pod_logs(
            namespace=namespace,
            pod_name=pod_name,
            since_seconds=300  # 5 minutes
        )

        if "logs" in sample and sample["logs"]:
            sample_lines = 0
            for container_logs in sample["logs"].values():
                if isinstance(container_logs, str):
                    sample_lines += len(container_logs.split('\n'))
                elif isinstance(container_logs, list):
                    sample_lines += len(container_logs)

            # Extrapolate to 24 hours (conservative estimate)
            # Assume sample represents 5 minutes, extrapolate to 24 hours
            estimated_total = sample_lines * (24 * 60 / 5)  # 24 hours / 5 minutes
            logger.info(f"Volume estimate for {pod_name}: {sample_lines} lines in 5min → ~{int(estimated_total)} total estimated")
            return int(estimated_total)

    except Exception as e:
        logger.debug(f"Volume estimation failed for {pod_name}: {e}")

    return 10000  # Conservative default estimate


# ============================================================================
# ETCD LOG HELPERS
# ============================================================================


def clean_etcd_logs(raw_logs: str) -> str:
    """
    Clean etcd logs by removing escape characters and properly formatting JSON log entries.

    This function handles the common issues with etcd logs fetched from Kubernetes:
    1. Multiple levels of JSON escaping (\\" becomes ")
    2. Escaped newlines (\\n becomes actual newlines)
    3. Duplicate timestamps (Kubernetes timestamp + etcd timestamp)
    4. Malformed JSON structure

    Example transformation:
    Input:  '2025-01-15T10:30:00.123456789Z {"level":"info","ts":"2025-01-15T10:30:00.123Z","msg":"test"}'
    Output: '[2025-01-15T10:30:00.123Z] [INFO] test'

    Args:
        raw_logs (str): Raw log content from Kubernetes API

    Returns:
        str: Cleaned and formatted log content
    """
    if not raw_logs or raw_logs.strip() == "":
        return raw_logs

    try:
        # Split logs into individual lines
        lines = raw_logs.strip().split('\n')
        cleaned_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Skip lines that are just error messages or info messages
            if line.startswith(('ERROR:', 'INFO:')):
                cleaned_lines.append(line)
                continue

            try:
                # Handle lines with Kubernetes timestamp prefix followed by JSON
                # Pattern: "2025-01-15T10:30:00.123456789Z {"level":"info",...}"
                timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(.*)$', line)

                if timestamp_match:
                    k8s_timestamp = timestamp_match.group(1)
                    json_part = timestamp_match.group(2)

                    # Remove multiple levels of escaping
                    # First level: \\" -> "
                    json_part = json_part.replace('\\\\"', '"')
                    # Second level: \\n -> \n
                    json_part = json_part.replace('\\n', '\n')
                    # Handle other common escapes
                    json_part = json_part.replace('\\/', '/')
                    json_part = json_part.replace('\\t', '\t')
                    json_part = json_part.replace('\\r', '\r')
                    json_part = json_part.replace('\\\\', '\\')

                    # Try to parse as JSON
                    try:
                        json_obj = json.loads(json_part)

                        # Extract key fields from etcd log JSON
                        level = json_obj.get('level', 'unknown')
                        etcd_timestamp = json_obj.get('ts', '')
                        caller = json_obj.get('caller', '')
                        msg = json_obj.get('msg', '')

                        # Create a cleaner log format
                        # Use etcd timestamp if available, otherwise use k8s timestamp
                        timestamp_to_use = etcd_timestamp if etcd_timestamp else k8s_timestamp

                        # Build formatted log entry
                        formatted_parts = []
                        if timestamp_to_use:
                            formatted_parts.append(f"[{timestamp_to_use}]")
                        if level:
                            formatted_parts.append(f"[{level.upper()}]")
                        if caller:
                            formatted_parts.append(f"[{caller}]")
                        if msg:
                            formatted_parts.append(msg)

                        # Add other important fields if present
                        for key, value in json_obj.items():
                            if key not in ['level', 'ts', 'caller', 'msg'] and value is not None:
                                if isinstance(value, (str, int, float, bool)):
                                    formatted_parts.append(f"{key}={value}")
                                else:
                                    formatted_parts.append(f"{key}={json.dumps(value)}")

                        formatted_line = " ".join(formatted_parts)
                        cleaned_lines.append(formatted_line)

                    except json.JSONDecodeError:
                        # If JSON parsing fails, just clean up the escaping and use as-is
                        cleaned_line = json_part.replace('\\"', '"').replace('\\n', '\n')
                        if k8s_timestamp:
                            cleaned_line = f"[{k8s_timestamp}] {cleaned_line}"
                        cleaned_lines.append(cleaned_line)

                else:
                    # Line doesn't match timestamp pattern, try to clean it anyway
                    cleaned_line = line.replace('\\\\"', '"').replace('\\n', '\n').replace('\\/', '/').replace('\\t', '\t').replace('\\r', '\r').replace('\\\\', '\\')

                    # Try to parse as JSON if it looks like JSON
                    if cleaned_line.startswith('{') and cleaned_line.endswith('}'):
                        try:
                            json_obj = json.loads(cleaned_line)
                            # Format as readable log entry
                            level = json_obj.get('level', 'unknown')
                            timestamp = json_obj.get('ts', '')
                            caller = json_obj.get('caller', '')
                            msg = json_obj.get('msg', '')

                            formatted_parts = []
                            if timestamp:
                                formatted_parts.append(f"[{timestamp}]")
                            if level:
                                formatted_parts.append(f"[{level.upper()}]")
                            if caller:
                                formatted_parts.append(f"[{caller}]")
                            if msg:
                                formatted_parts.append(msg)

                            formatted_line = " ".join(formatted_parts)
                            cleaned_lines.append(formatted_line)

                        except json.JSONDecodeError:
                            # Not valid JSON, use the cleaned line as-is
                            cleaned_lines.append(cleaned_line)
                    else:
                        # Not JSON, use the cleaned line as-is
                        cleaned_lines.append(cleaned_line)

            except Exception as e:
                # If any processing fails, include the original line with a note
                logger.debug(f"Failed to process log line: {e}")
                cleaned_lines.append(f"[UNPARSED] {line}")

        # Join the cleaned lines
        result = '\n'.join(cleaned_lines)

        # Final cleanup - remove excessive whitespace
        result = re.sub(r'\n\s*\n', '\n', result)  # Remove empty lines
        result = re.sub(r' +', ' ', result)  # Collapse multiple spaces

        return result.strip()

    except Exception as e:
        logger.error(f"Error cleaning etcd logs: {e}")
        # Return original logs if cleaning fails
        return raw_logs


def _handle_api_exception(e: 'ApiException', tool_name: str, strategy: str, namespace: str,
                         label_selector: str, results_dict: Dict[str, str]) -> None:
    """Helper function to handle Kubernetes API exceptions consistently."""
    strategy_lower = strategy.lower()

    if e.status == 404:
        logger.warning(f"[{tool_name}] {strategy} strategy: 404 Not Found - namespace '{namespace}' or resources not found")
        results_dict[f"info_{strategy_lower}_404"] = f"Namespace '{namespace}' or pods with label '{label_selector}' not found"
    elif e.status == 403:
        logger.warning(f"[{tool_name}] {strategy} strategy: 403 Forbidden - insufficient RBAC permissions")
        results_dict[f"error_{strategy_lower}_403"] = (f"Insufficient permissions for namespace '{namespace}'. "
                                                      f"Required: pods/list, pods/log permissions")
    elif e.status == 401:
        logger.error(f"[{tool_name}] {strategy} strategy: 401 Unauthorized - authentication failed")
        results_dict[f"error_{strategy_lower}_401"] = "Authentication failed. Check kubeconfig and credentials"
    else:
        logger.error(f"[{tool_name}] {strategy} strategy: API error {e.status} - {e.reason}")
        results_dict[f"error_{strategy_lower}_api"] = f"API error {e.status}: {e.reason}"


def _get_logs_with_k8s_client(
    k8s_core_api: 'client.CoreV1Api',
    pod_names: List[str],
    namespace: str,
    container_name: str,
    target_logs_dict: Dict[str, str],
    log_params: Dict[str, Union[int, str, bool, None]]
) -> bool:
    """
    Enhanced helper to fetch logs for a list of pod names with flexible time and line filtering.

    Args:
        k8s_core_api: Initialized CoreV1Api client
        pod_names: List of pod names to fetch logs from
        namespace: Namespace of the pods
        container_name: Name of the container within the pods
        target_logs_dict: Dictionary to populate with logs or error messages
        log_params: Dictionary containing log retrieval parameters:
            - tail_lines: Number of lines from end of logs
            - since_seconds: Logs newer than this many seconds
            - since_time: Logs newer than this RFC3339 timestamp
            - follow: Stream logs in real-time
            - timestamps: Include timestamps in output
            - previous: Get logs from previous container instance

    Returns:
        bool: True if logs were successfully fetched for at least one pod
    """
    logger.debug(f"Fetching logs for {len(pod_names)} pods in namespace '{namespace}', container '{container_name}'")
    at_least_one_log_fetched = False

    for pod_name in pod_names:
        logger.info(f"Fetching logs for pod '{pod_name}' with params: {log_params}")

        try:
            # Build log retrieval parameters, filtering out None values
            log_kwargs = {
                'name': pod_name,
                'namespace': namespace,
                'container': container_name,
                'timestamps': log_params.get('timestamps', True),
                'follow': log_params.get('follow', False),
                'previous': log_params.get('previous', False)
            }

            # Add time-based or line-based filtering (mutually exclusive in K8s API)
            # Note: Kubernetes API uses 'since' parameter for RFC3339 timestamps (not 'since_time')
            if log_params.get('since_time'):
                # Convert our 'since_time' to K8s API 'since' parameter
                log_kwargs['since'] = log_params['since_time']
            elif log_params.get('since_seconds'):
                log_kwargs['since_seconds'] = log_params['since_seconds']
            elif log_params.get('tail_lines'):
                log_kwargs['tail_lines'] = log_params['tail_lines']

            # Remove None values to avoid API errors
            log_kwargs = {k: v for k, v in log_kwargs.items() if v is not None}

            log_content = k8s_core_api.read_namespaced_pod_log(**log_kwargs)

            if log_content:
                # Clean etcd logs if this is an etcd container and cleaning is enabled
                if (container_name == "etcd" and
                    ("etcd" in pod_name.lower() or namespace in ["openshift-etcd", "kube-system"]) and
                    log_params.get('clean_logs', True)):
                    cleaned_content = clean_etcd_logs(log_content)
                    target_logs_dict[pod_name] = cleaned_content
                    logger.info(f"Successfully fetched and cleaned {len(cleaned_content)} characters of etcd logs for pod '{pod_name}'")
                else:
                    target_logs_dict[pod_name] = log_content
                    logger.info(f"Successfully fetched {len(log_content)} characters of logs for pod '{pod_name}'")
                at_least_one_log_fetched = True
            else:
                target_logs_dict[pod_name] = "INFO: No logs available for the specified time period/criteria"
                logger.info(f"No logs found for pod '{pod_name}' with current criteria")

        except ApiException as e:
            error_message = f"API error fetching logs for pod '{pod_name}': {e.status} - {e.reason}"
            if e.body:
                error_message += f" | Details: {str(e.body)[:200]}"

            logger.warning(error_message)
            target_logs_dict[pod_name] = f"ERROR: {error_message}"

        except Exception as e:
            error_message = f"Unexpected error fetching logs for pod '{pod_name}': {str(e)}"
            logger.error(error_message, exc_info=True)
            target_logs_dict[pod_name] = f"ERROR: {error_message}"

    return at_least_one_log_fetched


def _filter_logs_by_time_range(logs: str, until_time: datetime) -> str:
    """
    Filter log lines to only include entries before the specified until_time.

    Args:
        logs: Raw log content with timestamps
        until_time: Maximum timestamp (timezone-aware datetime)

    Returns:
        Filtered log content
    """
    if not logs or not until_time:
        return logs

    filtered_lines = []
    for line in logs.split('\n'):
        if not line.strip():
            continue

        # Try to extract timestamp from the beginning of the line
        # Common formats: "2024-01-15T10:30:45.123456Z" or "2024-01-15 10:30:45"
        try:
            # Check if line starts with a timestamp
            timestamp_match = line.split()[0] if line else None
            if timestamp_match:
                # Handle different timestamp formats
                if 'T' in timestamp_match:
                    # ISO format
                    log_time = datetime.fromisoformat(timestamp_match.replace('Z', '+00:00'))
                else:
                    # Try parsing date-time format
                    try:
                        # Try to get first two parts (date and time)
                        parts = line.split()
                        if len(parts) >= 2:
                            datetime_str = f"{parts[0]} {parts[1]}"
                            log_time = datetime.fromisoformat(datetime_str)
                        else:
                            continue
                    except:
                        continue

                # Only include logs before until_time
                if log_time <= until_time:
                    filtered_lines.append(line)
                else:
                    # Logs are typically chronological, so we can break early
                    break
            else:
                # Include lines without timestamps (might be continuation lines)
                filtered_lines.append(line)
        except (ValueError, IndexError):
            # If timestamp parsing fails, include the line to be safe
            filtered_lines.append(line)

    return '\n'.join(filtered_lines)


# ============================================================================
# SMART LOG ANALYSIS TOOLS
# ============================================================================


@mcp.tool()
async def smart_summarize_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    summary_level: str = "detailed",
    focus_areas: Optional[List[str]] = None,
    time_segments: int = 5,
    max_context_tokens: int = 10000,
    since_seconds: Optional[int] = None,
    tail_lines: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None
) -> Dict[str, Any]:
    """
    Adaptive pod log analysis with automatic volume management and multi-pass processing.

    When no time constraints specified, automatically estimates volume and selects optimal time windows.

    Args:
        namespace: Kubernetes namespace.
        pod_name: Pod name to analyze.
        container_name: Specific container (if multiple).
        summary_level: "brief", "detailed", or "comprehensive" (default: "detailed").
        focus_areas: Analysis focus (default: ["errors", "warnings", "performance"]).
        time_segments: Time-based segments to analyze (default: 5).
        max_context_tokens: Max tokens for analysis (default: 10000).
        since_seconds: Only if user specifies exact seconds.
        tail_lines: Only if user specifies exact line count.
        time_period: Only if user specifies period (e.g., "1h", "30m").
        start_time: Only if user specifies exact start time.
        end_time: Only if user specifies exact end time.

    Returns:
        Dict[str, Any]: Log analysis with insights, patterns, and recommendations.
    """
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings", "performance"]

    start_timestamp = time.time()
    tool_name = "smart_summarize_pod_logs"

    logger.info(f"[{tool_name}] Starting smart log analysis for pod '{pod_name}' in namespace '{namespace}'")
    logger.info(f"[{tool_name}] Parameters: summary_level={summary_level}, focus_areas={focus_areas}, "
                f"time_segments={time_segments}, max_context_tokens={max_context_tokens}")

    # Validate input parameters
    if not namespace or not isinstance(namespace, str):
        error_msg = f"Invalid namespace parameter: {namespace}. Must be a non-empty string."
        logger.error(f"[{tool_name}] {error_msg}")
        return {"error": error_msg}

    if not pod_name or not isinstance(pod_name, str):
        error_msg = f"Invalid pod_name parameter: {pod_name}. Must be a non-empty string."
        logger.error(f"[{tool_name}] {error_msg}")
        return {"error": error_msg}

    if summary_level not in ["brief", "detailed", "comprehensive"]:
        logger.warning(f"[{tool_name}] Invalid summary_level '{summary_level}', defaulting to 'detailed'")
        summary_level = "detailed"

    if time_segments <= 0:
        logger.warning(f"[{tool_name}] Invalid time_segments '{time_segments}', defaulting to 10")
        time_segments = 10

    if max_context_tokens < 500:
        logger.warning(f"[{tool_name}] Very low token limit ({max_context_tokens}), minimum is 500")
        max_context_tokens = 500

    try:
        # Step 1: Retrieve raw logs using existing function
        logger.info(f"[{tool_name}] Retrieving logs from pod '{pod_name}'")

        # CHECK FOR ADAPTIVE MODE FIRST (before parsing time parameters)
        user_specified_constraints = (
            since_seconds is not None or
            tail_lines is not None or
            time_period is not None or
            start_time is not None or
            end_time is not None
        )

        if not user_specified_constraints:
            # ADAPTIVE MODE: No user constraints specified
            logger.info(f"[{tool_name}] No time constraints specified - activating ADAPTIVE MODE")

            volume_estimate = await _quick_volume_estimate(namespace, pod_name)

            if volume_estimate > 50000:  # High volume
                log_params = {'tail_lines': 500}  # Conservative for high volume
                logger.info(f"[{tool_name}] HIGH VOLUME detected ({volume_estimate:,} estimated lines) - using 500 lines with error focus")
                # Boost error focus for high volume scenarios
                if "errors" not in focus_areas:
                    focus_areas = ["errors"] + list(focus_areas)
            elif volume_estimate > 10000:  # Medium volume
                log_params = {'tail_lines': 2000}  # Moderate for medium volume
                logger.info(f"[{tool_name}] MEDIUM VOLUME detected ({volume_estimate:,} estimated lines) - using 2000 lines")
            else:  # Low volume
                log_params = {'since_seconds': 7200}  # 2 hours for low volume
                logger.info(f"[{tool_name}] LOW VOLUME detected ({volume_estimate:,} estimated lines) - using 2 hour window for complete coverage")

            time_info = {'method': 'adaptive', 'strategy': 'volume_based', 'volume_estimate': volume_estimate}

        else:
            # MANUAL MODE: User specified constraints
            logger.info(f"[{tool_name}] User constraints detected - using MANUAL MODE")

            # Parse time parameters with enhanced support
            time_config = parse_time_parameters(
                since_seconds=since_seconds,
                time_period=time_period,
                start_time=start_time,
                end_time=end_time
            )

            log_params = time_config['log_params'].copy()
            time_info = time_config['time_info']

            if tail_lines is not None:
                log_params['tail_lines'] = tail_lines

        logger.info(f"[{tool_name}] Time configuration: {time_info}")

        # ADDITIONAL SAFETY: Ensure we don't process too much data even in adaptive mode
        if not log_params and max_context_tokens < 20000:
            # For small token budgets, be extra conservative
            log_params['tail_lines'] = min(1000, max_context_tokens // 10)
            logger.info(f"[{tool_name}] Small token budget detected ({max_context_tokens}), limiting to {log_params['tail_lines']} lines")

        raw_logs = await get_pod_logs(
            namespace=namespace,
            pod_name=pod_name,
            **log_params
        )

        if "error" in raw_logs:
            return {"error": f"Failed to retrieve logs: {raw_logs['error']}"}

        if "logs" not in raw_logs or not raw_logs["logs"]:
            return {
                "error": "No logs found for the specified pod",
                "metadata": {"pod_name": pod_name, "namespace": namespace}
            }

        # Step 2: Process logs for the target container or combine all containers
        all_log_lines = []
        container_info = {}

        for container, logs in raw_logs["logs"].items():
            if container_name and container != container_name:
                continue  # Skip other containers if specific container requested

            if isinstance(logs, list):
                container_lines = logs
            else:
                container_lines = str(logs).split('\n')

            container_info[container] = len(container_lines)
            all_log_lines.extend(container_lines)

        if not all_log_lines:
            return {
                "error": f"No logs found for container '{container_name}'" if container_name else "No log content found",
                "available_containers": list(raw_logs["logs"].keys())
            }

        # Remove empty lines
        all_log_lines = [line for line in all_log_lines if line.strip()]
        total_log_lines = len(all_log_lines)

        logger.info(f"[{tool_name}] Processing {total_log_lines} log lines from {len(container_info)} container(s)")

        # Step 3: Extract patterns based on focus areas
        logger.info(f"[{tool_name}] Extracting patterns for focus areas: {focus_areas}")
        patterns = extract_log_patterns(all_log_lines, focus_areas)

        # Step 4: Sample logs across time segments
        logger.info(f"[{tool_name}] Sampling logs across {time_segments} time segments")
        time_samples = sample_logs_by_time(all_log_lines, time_segments)

        # Step 5: Generate focused summary
        logger.info(f"[{tool_name}] Generating {summary_level} summary")
        summary = generate_focused_summary(patterns, focus_areas, summary_level)

        # Step 6: Prepare representative samples within strict token limits
        representative_samples = {}
        current_tokens = 0

        # Reserve tokens for summary and metadata (be very conservative)
        summary_text = str(summary)
        summary_tokens = calculate_context_tokens(summary_text)
        available_tokens = min(max_context_tokens - summary_tokens - 10000, 15000)  # Cap at 15K for samples

        logger.info(f"[{tool_name}] Summary uses ~{summary_tokens} tokens, {available_tokens} available for samples")

        # Add very limited samples from each focus area
        for area in focus_areas:
            if area in patterns and patterns[area] and current_tokens < available_tokens:
                samples = []
                # Limit to max 3 samples per area and truncate long messages
                for item in patterns[area][:3]:
                    # Truncate sample content to max 200 characters
                    original_content = item["content"]
                    truncated_content = original_content[:200] + "..." if len(original_content) > 200 else original_content

                    sample_item = {
                        "line_number": item["line_number"],
                        "content": truncated_content,
                        "timestamp": item.get("timestamp")
                    }

                    sample_tokens = calculate_context_tokens(truncated_content)

                    if current_tokens + sample_tokens < available_tokens:
                        samples.append(sample_item)
                        current_tokens += sample_tokens
                    else:
                        break

                if samples:
                    representative_samples[area] = samples

        # Step 7: Calculate processing metrics
        processing_time = time.time() - start_timestamp

        # Step 8: Compile final results
        results = {
            "summary": summary,
            "patterns": {k: v for k, v in patterns.items() if v},  # Only non-empty patterns
            "time_segments": {
                "segment_count": len(time_samples),
                "lines_per_segment": {k: len(v) for k, v in time_samples.items()}
            },
            "representative_samples": representative_samples,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "container_info": container_info,
                "analysis_parameters": {
                    "summary_level": summary_level,
                    "focus_areas": focus_areas,
                    "time_segments": time_segments,
                    "max_context_tokens": max_context_tokens
                },
                "processing_metrics": {
                    "total_log_lines": total_log_lines,
                    "processing_time_seconds": round(processing_time, 2),
                    "estimated_tokens_used": current_tokens + summary_tokens,
                    "token_efficiency": f"{((max_context_tokens - current_tokens - summary_tokens) / max_context_tokens * 100):.1f}% unused",
                    "patterns_extracted": sum(len(v) for v in patterns.values())
                }
            }
        }

        logger.info(f"[{tool_name}] Analysis completed successfully in {processing_time:.2f}s")
        logger.info(f"[{tool_name}] Found {results['metadata']['processing_metrics']['patterns_extracted']} pattern matches")

        # Apply truncation to ensure output fits within token limit
        results = truncate_to_token_limit(results, max_context_tokens)
        if results.get('_truncated'):
            logger.info(f"[{tool_name}] Output truncated to fit within {max_context_tokens} token limit")

        return results

    except Exception as e:
        error_msg = f"Unexpected error during log analysis: {str(e)}"
        logger.error(f"[{tool_name}] {error_msg}", exc_info=True)
        return {
            "error": error_msg,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "processing_time": time.time() - start_timestamp
            }
        }


@mcp.tool()
async def investigate_tls_certificate_issues(
    time_range: str = "24h",
    max_namespaces: int = 20,
    focus_on_system_namespaces: bool = True
) -> Dict[str, Any]:
    """
    Investigate TLS/certificate issues across the cluster with targeted search and analysis.

    Searches system namespaces for TLS error patterns and correlates with certificate events.

    Args:
        time_range: Search time range (default: "24h").
        max_namespaces: Max namespaces to search (default: 20).
        focus_on_system_namespaces: Prioritize system namespaces (default: True).

    Returns:
        Dict: TLS issues, affected pods, certificate problems, and remediation suggestions.
    """
    try:
        tool_name = "investigate_tls_certificate_issues"
        logger.info(f"[{tool_name}] Starting TLS certificate issue investigation")

        # Get namespaces to search, prioritizing system namespaces
        all_namespaces = await list_namespaces()

        if focus_on_system_namespaces:
            # Prioritize system namespaces where TLS issues commonly occur
            system_namespaces = [
                ns for ns in all_namespaces
                if any(pattern in ns for pattern in [
                    'openshift-', 'kube-', 'istio-', 'ingress', 'cert-', 'tls-',
                    'monitoring', 'logging', 'registry', 'authentication'
                ])
            ]
            # Add some Tekton/CI-CD namespaces
            tekton_ns = await detect_tekton_namespaces()
            for category in tekton_ns.values():
                system_namespaces.extend(category[:3])  # Top 3 from each category

            # Remove duplicates and limit
            target_namespaces = list(set(system_namespaces))[:max_namespaces]
        else:
            target_namespaces = all_namespaces[:max_namespaces]

        logger.info(f"[{tool_name}] Searching {len(target_namespaces)} namespaces for TLS issues")

        # Search for TLS issues across target namespaces
        tls_issues = []
        affected_pods = []
        certificate_problems = []

        for namespace in target_namespaces:
            try:
                # Get pods in namespace
                pods_info = await list_pods_in_namespace(namespace)

                if not isinstance(pods_info, list) or not pods_info:
                    continue

                # Search pod logs for TLS patterns
                for pod_info in pods_info[:3]:  # Limit to 3 pods per namespace
                    if isinstance(pod_info, dict) and 'error' not in pod_info:
                        pod_name = pod_info.get('name', '')

                        try:
                            # Use conservative log analysis focused on TLS issues
                            pod_analysis = await smart_summarize_pod_logs(
                                namespace=namespace,
                                pod_name=pod_name,
                                summary_level="brief",
                                focus_areas=["errors", "security"],
                                max_context_tokens=5000,
                                tail_lines=500  # Conservative limit
                            )

                            if "error" not in pod_analysis:
                                # Check for TLS patterns in the analysis
                                patterns = pod_analysis.get("patterns", {})
                                error_patterns = patterns.get("errors", [])

                                tls_related_errors = []
                                for error in error_patterns:
                                    error_content = error.get("content", "").lower()
                                    if any(tls_pattern in error_content for tls_pattern in [
                                        "tls", "certificate", "x509", "ssl", "handshake",
                                        "bad certificate", "certificate verify failed",
                                        "certificate has expired", "certificate authority"
                                    ]):
                                        tls_related_errors.append(error)

                                if tls_related_errors:
                                    tls_issues.extend(tls_related_errors)
                                    affected_pods.append({
                                        "namespace": namespace,
                                        "pod_name": pod_name,
                                        "pod_status": pod_info.get("status", "Unknown"),
                                        "tls_errors": len(tls_related_errors),
                                        "sample_error": tls_related_errors[0].get("content", "")[:150] + "..."
                                    })

                                    logger.info(f"[{tool_name}] Found {len(tls_related_errors)} TLS issues in pod {pod_name}")

                        except Exception as e:
                            logger.debug(f"Error analyzing pod {pod_name} in {namespace}: {e}")
                            continue

                # Also check namespace events for certificate-related events
                try:
                    events_result = await smart_get_namespace_events(
                        namespace=namespace,
                        time_period=time_range,
                        focus_areas=["errors", "warnings"],
                        max_context_tokens=3000
                    )

                    if "events" in events_result and events_result["events"]:
                        for event in events_result["events"][:5]:  # Top 5 events
                            event_content = event.get("event_string", "").lower()

                            tls_patterns = ["certificate", "tls", "x509", "ssl", "handshake"]
                            matched_pattern = None
                            for pattern in tls_patterns:
                                if pattern in event_content:
                                    matched_pattern = pattern
                                    break

                            if matched_pattern:
                                certificate_problems.append({
                                    "namespace": namespace,
                                    "event_type": "kubernetes_event",
                                    "severity": event.get("severity", "UNKNOWN"),
                                    "content": event.get("event_string", "")[:200] + "...",
                                    "timestamp": event.get("timestamp", "unknown")
                                })

                except Exception as e:
                    logger.debug(f"Error checking events in {namespace}: {e}")

            except Exception as e:
                logger.debug(f"Error processing namespace {namespace}: {e}")
                continue

        # Generate analysis and recommendations
        total_issues = len(tls_issues)
        total_affected_pods = len(affected_pods)
        total_certificate_events = len(certificate_problems)

        analysis_summary = {
            "time_range": time_range,
            "namespaces_searched": len(target_namespaces),
            "total_tls_issues": total_issues,
            "affected_pods": total_affected_pods,
            "certificate_events": total_certificate_events,
            "investigation_focus": "system_namespaces" if focus_on_system_namespaces else "all_namespaces"
        }

        # Generate specific recommendations for TLS issues
        recommendations = []
        if total_issues > 0:
            recommendations.append(f"Found {total_issues} TLS-related issues across {total_affected_pods} pods")
            recommendations.append("Check certificate expiration dates and CA trust chains")
            recommendations.append("Verify service mesh and ingress TLS configurations")

            if any("expired" in issue.get("content", "").lower() for issue in tls_issues):
                recommendations.append("Certificate expiration detected - immediate renewal required")

            if any("authority" in issue.get("content", "").lower() for issue in tls_issues):
                recommendations.append("Certificate authority issues detected - check CA trust store")

        else:
            recommendations.append("No TLS certificate issues found in searched namespaces")

        if total_affected_pods > 5:
            recommendations.append("Multiple pods affected - potential cluster-wide certificate issue")

        return {
            "analysis_summary": analysis_summary,
            "tls_issues": tls_issues[:20],  # Limit to top 20 issues
            "affected_pods": affected_pods,
            "certificate_events": certificate_problems,
            "recommendations": recommendations,
            "search_metadata": {
                "tool_optimized_for": "tls_certificate_investigations",
                "token_budget_used": "conservative",
                "search_efficiency": f"{total_issues} issues found across {len(target_namespaces)} namespaces"
            }
        }

    except Exception as e:
        logger.error(f"[{tool_name}] Error in TLS investigation: {str(e)}", exc_info=True)
        return {
            "error": f"TLS investigation failed: {str(e)}",
            "suggestion": "Try using direct pod log analysis for specific pods with TLS issues"
        }


@mcp.tool()
async def conservative_namespace_overview(
    namespace: str,
    max_pods: int = 10,
    focus_areas: Optional[List[str]] = None,
    sample_strategy: str = "smart"
) -> Dict[str, Any]:
    """
    Conservative namespace analysis optimized for large namespaces with strict token limits.

    Smart-samples critical pods (failed, high-restart, error states) for rapid issue detection.

    Args:
        namespace: Kubernetes namespace to analyze.
        max_pods: Maximum pods to analyze (default: 10).
        focus_areas: Areas to focus on (default: ["errors", "warnings"]).
        sample_strategy: "smart" for intelligent sampling, "recent" for newest pods.

    Returns:
        Dict: Analysis results with pod health, issues detected, and recommendations.
    """
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings"]

    try:
        tool_name = "conservative_namespace_overview"
        logger.info(f"[{tool_name}] Starting conservative analysis of namespace '{namespace}' (max {max_pods} pods)")

        # Ultra-conservative token budget
        max_total_tokens = 45000  # Well under any limit
        tokens_per_pod = max_total_tokens // max_pods

        # Get all pods
        pods_info = await list_pods_in_namespace(namespace)
        if isinstance(pods_info, list) and pods_info and "error" in pods_info[0]:
            return {"error": f"Failed to discover pods: {pods_info[0]['error']}"}

        total_pods = len(pods_info) if isinstance(pods_info, list) else 0
        logger.info(f"[{tool_name}] Found {total_pods} pods, will analyze top {min(max_pods, total_pods)}")

        # Report when no pods are found — may indicate RBAC restrictions
        if total_pods == 0:
            return {
                "overview": {
                    "namespace": namespace,
                    "total_pods": 0,
                    "pods_analyzed": 0,
                    "pods_with_issues": 0,
                    "critical_issues_found": 0,
                    "analysis_strategy": f"conservative sampling of 0/0 pods"
                },
                "pod_findings": {},
                "critical_issues": [],
                "recommendations": [
                    f"No pods found in namespace '{namespace}'",
                    "This may indicate RBAC restrictions preventing pod listing, or the namespace has no running workloads",
                    "Verify access with: kubectl auth can-i list pods -n " + namespace
                ],
                "conservative_metadata": {
                    "token_budget": f"<{max_total_tokens:,} tokens (conservative)",
                    "sampling_strategy": sample_strategy,
                    "coverage_ratio": "0/0",
                    "optimized_for": "large_namespaces",
                    "note": "zero_pods_detected"
                }
            }

        # Smart pod selection based on strategy
        if sample_strategy == "smart" and isinstance(pods_info, list):
            # Prioritize pods likely to have issues
            # Uses container_states (CrashLoopBackOff, ImagePullBackOff, Error, OOMKilled)
            # and restart_count from enhanced list_pods_in_namespace
            error_states = {"CrashLoopBackOff", "ImagePullBackOff", "Error", "OOMKilled", "ContainerCannotRun"}
            prioritized_pods = sorted(pods_info, key=lambda p: (
                p.get("status") == "Failed",  # Failed pods first (pod phase)
                any(state in error_states for state in p.get("container_states", [])),  # Container error states
                p.get("restart_count", 0) > 0,  # Pods with restarts
                p.get("restart_count", 0),  # Higher restart count = higher priority
                "error" in p.get("name", "").lower(),  # Names suggesting issues
                "failed" in p.get("name", "").lower(),
            ), reverse=True)
        else:
            # Recent pods strategy
            prioritized_pods = sorted(pods_info, key=lambda p: p.get("creation_timestamp") or "", reverse=True)

        # Analyze selected pods with strict token limits
        findings = {}
        issues_found = []

        for i, pod_info in enumerate(prioritized_pods[:max_pods]):
            pod_name = pod_info.get("name", "")
            pod_status = pod_info.get("status", "Unknown")

            try:
                # Ultra-conservative pod analysis
                pod_analysis = await smart_summarize_pod_logs(
                    namespace=namespace,
                    pod_name=pod_name,
                    summary_level="brief",
                    focus_areas=focus_areas,
                    max_context_tokens=tokens_per_pod,
                )

                if "error" not in pod_analysis:
                    # Extract only critical information
                    essential_info = {
                        "status": pod_status,
                        "log_lines": pod_analysis.get("metadata", {}).get("processing_metrics", {}).get("total_log_lines", 0),
                        "patterns_found": pod_analysis.get("metadata", {}).get("processing_metrics", {}).get("patterns_extracted", 0),
                        "has_errors": bool(pod_analysis.get("patterns", {}).get("errors")),
                        "has_warnings": bool(pod_analysis.get("patterns", {}).get("warnings"))
                    }

                    # Extract top issue if any
                    if pod_analysis.get("patterns", {}).get("errors"):
                        top_error = pod_analysis["patterns"]["errors"][0]
                        essential_info["top_issue"] = f"{top_error['content'][:80]}..."
                        issues_found.append(f"Pod {pod_name}: {essential_info['top_issue']}")

                    findings[pod_name] = essential_info

                logger.info(f"[{tool_name}] Analyzed pod {i+1}/{min(max_pods, total_pods)}: {pod_name}")

            except Exception as e:
                logger.warning(f"Failed to analyze pod {pod_name}: {e}")
                findings[pod_name] = {"status": pod_status, "error": str(e)}

        # Generate ultra-compact summary
        summary = {
            "namespace": namespace,
            "total_pods": total_pods,
            "pods_analyzed": len(findings),
            "pods_with_issues": len([f for f in findings.values() if f.get("has_errors") or f.get("has_warnings")]),
            "critical_issues_found": len(issues_found),
            "analysis_strategy": f"conservative sampling of {min(max_pods, total_pods)}/{total_pods} pods"
        }

        # Generate focused recommendations
        recommendations = []
        if issues_found:
            recommendations.append(f"Found {len(issues_found)} issues requiring investigation")
            recommendations.extend(issues_found[:5])  # Top 5 issues only
        else:
            recommendations.append("No critical issues detected in sampled pods")

        if total_pods > max_pods:
            recommendations.append(f"Analyzed {max_pods}/{total_pods} pods - use focused investigation for complete coverage")

        return {
            "overview": summary,
            "pod_findings": findings,
            "critical_issues": issues_found[:5],  # Top 5 only
            "recommendations": recommendations[:5],  # Top 5 only
            "conservative_metadata": {
                "token_budget": f"<{max_total_tokens:,} tokens (conservative)",
                "sampling_strategy": sample_strategy,
                "coverage_ratio": f"{len(findings)}/{total_pods}",
                "optimized_for": "large_namespaces"
            }
        }

    except Exception as e:
        logger.error(f"[{tool_name}] Error in conservative analysis: {str(e)}", exc_info=True)
        return {
            "error": f"Conservative analysis failed: {str(e)}",
            "namespace": namespace,
            "suggestion": "Try analyzing individual pods directly"
        }


@mcp.tool()
async def adaptive_namespace_investigation(
    namespace: str,
    investigation_query: str = "investigate all logs and events for potential issues",
    max_pods: int = 20,
    focus_areas: Optional[List[str]] = None,
    token_budget: int = 200000
) -> Dict[str, Any]:
    """
    Adaptive namespace investigation with progressive analysis and token budget management.

    Best for medium namespaces (5-30 pods). Prioritizes failed/error pods, correlates events.

    Args:
        namespace: Kubernetes namespace to investigate.
        investigation_query: What to investigate (default: "investigate all logs and events for potential issues").
        max_pods: Maximum pods to analyze (default: 20).
        focus_areas: Areas to focus on (default: ["errors", "warnings", "performance"]).
        token_budget: Max tokens for investigation (default: 200000).

    Returns:
        Dict: Pod analysis, event correlation, findings, and recommendations.
    """
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings", "performance"]

    # Input validation
    if not namespace or not isinstance(namespace, str):
        return {"error": "Invalid namespace parameter: must be a non-empty string"}
    namespace = namespace.strip()
    if not namespace:
        return {"error": "Namespace cannot be empty or whitespace only"}

    if not isinstance(max_pods, int) or max_pods <= 0:
        max_pods = 20  # Reset to default if invalid

    if not isinstance(token_budget, int) or token_budget <= 0:
        token_budget = 200000  # Reset to default if invalid

    try:
        tool_name = "adaptive_namespace_investigation"
        logger.info(f"[{tool_name}] Starting adaptive investigation of namespace '{namespace}'")
        logger.info(f"[{tool_name}] Query: {investigation_query}")
        logger.info(f"[{tool_name}] Token budget: {token_budget:,}, Max pods: {max_pods}")

        # Initialize adaptive processor with specified budget
        processor = AdaptiveLogProcessor(max_token_budget=token_budget)

        # Phase 1: Smart Discovery (10% of budget)
        discovery_budget = int(token_budget * 0.1)
        logger.info(f"[{tool_name}] Phase 1: Discovery (budget: {discovery_budget:,} tokens)")

        # Get all pods in namespace
        pods_info = await list_pods_in_namespace(namespace)
        if isinstance(pods_info, list) and pods_info and "error" in pods_info[0]:
            return {"error": f"Failed to discover pods: {pods_info[0]['error']}"}

        total_pods = len(pods_info) if isinstance(pods_info, list) else 0
        pods_to_analyze = min(max_pods, total_pods)

        # Early return if no pods found
        if total_pods == 0:
            logger.info(f"[{tool_name}] No pods found in namespace '{namespace}'")
            return {
                "investigation_summary": {
                    "namespace": namespace,
                    "status": "no_pods_found",
                    "message": f"No pods found in namespace '{namespace}'"
                },
                "pod_findings": {},
                "namespace_events": {},
                "critical_issues": [],
                "recommendations": ["Verify namespace exists and has running workloads"]
            }

        # Get namespace events for correlation (compressed for synthesis)
        events_result = await smart_get_namespace_events(
            namespace=namespace,
            strategy="smart_summary",
            focus_areas=focus_areas,
            max_context_tokens=discovery_budget // 2
        )

        # Validate events_result and compress for synthesis
        if not isinstance(events_result, dict):
            events_result = {"error": "Invalid events result type"}
        compressed_events = _compress_events_for_synthesis(events_result)

        # Track actual token usage from events result instead of full budget allocation
        actual_event_tokens = events_result.get("token_usage", {}).get("total_estimated", discovery_budget // 4)
        processor.record_usage(actual_event_tokens)

        # Phase 2: Intelligent Analysis (80% of budget)
        analysis_budget = int(token_budget * 0.8)
        per_pod_budget = analysis_budget // pods_to_analyze if pods_to_analyze > 0 else analysis_budget

        logger.info(f"[{tool_name}] Phase 2: Analysis (budget: {analysis_budget:,} tokens, {per_pod_budget:,} per pod)")

        findings = {}
        critical_issues = []
        pods_analyzed = 0

        # Prioritize pods for analysis
        if isinstance(pods_info, list) and pods_info:
            # Sort pods by priority (failed, high restart count, container error states)
            # Uses container_states and restart_count from enhanced list_pods_in_namespace
            error_states = {"CrashLoopBackOff", "ImagePullBackOff", "Error", "OOMKilled", "ContainerCannotRun"}
            prioritized_pods = sorted(pods_info, key=lambda p: (
                p.get("status") == "Failed",  # Failed pods first (pod phase)
                any(state in error_states for state in p.get("container_states", [])),  # Container error states
                p.get("restart_count", 0) > 0,  # Pods with restarts
                p.get("restart_count", 0),  # Higher restart count = higher priority
                p.get("name", "").endswith(("-failed", "-error")),  # Names indicating issues
            ), reverse=True)

            # Process pods in parallel batches for performance
            # Batch size balances parallelism with token budget checks
            batch_size = 4
            pods_to_process = prioritized_pods[:pods_to_analyze]
            summary_level = "brief" if pods_to_analyze > 10 else "detailed"
            max_tokens_per_pod = min(per_pod_budget, 15000)

            async def analyze_single_pod(pod_info: Dict[str, Any]) -> Dict[str, Any]:
                """Analyze a single pod and return results."""
                pod_name = pod_info.get("name", "")
                pod_status = pod_info.get("status", "Unknown")
                try:
                    pod_analysis = await smart_summarize_pod_logs(
                        namespace=namespace,
                        pod_name=pod_name,
                        summary_level=summary_level,
                        focus_areas=focus_areas,
                        max_context_tokens=max_tokens_per_pod
                    )
                    return {"pod_name": pod_name, "pod_status": pod_status, "analysis": pod_analysis, "error": None}
                except Exception as e:
                    logger.warning(f"Failed to analyze pod {pod_name}: {e}")
                    return {"pod_name": pod_name, "pod_status": pod_status, "analysis": None, "error": str(e)}

            # Process in batches
            for batch_start in range(0, len(pods_to_process), batch_size):
                # Check token budget before starting batch
                # GUARANTEE: Always process at least the first batch to ensure meaningful results
                is_first_batch = (batch_start == 0)
                actual_batch_size = min(batch_size, len(pods_to_process) - batch_start)
                batch_budget_needed = per_pod_budget * actual_batch_size

                if not is_first_batch and not processor.can_process_more(batch_budget_needed):
                    logger.info(f"Token budget exhausted - analyzed {pods_analyzed}/{pods_to_analyze} pods")
                    break

                batch = pods_to_process[batch_start:batch_start + batch_size]
                logger.info(f"[{tool_name}] Processing batch of {len(batch)} pods in parallel")

                # Run batch in parallel
                batch_results = await asyncio.gather(*[analyze_single_pod(p) for p in batch])

                # Process batch results
                for result in batch_results:
                    pod_name = result["pod_name"]
                    pod_status = result["pod_status"]

                    if result["error"]:
                        findings[pod_name] = {"status": pod_status, "error": result["error"]}
                    elif result["analysis"] and "error" not in result["analysis"]:
                        # INTELLIGENT FILTERING: Only keep essential data to prevent token overflow
                        filtered_analysis = _filter_analysis_for_synthesis(result["analysis"], focus_areas)

                        findings[pod_name] = {
                            "status": pod_status,
                            "analysis": filtered_analysis,
                            "priority_reason": "failed_pod" if pod_status == "Failed" else "normal_processing"
                        }

                        # Extract critical issues
                        if result["analysis"].get("patterns", {}).get("errors"):
                            critical_issues.extend([
                                f"Pod {pod_name}: {error['content'][:100]}..."
                                for error in result["analysis"]["patterns"]["errors"][:2]
                            ])

                    # Track actual tokens used from analysis metadata, not the full budget allocation
                    actual_pod_tokens = 0
                    if result["analysis"]:
                        actual_pod_tokens = result["analysis"].get("metadata", {}).get(
                            "processing_metrics", {}
                        ).get("estimated_tokens_used", per_pod_budget // 4)
                    processor.record_usage(max(actual_pod_tokens, 100))  # At least 100 tokens per pod
                    pods_analyzed += 1

                logger.info(f"[{tool_name}] Analyzed {pods_analyzed}/{pods_to_analyze} pods so far")

                # Early termination if many critical issues found
                if len(critical_issues) >= 10:
                    logger.info(f"Early termination: {len(critical_issues)} critical issues found")
                    break

        # Phase 3: Synthesis (10% of budget)
        synthesis_budget = int(token_budget * 0.1)
        logger.info(f"[{tool_name}] Phase 3: Synthesis (budget: {synthesis_budget:,} tokens)")

        # Generate comprehensive summary
        investigation_summary = {
            "namespace": namespace,
            "investigation_query": investigation_query,
            "total_pods_found": total_pods,
            "pods_analyzed": pods_analyzed,
            "critical_issues_found": len(critical_issues),
            "token_budget_used": f"{min(processor.get_usage_percentage(), 100.0):.1f}%",
            "adaptive_strategy": "volume-based time windowing with progressive pod analysis"
        }

        # Generate recommendations based on findings
        recommendations = []
        if critical_issues:
            recommendations.append(f"{len(critical_issues)} critical issues require immediate attention")
            recommendations.extend(critical_issues[:5])  # Top 5 issues

        if pods_analyzed < total_pods:
            recommendations.append(f"Only analyzed {pods_analyzed}/{total_pods} pods due to token constraints - consider focused investigation of remaining pods")

        if not critical_issues and pods_analyzed > 5:
            recommendations.append("No critical issues detected in analyzed pods - namespace appears healthy")

        # FINAL TOKEN SAFETY: Return compressed results to prevent context overflow
        return {
            "investigation_summary": investigation_summary,
            "pod_findings": findings,  # Already filtered per pod
            "namespace_events": compressed_events,  # Compressed events
            "critical_issues": critical_issues[:10],  # Limit to top 10 critical issues
            "recommendations": recommendations[:8],  # Limit to top 8 recommendations
            "adaptive_metadata": {
                "processing_mode": "adaptive",
                "token_efficiency": f"{(pods_analyzed * 1000 / max(1, processor.used_tokens)):.3f} pods per 1k tokens",
                "tokens_used": processor.used_tokens,
                "coverage": f"{pods_analyzed}/{total_pods} pods analyzed",
                "data_filtering": "applied to prevent token overflow",
                "synthesis_optimized": True
            }
        }

    except Exception as e:
        logger.error(f"[{tool_name}] Error in adaptive investigation: {str(e)}", exc_info=True)
        return {
            "error": f"Adaptive investigation failed: {str(e)}",
            "namespace": namespace,
            "suggestion": "Try investigating individual pods or use smaller scope"
        }


# ============================================================================
# ETCD LOGS TOOL
# ============================================================================


@mcp.tool()
def get_etcd_logs(
    tail_lines: Optional[int] = 200,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    until_time: Optional[str] = None,
    follow: bool = False,
    timestamps: bool = True,
    previous: bool = False,
    clean_logs: bool = True
) -> Dict[str, str]:
    """
    Retrieve etcd pod logs from Kubernetes/OpenShift with flexible time and line filtering.

    Auto-detects cluster type and uses appropriate namespace/label selectors.

    Args:
        tail_lines: Lines from end of logs (default: 200, None for all).
        since_seconds: Logs newer than N seconds (overrides tail_lines).
        since_time: Logs newer than RFC3339 timestamp (overrides since_seconds).
        until_time: Logs older than RFC3339 timestamp (requires since_time or since_seconds).
        follow: Stream logs in real-time (default: False).
        timestamps: Include timestamps (default: True).
        previous: Get logs from previous container instance (default: False).
        clean_logs: Clean/format logs (default: True).

    Returns:
        Dict[str, str]: Pod names as keys, logs as values.
    """
    tool_name = "get_etcd_logs_k8s_client"
    logger.info(f"Tool '{tool_name}' started with params: tail_lines={tail_lines}, "
                f"since_seconds={since_seconds}, since_time={since_time}, until_time={until_time}, "
                f"follow={follow}, timestamps={timestamps}, previous={previous}, "
                f"clean_logs={clean_logs}")

    # Validate input parameters
    parsed_since_time = None
    parsed_until_time = None

    if since_time:
        try:
            # Validate RFC3339 timestamp format
            parsed_since_time = datetime.fromisoformat(since_time.replace('Z', '+00:00'))
        except ValueError as e:
            logger.error(f"[{tool_name}] Invalid since_time format: {since_time}")
            return {"critical_error": f"Invalid since_time format '{since_time}'. Use RFC3339 format (e.g., '2024-01-15T10:30:00Z' or '2024-01-15T10:30:00'): {str(e)}"}

    if until_time:
        try:
            # Validate RFC3339 timestamp format
            parsed_until_time = datetime.fromisoformat(until_time.replace('Z', '+00:00'))
        except ValueError as e:
            logger.error(f"[{tool_name}] Invalid until_time format: {until_time}")
            return {"critical_error": f"Invalid until_time format '{until_time}'. Use RFC3339 format (e.g., '2024-01-15T11:30:00Z' or '2024-01-15T11:30:00'): {str(e)}"}

        # Ensure until_time requires since_time or since_seconds
        if not since_time and not since_seconds:
            logger.error(f"[{tool_name}] until_time requires since_time or since_seconds to be specified")
            return {"critical_error": "until_time parameter requires either since_time or since_seconds to define a time range"}

        # Ensure timestamps are enabled for accurate filtering
        if not timestamps:
            logger.warning(f"[{tool_name}] until_time specified but timestamps=False. Enabling timestamps for accurate filtering.")
            timestamps = True

        # Validate time range logic
        if parsed_since_time and parsed_until_time and parsed_until_time <= parsed_since_time:
            logger.error(f"[{tool_name}] until_time must be after since_time")
            return {"critical_error": f"Invalid time range: until_time ({until_time}) must be after since_time ({since_time})"}

    if since_seconds is not None and since_seconds < 0:
        logger.error(f"[{tool_name}] Invalid since_seconds: {since_seconds}")
        return {"critical_error": f"since_seconds must be non-negative, got: {since_seconds}"}

    if tail_lines is not None and tail_lines <= 0:
        logger.error(f"[{tool_name}] Invalid tail_lines: {tail_lines}")
        return {"critical_error": f"tail_lines must be positive, got: {tail_lines}"}

    accumulated_results: Dict[str, str] = {}
    strategies_attempted = []
    logs_successfully_fetched = False

    # --- Strategy 1: OpenShift ---
    os_namespace = "openshift-etcd"
    os_label_selector = "k8s-app=etcd"
    os_container = "etcd"
    strategies_attempted.append("OpenShift")

    logger.info(f"[{tool_name}] Attempting OpenShift etcd strategy: ns='{os_namespace}', label='{os_label_selector}'")
    try:
        pod_list_os = k8s_core_api.list_namespaced_pod(
            namespace=os_namespace,
            label_selector=os_label_selector,
            timeout_seconds=10
        )
        if pod_list_os.items:
            pod_names_os = [pod.metadata.name for pod in pod_list_os.items if pod.metadata and pod.metadata.name]
            logger.info(f"[{tool_name}] OpenShift strategy: Found {len(pod_names_os)} etcd pod(s). Fetching logs.")

            log_params = {
                'tail_lines': tail_lines,
                'since_seconds': since_seconds,
                'since_time': since_time,
                'follow': follow,
                'timestamps': timestamps,
                'previous': previous,
                'clean_logs': clean_logs
            }

            if _get_logs_with_k8s_client(k8s_core_api, pod_names_os, os_namespace, os_container, accumulated_results, log_params):
                # Apply time range filtering if until_time is specified
                if parsed_until_time:
                    logger.info(f"[{tool_name}] Applying time range filter: until {until_time}")
                    for pod_name in list(accumulated_results.keys()):
                        if not pod_name.startswith("error_") and not pod_name.startswith("info_"):
                            original_length = len(accumulated_results[pod_name])
                            accumulated_results[pod_name] = _filter_logs_by_time_range(
                                accumulated_results[pod_name],
                                parsed_until_time
                            )
                            filtered_length = len(accumulated_results[pod_name])
                            logger.info(f"[{tool_name}] Filtered logs for {pod_name}: {original_length} -> {filtered_length} characters")

                logger.info(f"[{tool_name}] Successfully fetched logs using OpenShift strategy")
                logs_successfully_fetched = True
            else:
                logger.warning(f"[{tool_name}] OpenShift strategy: Found pods but failed to fetch any logs")
        else:
            logger.info(f"[{tool_name}] OpenShift strategy: No etcd pods found")
            accumulated_results["info_openshift_no_pods"] = f"No pods found in namespace '{os_namespace}' with label '{os_label_selector}'"

    except ApiException as e:
        _handle_api_exception(e, tool_name, "OpenShift", os_namespace, os_label_selector, accumulated_results)
    except Exception as e:
        logger.error(f"[{tool_name}] OpenShift strategy: Unexpected error: {str(e)}", exc_info=True)
        accumulated_results["error_openshift_unexpected"] = str(e)

    if logs_successfully_fetched:
        return accumulated_results

    # --- Strategy 2: Standard Kubernetes ---
    kube_namespace = "kube-system"
    kube_label_selector = "component=etcd"
    kube_container = "etcd"
    strategies_attempted.append("StandardK8s")

    logger.info(f"[{tool_name}] Attempting standard Kubernetes etcd strategy: ns='{kube_namespace}', label='{kube_label_selector}'")
    standard_k8s_results: Dict[str, str] = {}

    try:
        pod_list_kube = k8s_core_api.list_namespaced_pod(
            namespace=kube_namespace,
            label_selector=kube_label_selector,
            timeout_seconds=10
        )
        if pod_list_kube.items:
            pod_names_kube = [pod.metadata.name for pod in pod_list_kube.items if pod.metadata and pod.metadata.name]
            logger.info(f"[{tool_name}] Standard K8s strategy: Found {len(pod_names_kube)} etcd pod(s). Fetching logs.")

            log_params = {
                'tail_lines': tail_lines,
                'since_seconds': since_seconds,
                'since_time': since_time,
                'follow': follow,
                'timestamps': timestamps,
                'previous': previous,
                'clean_logs': clean_logs
            }

            if _get_logs_with_k8s_client(k8s_core_api, pod_names_kube, kube_namespace, kube_container, standard_k8s_results, log_params):
                # Apply time range filtering if until_time is specified
                if parsed_until_time:
                    logger.info(f"[{tool_name}] Applying time range filter: until {until_time}")
                    for pod_name in list(standard_k8s_results.keys()):
                        if not pod_name.startswith("error_") and not pod_name.startswith("info_"):
                            original_length = len(standard_k8s_results[pod_name])
                            standard_k8s_results[pod_name] = _filter_logs_by_time_range(
                                standard_k8s_results[pod_name],
                                parsed_until_time
                            )
                            filtered_length = len(standard_k8s_results[pod_name])
                            logger.info(f"[{tool_name}] Filtered logs for {pod_name}: {original_length} -> {filtered_length} characters")

                logger.info(f"[{tool_name}] Successfully fetched logs using standard Kubernetes strategy")
                return standard_k8s_results
            else:
                logger.warning(f"[{tool_name}] Standard K8s strategy: Found pods but failed to fetch any logs")
                accumulated_results.update(standard_k8s_results)
        else:
            logger.info(f"[{tool_name}] Standard K8s strategy: No etcd pods found")
            accumulated_results["info_kube_no_pods"] = f"No pods found in namespace '{kube_namespace}' with label '{kube_label_selector}'"

    except ApiException as e:
        _handle_api_exception(e, tool_name, "StandardK8s", kube_namespace, kube_label_selector, accumulated_results)
    except Exception as e:
        logger.error(f"[{tool_name}] Standard K8s strategy: Unexpected error: {str(e)}", exc_info=True)
        accumulated_results["error_kube_unexpected"] = str(e)

    # Final summary
    has_actual_logs = any(
        not key.startswith(("error_", "info_", "critical_"))
        for key in accumulated_results
    )

    if not has_actual_logs:
        summary_message = (f"Failed to fetch etcd logs from any cluster type. "
                          f"Attempted strategies: {', '.join(strategies_attempted)}. "
                          f"Check RBAC permissions and cluster configuration.")

        if not accumulated_results:
            accumulated_results["final_summary"] = summary_message
        else:
            # Prepend summary for context
            final_results = {"final_summary": summary_message}
            final_results.update(accumulated_results)
            accumulated_results = final_results

    logger.info(f"[{tool_name}] Log fetching complete. Results: {len(accumulated_results)} entries")
    return accumulated_results


@mcp.tool()
async def stream_analyze_pod_logs(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    chunk_size: int = 5000,
    analysis_mode: str = "errors_and_warnings",
    time_window: Optional[str] = None,
    follow: bool = False,
    max_chunks: int = 50,
    since_seconds: Optional[int] = None,
    tail_lines: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    max_context_tokens: int = 50000
) -> Dict[str, Any]:
    """
    Stream and analyze pod logs in chunks with progressive pattern detection.

    Processes logs in manageable chunks for memory efficiency and real-time insights.

    Args:
        namespace: Kubernetes namespace.
        pod_name: Pod name to stream logs from.
        container_name: Specific container (if multiple).
        chunk_size: Lines per chunk (default: 5000).
        analysis_mode: "errors_only", "errors_and_warnings" (default), "full_analysis", or "custom_patterns".
        time_window: Time window for historical logs (e.g., "1h", "6h", "24h").
        follow: Stream logs in real-time (default: False).
        max_chunks: Max chunks to process (default: 50).
        since_seconds: Logs from last N seconds.
        tail_lines: Limit to last N lines.
        time_period: Time period (e.g., "1h", "30m").
        start_time: Start time (ISO format).
        end_time: End time (ISO format).
        max_context_tokens: Maximum tokens for output (default: 50000).

    Returns:
        Dict[str, Any]: Keys: chunks, overall_summary, trending_patterns, recommendations, metadata.
    """
    start_timestamp = time.time()
    tool_name = "stream_analyze_pod_logs"

    logger.info(f"[{tool_name}] Starting streaming log analysis for pod '{pod_name}' in namespace '{namespace}'")
    logger.info(f"[{tool_name}] Parameters: chunk_size={chunk_size}, analysis_mode={analysis_mode}, "
                f"follow={follow}, max_chunks={max_chunks}")

    # Validate input parameters
    if not namespace or not isinstance(namespace, str):
        error_msg = f"Invalid namespace parameter: {namespace}. Must be a non-empty string."
        logger.error(f"[{tool_name}] {error_msg}")
        return {"error": error_msg}

    if not pod_name or not isinstance(pod_name, str):
        error_msg = f"Invalid pod_name parameter: {pod_name}. Must be a non-empty string."
        logger.error(f"[{tool_name}] {error_msg}")
        return {"error": error_msg}

    if chunk_size < 1000 or chunk_size > 10000:
        logger.warning(f"[{tool_name}] chunk_size {chunk_size} out of range [1000-10000], setting to 5000")
        chunk_size = 5000

    if analysis_mode not in ["errors_only", "errors_and_warnings", "full_analysis", "custom_patterns"]:
        logger.warning(f"[{tool_name}] Invalid analysis_mode '{analysis_mode}', defaulting to 'errors_and_warnings'")
        analysis_mode = "errors_and_warnings"

    try:
        # Initialize stream processor
        processor = LogStreamProcessor(chunk_size=chunk_size, analysis_mode=analysis_mode)

        # Parse time parameters with enhanced support (prioritize new parameters over legacy time_window)
        if time_period or start_time or end_time or since_seconds:
            # Use new enhanced time parsing
            time_config = parse_time_parameters(
                since_seconds=since_seconds,
                time_period=time_period,
                start_time=start_time,
                end_time=end_time
            )
            log_params = time_config['log_params'].copy()
            time_info = time_config['time_info']
            logger.info(f"[{tool_name}] Using enhanced time configuration: {time_info}")
        else:
            # Fall back to legacy time_window for backward compatibility
            log_params = {}
            if time_window:
                # Convert time window to seconds
                time_mapping = {"1h": 3600, "6h": 21600, "24h": 86400, "1d": 86400}
                if time_window in time_mapping:
                    log_params['since_seconds'] = time_mapping[time_window]
                    logger.info(f"[{tool_name}] Using legacy time_window: {time_window}")
                else:
                    logger.warning(f"[{tool_name}] Unknown time_window '{time_window}', ignoring")

        # Handle tail_lines parameter
        if tail_lines is not None:
            log_params['tail_lines'] = tail_lines
        elif 'since_seconds' not in log_params:
            # AGGRESSIVE DEFAULT: Always limit tail_lines for streaming to prevent token overflow
            log_params['tail_lines'] = 2000
            logger.warning(f"[{tool_name}] No time constraints specified, defaulting to 2000 tail lines to prevent token overflow")

        # Retrieve logs
        logger.info(f"[{tool_name}] Retrieving logs from pod '{pod_name}'")
        raw_logs = await get_pod_logs(
            namespace=namespace,
            pod_name=pod_name,
            **log_params
        )

        if "error" in raw_logs:
            return {"error": f"Failed to retrieve logs: {raw_logs['error']}"}

        if "logs" not in raw_logs or not raw_logs["logs"]:
            return {
                "error": "No logs found for the specified pod",
                "metadata": {"pod_name": pod_name, "namespace": namespace}
            }

        # Process logs from target container
        all_log_lines = []
        container_info = {}

        for container, logs in raw_logs["logs"].items():
            if container_name and container != container_name:
                continue

            if isinstance(logs, list):
                container_lines = logs
            else:
                container_lines = str(logs).split('\n')

            container_info[container] = len(container_lines)
            all_log_lines.extend(container_lines)

        if not all_log_lines:
            return {
                "error": f"No logs found for container '{container_name}'" if container_name else "No log content found",
                "available_containers": list(raw_logs["logs"].keys())
            }

        # Remove empty lines
        all_log_lines = [line for line in all_log_lines if line.strip()]
        total_log_lines = len(all_log_lines)

        logger.info(f"[{tool_name}] Streaming analysis of {total_log_lines} log lines in chunks of {chunk_size}")

        # Stream process logs
        chunk_results = []
        lines_processed = 0
        chunks_processed = 0

        for line in all_log_lines:
            if chunks_processed >= max_chunks:
                logger.info(f"[{tool_name}] Reached max_chunks limit ({max_chunks}), stopping")
                break

            chunk_result = processor.add_line(line)
            lines_processed += 1

            if chunk_result:
                chunk_results.append(chunk_result)
                chunks_processed += 1
                logger.info(f"[{tool_name}] Processed chunk {chunks_processed}: {chunk_result['chunk_summary']['total_issues']} issues found")

        # Process any remaining lines
        final_chunk = processor.finalize()
        if final_chunk:
            chunk_results.append(final_chunk)
            chunks_processed += 1

        # Generate overall summary and trending analysis
        overall_summary = generate_streaming_summary(chunk_results)
        trending_patterns = analyze_trending_patterns(chunk_results)
        recommendations = generate_streaming_recommendations(overall_summary, trending_patterns)

        # Calculate processing metrics
        processing_time = time.time() - start_timestamp

        results = {
            "chunks": chunk_results,
            "overall_summary": overall_summary,
            "trending_patterns": trending_patterns,
            "recommendations": recommendations,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "container_info": container_info,
                "analysis_parameters": {
                    "chunk_size": chunk_size,
                    "analysis_mode": analysis_mode,
                    "follow": follow,
                    "max_chunks": max_chunks
                },
                "processing_metrics": {
                    "total_log_lines": total_log_lines,
                    "lines_processed": lines_processed,
                    "chunks_processed": chunks_processed,
                    "processing_time_seconds": round(processing_time, 2),
                    "average_chunk_processing_time": round(processing_time / max(chunks_processed, 1), 3)
                }
            }
        }

        logger.info(f"[{tool_name}] Streaming analysis completed in {processing_time:.2f}s")
        logger.info(f"[{tool_name}] Processed {chunks_processed} chunks with {overall_summary.get('total_issues', 0)} total issues")

        # Apply truncation to ensure output fits within token limit
        results = truncate_to_token_limit(results, max_context_tokens)
        if results.get('_truncated'):
            logger.info(f"[{tool_name}] Output truncated to fit within {max_context_tokens} token limit")

        return results

    except Exception as e:
        error_msg = f"Unexpected error during streaming log analysis: {str(e)}"
        logger.error(f"[{tool_name}] {error_msg}", exc_info=True)
        return {
            "error": error_msg,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "processing_time": time.time() - start_timestamp
            }
        }


@mcp.tool()
async def analyze_pod_logs_hybrid(
    namespace: str,
    pod_name: str,
    container_name: Optional[str] = None,
    strategy: str = "auto",
    request_type: str = "investigation",
    urgency: str = "medium",
    use_cache: bool = True,
    custom_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Hybrid log analyzer with intelligent strategy selection and caching.

    Automatically selects best analysis approach based on context and urgency.

    Args:
        namespace: Kubernetes namespace.
        pod_name: Pod name to analyze.
        container_name: Specific container (if multiple).
        strategy: "auto" (default), "smart_summary", "streaming", or "hybrid".
        request_type: "investigation", "troubleshooting", or "monitoring".
        urgency: "low", "medium" (default), "high", or "critical".
        use_cache: Use intelligent caching (default: True).
        custom_params: Custom parameters for strategies.

    Returns:
        Dict[str, Any]: Keys: strategy_used, analysis_results, supplementary_insights,
                        performance_metrics, recommendations, cache_info.
    """
    start_timestamp = time.time()
    tool_name = "analyze_pod_logs_hybrid"

    logger.info(f"[{tool_name}] Starting hybrid log analysis for pod '{pod_name}' in namespace '{namespace}'")
    logger.info(f"[{tool_name}] Parameters: strategy={strategy}, request_type={request_type}, "
                f"urgency={urgency}, use_cache={use_cache}")

    # Validate input parameters
    if not namespace or not isinstance(namespace, str):
        error_msg = f"Invalid namespace parameter: {namespace}. Must be a non-empty string."
        logger.error(f"[{tool_name}] {error_msg}")
        return {"error": error_msg}

    if not pod_name or not isinstance(pod_name, str):
        error_msg = f"Invalid pod_name parameter: {pod_name}. Must be a non-empty string."
        logger.error(f"[{tool_name}] {error_msg}")
        return {"error": error_msg}

    # Normalize parameters
    valid_strategies = ["auto", "smart_summary", "streaming", "hybrid"]
    if strategy not in valid_strategies:
        logger.warning(f"[{tool_name}] Invalid strategy '{strategy}', defaulting to 'auto'")
        strategy = "auto"

    valid_request_types = ["investigation", "troubleshooting", "monitoring"]
    if request_type not in valid_request_types:
        logger.warning(f"[{tool_name}] Invalid request_type '{request_type}', defaulting to 'investigation'")
        request_type = "investigation"

    valid_urgency_levels = ["low", "medium", "high", "critical"]
    if urgency not in valid_urgency_levels:
        logger.warning(f"[{tool_name}] Invalid urgency '{urgency}', defaulting to 'medium'")
        urgency = "medium"

    try:
        # Check cache first if enabled
        cache_key_params = {
            "container_name": container_name,
            "strategy": strategy,
            "request_type": request_type,
            "urgency": urgency,
            "custom_params": custom_params
        }

        cached_result = None
        if use_cache:
            cached_result = analysis_cache.get(namespace, pod_name, cache_key_params)
            if cached_result:
                logger.info(f"[{tool_name}] Returning cached result")
                cached_result["cache_info"] = {"cache_hit": True, "cache_age_seconds": time.time() - start_timestamp}
                return cached_result

        # Estimate log characteristics for strategy selection
        log_size_estimate = StrategySelector.estimate_log_size(namespace, pod_name)

        # Create analysis context
        context = LogAnalysisContext(
            log_size_estimate=log_size_estimate,
            pod_name=pod_name,
            namespace=namespace,
            request_type=request_type,
            urgency=urgency,
            time_sensitivity=(urgency in ["high", "critical"]),
            follow_up_analysis=False
        )

        # Select optimal strategy
        if strategy == "auto":
            available_strategies = [LogAnalysisStrategy.SMART_SUMMARY, LogAnalysisStrategy.STREAMING]
            selected_strategy = StrategySelector.select_strategy(context, available_strategies)
        else:
            strategy_mapping = {
                "smart_summary": LogAnalysisStrategy.SMART_SUMMARY,
                "streaming": LogAnalysisStrategy.STREAMING,
                "hybrid": LogAnalysisStrategy.HYBRID
            }
            selected_strategy = strategy_mapping[strategy]

        logger.info(f"[{tool_name}] Selected strategy: {selected_strategy.value} based on log_size={log_size_estimate}, "
                   f"urgency={urgency}, request_type={request_type}")

        # Prepare strategy-specific parameters
        strategy_params = custom_params.copy() if custom_params else {}
        strategy_params.update({
            "namespace": namespace,
            "pod_name": pod_name,
            "container_name": container_name
        })

        # Execute primary strategy
        primary_results = None
        supplementary_results = {}

        if selected_strategy == LogAnalysisStrategy.SMART_SUMMARY:
            # Configure smart summary based on context
            if urgency in ["high", "critical"]:
                strategy_params.update({
                    "summary_level": "brief",
                    "max_context_tokens": 5000,
                    "time_segments": 3
                })
            elif urgency == "low":
                strategy_params.update({
                    "summary_level": "comprehensive",
                    "max_context_tokens": 15000,
                    "time_segments": 10
                })
            else:
                strategy_params.update({
                    "summary_level": "detailed",
                    "max_context_tokens": 8000,
                    "time_segments": 5
                })

            primary_results = await smart_summarize_pod_logs(**strategy_params)

        elif selected_strategy == LogAnalysisStrategy.STREAMING:
            # Configure streaming based on context
            if urgency == "critical":
                strategy_params.update({
                    "chunk_size": 1000,
                    "analysis_mode": "errors_only",
                    "max_chunks": 20
                })
            elif request_type == "troubleshooting":
                strategy_params.update({
                    "chunk_size": 3000,
                    "analysis_mode": "errors_and_warnings",
                    "max_chunks": 30
                })
            else:
                strategy_params.update({
                    "chunk_size": 5000,
                    "analysis_mode": "full_analysis",
                    "max_chunks": 50
                })

            primary_results = await stream_analyze_pod_logs(**strategy_params)

        elif selected_strategy == LogAnalysisStrategy.HYBRID:
            # Run both strategies and combine results
            summary_params = strategy_params.copy()
            summary_params.update({
                "summary_level": "detailed",
                "max_context_tokens": 20000,
                "time_segments": 8
            })

            streaming_params = strategy_params.copy()
            streaming_params.update({
                "chunk_size": 4000,
                "analysis_mode": "errors_and_warnings",
                "max_chunks": 25
            })

            # Run both analyses
            summary_result = await smart_summarize_pod_logs(**summary_params)
            streaming_result = await stream_analyze_pod_logs(**streaming_params)

            # Combine results
            primary_results = {
                "combined_analysis": {
                    "summary_analysis": summary_result,
                    "streaming_analysis": streaming_result
                },
                "hybrid_insights": combine_analysis_results(summary_result, streaming_result)
            }

        # Generate supplementary insights based on primary results
        supplementary_results = generate_supplementary_insights(primary_results, context)

        # Generate performance metrics
        processing_time = time.time() - start_timestamp
        performance_metrics = {
            "processing_time_seconds": round(processing_time, 2),
            "strategy_selected": selected_strategy.value,
            "strategy_selection_reason": get_strategy_selection_reason(context, selected_strategy),
            "log_size_estimate": log_size_estimate,
            "cache_enabled": use_cache
        }

        # Generate recommendations based on strategy and results
        recommendations = generate_hybrid_recommendations(primary_results, context, selected_strategy)

        # Compile final results
        results = {
            "strategy_used": {
                "strategy": selected_strategy.value,
                "selection_reason": performance_metrics["strategy_selection_reason"],
                "context": {
                    "request_type": request_type,
                    "urgency": urgency,
                    "log_size_estimate": log_size_estimate
                }
            },
            "analysis_results": primary_results,
            "supplementary_insights": supplementary_results,
            "performance_metrics": performance_metrics,
            "recommendations": recommendations,
            "cache_info": {
                "cache_hit": False,
                "cache_enabled": use_cache,
                "cache_key_generated": use_cache
            }
        }

        # Cache results if enabled
        if use_cache and primary_results and "error" not in primary_results:
            analysis_cache.set(namespace, pod_name, cache_key_params, results)

        logger.info(f"[{tool_name}] Hybrid analysis completed in {processing_time:.2f}s using {selected_strategy.value}")

        return results

    except Exception as e:
        error_msg = f"Unexpected error during hybrid log analysis: {str(e)}"
        logger.error(f"[{tool_name}] {error_msg}", exc_info=True)
        return {
            "error": error_msg,
            "metadata": {
                "pod_name": pod_name,
                "namespace": namespace,
                "strategy_attempted": strategy,
                "processing_time": time.time() - start_timestamp
            }
        }


@mcp.tool()
async def progressive_event_analysis(
    namespace: str,
    analysis_level: str = "overview",
    time_period: Optional[str] = None,
    event_filters: Optional[Dict[str, Any]] = None,
    seed_event_id: Optional[str] = None,
    focus_areas: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Progressive event analysis with multiple detail levels and correlation detection.

    Args:
        namespace: Kubernetes namespace to analyze.
        analysis_level: "overview", "detailed", "correlation", or "deep_dive" (default: "overview").
        time_period: Time window (e.g., "2h", "4h", "1d").
        event_filters: Filters like {"severity": ["CRITICAL"], "category": ["FAILURE"]}.
        seed_event_id: Event ID for correlation analysis.
        focus_areas: Areas to emphasize (default: ["errors", "warnings", "failures"]).

    Returns:
        Dict: Analysis results based on selected level.
    """
    # Handle mutable default argument - set default inside function
    if focus_areas is None:
        focus_areas = ["errors", "warnings", "failures"]

    tool_name = "progressive_event_analysis"
    logger.info(f"[{tool_name}] Starting {analysis_level} analysis for namespace '{namespace}'")

    try:
        # First get events using smart handler
        smart_result = await smart_get_namespace_events(
            namespace=namespace,
            time_period=time_period,
            strategy="smart_summary",
            focus_areas=focus_areas,
            include_summary=False  # We'll generate our own analysis
        )

        if "error" in smart_result:
            return {"error": f"Failed to fetch events: {smart_result['error']}"}

        # Extract classified events
        classified_events = []
        for event in smart_result.get("events", []):
            classified_events.append({
                "event_string": event.get("event_string", ""),
                "severity": event.get("severity"),
                "category": event.get("category"),
                "relevance_score": event.get("relevance_score", 0),
                "timestamp": datetime.fromisoformat(event.get("timestamp", datetime.now().isoformat())),
                "token_estimate": event.get("token_estimate", 0)
            })

        if not classified_events:
            # Progressive fallback: try wider time windows before giving up
            fallback_periods = ["12h", "24h", "7d"]
            original_period = time_period or "default"
            for fallback_period in fallback_periods:
                if fallback_period == time_period:
                    continue
                logger.info(f"[{tool_name}] No events with {original_period}, trying {fallback_period}")
                fallback_result = await smart_get_namespace_events(
                    namespace=namespace,
                    time_period=fallback_period,
                    strategy="smart_summary",
                    focus_areas=focus_areas,
                    include_summary=False
                )
                for event in fallback_result.get("events", []):
                    classified_events.append({
                        "event_string": event.get("event_string", ""),
                        "severity": event.get("severity"),
                        "category": event.get("category"),
                        "relevance_score": event.get("relevance_score", 0),
                        "timestamp": datetime.fromisoformat(event.get("timestamp", datetime.now().isoformat())),
                        "token_estimate": event.get("token_estimate", 0)
                    })
                if classified_events:
                    time_period = fallback_period
                    logger.info(f"[{tool_name}] Found {len(classified_events)} events with {fallback_period} fallback")
                    break

            if not classified_events:
                return {
                    "namespace": namespace,
                    "analysis_level": analysis_level,
                    "message": "No events found for analysis",
                    "time_periods_tried": [original_period] + fallback_periods,
                    "suggestion": "No events in this namespace within the last 7 days. The namespace may not generate Kubernetes events, or events have been garbage collected."
                }

        # Initialize progressive analyzer
        analyzer = ProgressiveEventAnalyzer(classified_events)

        # Perform analysis based on level
        analysis_result = {
            "namespace": namespace,
            "analysis_level": analysis_level,
            "total_events": len(classified_events),
            "time_period": time_period,
            "generated_at": datetime.now().isoformat()
        }

        if analysis_level == "overview":
            analysis_result["overview"] = analyzer.get_overview()

        elif analysis_level == "detailed":
            analysis_result["detailed_analysis"] = analyzer.get_detailed_analysis(event_filters)

        elif analysis_level == "correlation":
            analysis_result["correlation_analysis"] = analyzer.get_correlation_analysis(seed_event_id)

        elif analysis_level == "deep_dive":
            analysis_result["overview"] = analyzer.get_overview()
            analysis_result["detailed_analysis"] = analyzer.get_detailed_analysis(event_filters)
            analysis_result["correlation_analysis"] = analyzer.get_correlation_analysis(seed_event_id)
            analysis_result["deep_dive_insights"] = [
                "Complete multi-level analysis performed",
                "Review all sections for comprehensive understanding",
                "Use correlation data for root cause analysis"
            ]

        else:
            return {"error": f"Unknown analysis level: {analysis_level}"}

        logger.info(f"[{tool_name}] Completed {analysis_level} analysis successfully")
        return analysis_result

    except Exception as e:
        logger.error(f"[{tool_name}] Error in progressive analysis: {str(e)}", exc_info=True)
        return {
            "error": f"Progressive analysis failed: {str(e)}",
            "suggestion": "Try a simpler analysis level like 'overview'"
        }


@mcp.tool()
async def advanced_event_analytics(
    namespace: str,
    time_period: Optional[str] = None,
    include_ml_patterns: bool = True,
    include_log_correlation: bool = True,
    include_metrics_correlation: bool = True,
    include_runbook_suggestions: bool = True,
    analysis_depth: str = "comprehensive"
) -> Dict[str, Any]:
    """
    Advanced ML-powered event analytics with log/metrics integration and runbook suggestions.

    Args:
        namespace: Kubernetes namespace to analyze.
        time_period: Time window (e.g., "4h", "1d", "12h").
        include_ml_patterns: Enable ML pattern detection (default: True).
        include_log_correlation: Correlate with log data (default: True).
        include_metrics_correlation: Correlate with metrics (default: True).
        include_runbook_suggestions: Generate runbook suggestions (default: True).
        analysis_depth: "basic", "comprehensive" (default), or "deep".

    Returns:
        Dict: Advanced analytics with ML insights, correlations, and runbook suggestions.
    """

    tool_name = "advanced_event_analytics"

    # Validate analysis_depth
    valid_depths = {"basic", "comprehensive", "deep"}
    if analysis_depth not in valid_depths:
        return {"error": f"Invalid analysis_depth '{analysis_depth}'. Must be one of: {', '.join(sorted(valid_depths))}"}

    logger.info(f"[{tool_name}] Starting advanced analytics for namespace '{namespace}'")

    try:
        # Step 1: Get base event data — scale progressive analysis to depth
        depth_to_level = {"basic": "overview", "comprehensive": "detailed", "deep": "deep_dive"}
        base_level = depth_to_level.get(analysis_depth, "detailed")
        base_result = await progressive_event_analysis(
            namespace=namespace,
            analysis_level=base_level,
            time_period=time_period
        )

        if "error" in base_result:
            return {"error": f"Failed to get base event data: {base_result['error']}"}

        # Extract events from progressive analysis results (avoid duplicate API calls)
        events_data = []
        if "overview" in base_result:
            # Use events already fetched by progressive_event_analysis
            overview_events = base_result.get("overview", {}).get("events", [])
            for event in overview_events:
                events_data.append({
                    "event_string": event.get("event_string", ""),
                    "severity": event.get("severity"),
                    "category": event.get("category"),
                    "timestamp": datetime.fromisoformat(event.get("timestamp", datetime.now().isoformat())),
                    "relevance_score": event.get("relevance_score", 0)
                })

        if not events_data:
            # Fallback: even without events, try log and metrics correlation if enabled
            fallback_result = {
                "namespace": namespace,
                "analysis_type": "advanced_analytics",
                "analysis_depth": analysis_depth,
                "total_events_analyzed": 0,
                "time_period": time_period,
                "generated_at": datetime.now().isoformat(),
                "note": "No Kubernetes events found; performing log/metrics-only analysis"
            }
            has_fallback_data = False

            if include_log_correlation:
                try:
                    log_integrator = LogMetricsIntegrator([])
                    log_correlation = await log_integrator.correlate_with_logs(namespace, time_period or "2h")
                    fallback_result["log_correlation"] = log_correlation
                    has_fallback_data = True
                except Exception as e:
                    logger.warning(f"[{tool_name}] Log correlation fallback failed: {e}")

            if include_metrics_correlation:
                try:
                    if not include_log_correlation:
                        log_integrator = LogMetricsIntegrator([])
                    metrics_correlation = await log_integrator.correlate_with_metrics(namespace)
                    fallback_result["metrics_correlation"] = metrics_correlation
                    has_fallback_data = True
                except Exception as e:
                    logger.warning(f"[{tool_name}] Metrics correlation fallback failed: {e}")

            if include_runbook_suggestions:
                fallback_result["runbook_suggestions"] = [
                    "No events detected — check if event generation is working in this namespace",
                    "Verify namespace has active workloads: kubectl get pods -n " + namespace,
                    "Check if events are being garbage collected prematurely"
                ]
                has_fallback_data = True

            if not has_fallback_data:
                fallback_result["message"] = "No events available and fallback analysis produced no data"
                fallback_result["suggestion"] = "Try a longer time period or different namespace"

            return fallback_result

        # Initialize analysis result
        analytics_result = {
            "namespace": namespace,
            "analysis_type": "advanced_analytics",
            "analysis_depth": analysis_depth,
            "total_events_analyzed": len(events_data),
            "time_period": time_period,
            "generated_at": datetime.now().isoformat(),
            "base_analysis": base_result
        }

        # Step 2: ML-powered pattern detection
        if include_ml_patterns:
            logger.info(f"[{tool_name}] Running ML pattern detection")
            ml_detector = MLPatternDetector(events_data)
            ml_patterns = ml_detector.detect_patterns()
            analytics_result["ml_patterns"] = ml_patterns
        else:
            analytics_result["ml_patterns"] = {"disabled": True}

        # Step 3: Log correlation
        if include_log_correlation:
            logger.info(f"[{tool_name}] Correlating with log data")
            log_integrator = LogMetricsIntegrator(events_data)
            log_correlation = await log_integrator.correlate_with_logs(namespace, time_period or "2h")
            analytics_result["log_correlation"] = log_correlation

        # Step 4: Metrics correlation
        if include_metrics_correlation:
            logger.info(f"[{tool_name}] Correlating with metrics")
            if not include_log_correlation:
                log_integrator = LogMetricsIntegrator(events_data)
            metrics_correlation = await log_integrator.correlate_with_metrics(namespace)
            analytics_result["metrics_correlation"] = metrics_correlation

        # Step 5: Runbook suggestions
        if include_runbook_suggestions:
            logger.info(f"[{tool_name}] Generating runbook suggestions")
            runbook_engine = RunbookSuggestionEngine(
                events_data,
                analytics_result.get("ml_patterns", {})
            )
            runbook_suggestions = runbook_engine.suggest_runbooks()
            analytics_result["runbook_suggestions"] = runbook_suggestions

        # Step 6: Generate comprehensive insights
        analytics_result["comprehensive_insights"] = await generate_comprehensive_insights(
            analytics_result,
            analysis_depth
        )

        # Step 7: Risk assessment and recommendations
        analytics_result["risk_assessment"] = assess_overall_risk(analytics_result)
        analytics_result["strategic_recommendations"] = generate_strategic_recommendations(analytics_result)

        logger.info(f"[{tool_name}] Advanced analytics completed successfully")
        return analytics_result

    except Exception as e:
        logger.error(f"[{tool_name}] Error in advanced analytics: {str(e)}", exc_info=True)
        return {
            "error": f"Advanced analytics failed: {str(e)}",
            "suggestion": "Try with reduced analysis scope or shorter time period"
        }


@mcp.tool()
async def automated_triage_rca_report_generator(
    failure_identifier: str,
    namespace: Optional[str] = None,
    investigation_depth: str = "standard",
    include_related_failures: bool = True,
    time_window: str = "2h",
    generate_timeline: bool = True,
    include_remediation: bool = True
) -> Dict[str, Any]:
    """
    Generate automated Root Cause Analysis (RCA) report for pipeline/pod failures.

    Performs log analysis, resource checks, event correlation, and provides remediation suggestions.

    Args:
        failure_identifier: Pipeline run name, pod name, or failure event ID.
        namespace: Optional namespace where the failure occurred. If not provided, searches across detected CI/CD namespaces.
        investigation_depth: "quick", "standard" (default), or "deep".
        include_related_failures: Analyze related recent failures (default: True).
        time_window: Time window for related events (default: "2h").
        generate_timeline: Generate event timeline (default: True).
        include_remediation: Include remediation steps (default: True).

    Returns:
        Dict: RCA report with summary, timeline, root cause, diagnostics, and remediation.
    """
    # Validate investigation_depth
    valid_depths = {"quick", "standard", "deep"}
    if investigation_depth not in valid_depths:
        return {"error": f"Invalid investigation_depth '{investigation_depth}'. Must be one of: {', '.join(sorted(valid_depths))}"}

    try:
        logger.info(f"Starting automated RCA for failure: {failure_identifier}")
        investigation_start = datetime.now().isoformat()

        # Initialize report structure
        report = {
            "investigation_summary": {
                "failure_id": failure_identifier,
                "investigation_started": investigation_start,
                "failure_type": "Unknown",
                "severity": "Medium",
                "root_cause_confidence": 0.0
            },
            "failure_timeline": [],
            "root_cause_analysis": {
                "primary_cause": {},
                "contributing_factors": [],
                "affected_systems": []
            },
            "diagnostic_data": {
                "logs_analyzed": {},
                "resource_analysis": {},
                "configuration_issues": [],
                "dependency_failures": []
            },
            "remediation_plan": {
                "immediate_actions": [],
                "preventive_measures": []
            },
            "related_incidents": []
        }

        # Parse time window
        time_hours = 2
        if time_window.endswith('h'):
            time_hours = int(time_window[:-1])
        elif time_window.endswith('m'):
            time_hours = int(time_window[:-1]) / 60

        # Step 1: Identify failure type and locate namespace
        failure_context = await identify_failure_context(failure_identifier, detect_tekton_namespaces, k8s_custom_api, k8s_core_api, logger, namespace)
        if not failure_context["found"]:
            report["investigation_summary"]["failure_type"] = "Not Found"
            report["investigation_summary"]["severity"] = "Low"
            report["investigation_summary"]["search_note"] = failure_context.get(
                "search_note", f"Resource '{failure_identifier}' not found in any namespace"
            )
            report["investigation_summary"]["namespaces_searched"] = failure_context.get("namespaces_searched", [])
            report["remediation_plan"] = {
                "immediate_actions": [
                    f"Verify the resource name '{failure_identifier}' is correct",
                    "The resource may have been garbage collected by Tekton pruner",
                    "Try using the query_kubearchive tool to retrieve archived logs",
                    "Check if there are related events: kubectl get events -n <namespace> --field-selector involvedObject.name=<name>",
                ],
                "preventive_measures": [
                    "Investigate sooner after failures (before GC runs)",
                    "Consider increasing Tekton resource retention period",
                ]
            }
            return report

        # Handle GC'd resources found via events
        gc_detected = failure_context.get("gc_detected", False)
        target_namespace = failure_context["namespace"]
        failure_type = failure_context["type"]
        report["investigation_summary"]["failure_type"] = failure_type

        if gc_detected:
            report["investigation_summary"]["gc_detected"] = True
            report["investigation_summary"]["note"] = (
                f"Resource was garbage collected but {failure_context.get('event_count', 0)} "
                f"event(s) were found. Analysis is based on available event data."
            )
            # Populate timeline from the events we found
            gc_events = failure_context.get("events", [])
            report["failure_timeline"] = [
                {
                    "timestamp": ev.get("last_timestamp", "unknown"),
                    "event": ev.get("reason", "unknown"),
                    "message": ev.get("message", ""),
                    "type": ev.get("type", "Normal"),
                    "source": "kubernetes_event"
                }
                for ev in gc_events
            ]

        # Step 2: Core failure analysis based on type
        if failure_type == "pipelinerun":
            primary_analysis = await analyze_pipeline_failure(target_namespace, failure_identifier, investigation_depth, analyze_failed_pipeline, analyze_pipeline_performance, get_pod_logs, analyze_logs, detect_log_anomalies, analyze_pipeline_dependencies, logger)
        elif failure_type == "pod":
            primary_analysis = await analyze_pod_failure(target_namespace, failure_identifier, investigation_depth, k8s_core_api, get_pod_logs, analyze_logs, detect_log_anomalies, smart_get_namespace_events, logger)
        else:
            primary_analysis = await analyze_generic_failure(target_namespace, failure_identifier, investigation_depth, smart_get_namespace_events, logger)

        # Step 3: Build failure timeline
        timeline_events = []
        if generate_timeline:
            timeline_events = await build_failure_timeline(target_namespace, failure_identifier, time_hours, smart_get_namespace_events, logger)
            if timeline_events:
                report["failure_timeline"] = timeline_events
            # If no new timeline events found but we have GC events, keep those
            elif not report.get("failure_timeline"):
                report["failure_timeline"] = []

        # Step 4: Correlate with related failures
        related_failures = []
        if include_related_failures:
            related_failures = await find_related_failures(target_namespace, failure_identifier, time_hours, investigation_depth, list_pipelineruns, logger)
            report["related_incidents"] = related_failures

        # Step 5: Advanced correlation and root cause analysis
        root_cause_data = await perform_advanced_rca(
            primary_analysis, timeline_events, related_failures, investigation_depth, categorize_errors, logger
        )

        # Step 6: Resource and configuration analysis
        resource_analysis = await analyze_resource_constraints(target_namespace, failure_identifier, k8s_core_api, logger)
        config_analysis = await analyze_configuration_issues(target_namespace, failure_identifier, logger)

        # Step 7: Compile comprehensive analysis
        report["root_cause_analysis"] = root_cause_data["root_cause_analysis"]
        report["diagnostic_data"] = {
            "logs_analyzed": primary_analysis.get("logs_analyzed", {}),
            "resource_analysis": resource_analysis,
            "configuration_issues": config_analysis,
            "dependency_failures": root_cause_data.get("dependency_failures", [])
        }

        # Step 8: Generate remediation plan
        if include_remediation:
            remediation_plan = await generate_remediation_plan(
                root_cause_data, primary_analysis, resource_analysis, config_analysis, recommend_actions, logger
            )
            report["remediation_plan"] = remediation_plan

        # Step 9: Calculate confidence and severity
        confidence_score = calculate_confidence_score(primary_analysis, root_cause_data, timeline_events)
        severity_analysis = assess_failure_severity(primary_analysis, root_cause_data, resource_analysis, config_analysis)
        severity = severity_analysis["severity_level"]

        report["investigation_summary"]["root_cause_confidence"] = confidence_score
        report["investigation_summary"]["severity"] = severity

        logger.info(f"RCA completed for {failure_identifier} with confidence: {confidence_score:.2f}")
        return report

    except Exception as e:
        logger.error(f"Error in automated RCA for {failure_identifier}: {str(e)}", exc_info=True)
        return {
            "investigation_summary": {
                "failure_id": failure_identifier,
                "investigation_started": datetime.now().isoformat(),
                "failure_type": "Error",
                "severity": "High",
                "root_cause_confidence": 0.0
            },
            "failure_timeline": [],
            "root_cause_analysis": {"primary_cause": {"error": str(e)}, "contributing_factors": [], "affected_systems": []},
            "diagnostic_data": {"logs_analyzed": {}, "resource_analysis": {}, "configuration_issues": [], "dependency_failures": []},
            "remediation_plan": {"immediate_actions": ["Check tool logs for detailed error information"], "preventive_measures": []},
            "related_incidents": []
        }


@mcp.tool()
async def check_cluster_certificate_health(
    warning_threshold_days: int = 30,
    critical_threshold_days: int = 7,
    include_system_certs: bool = True,
    namespaces: Optional[List[str]] = None,
    certificate_types: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Scan for expiring certificates across the cluster to prevent service disruptions.

    Scans TLS secrets, system certificates, and provides renewal recommendations.

    Args:
        warning_threshold_days: Days before expiration for warning (default: 30).
        critical_threshold_days: Days before expiration for critical alert (default: 7).
        include_system_certs: Include system certificates (default: True).
        namespaces: Namespaces to scan (default: all accessible).
        certificate_types: Types to check: "tls", "ca", "client", "server" (default: all).

    Returns:
        Dict: Certificate health with expiration timeline, recommendations, and security findings.
    """
    try:
        logger.info(f"Starting cluster certificate health scan with thresholds: warning={warning_threshold_days}d, critical={critical_threshold_days}d")

        # Initialize result structure
        result = {
            "scan_summary": {
                "total_certificates": 0,
                "healthy_certificates": 0,
                "warning_certificates": 0,
                "critical_certificates": 0,
                "expired_certificates": 0,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "namespaces_scanned": 0,
                "namespaces_skipped_rbac": 0,
                "namespaces_total": 0
            },
            "certificate_details": [],
            "system_certificates": [],
            "expiration_timeline": [],
            "renewal_recommendations": [],
            "security_findings": [],
            "certificate_authorities": [],
            "scan_coverage": {
                "scanned_namespaces": [],
                "skipped_namespaces_rbac": []
            }
        }

        # Determine namespaces to scan
        target_namespaces = namespaces or []
        if not target_namespaces:
            # Get all accessible namespaces
            try:
                all_ns = k8s_core_api.list_namespace()
                target_namespaces = [ns.metadata.name for ns in all_ns.items if ns.metadata and ns.metadata.name]
                logger.info(f"Scanning all {len(target_namespaces)} accessible namespaces")
            except ApiException as e:
                logger.warning(f"Could not list all namespaces, using default set: {e.reason}")
                target_namespaces = ['default', 'kube-system', 'openshift-config', 'openshift-ingress']

        # Set default certificate types
        if not certificate_types:
            certificate_types = ["tls", "ca", "client", "server"]

        certificates_found = []
        ca_certificates = {}
        scanned_namespaces = []
        skipped_namespaces_rbac = []

        # Scan for TLS secrets in each namespace
        for namespace in target_namespaces:
            try:
                logger.debug(f"Scanning namespace: {namespace}")
                secrets = k8s_core_api.list_namespaced_secret(namespace)
                scanned_namespaces.append(namespace)

                for secret in secrets.items:
                    if not secret.data:
                        continue

                    # Check if secret contains certificate data
                    cert_keys = ['tls.crt', 'ca.crt', 'cert', 'certificate', 'client.crt', 'server.crt']

                    for key in cert_keys:
                        if key in secret.data:
                            try:
                                # Decode base64 certificate data
                                cert_data = base64.b64decode(secret.data[key]).decode('utf-8')

                                # Handle certificate chains (multiple certificates)
                                cert_blocks = cert_data.split('-----END CERTIFICATE-----')

                                for i, cert_block in enumerate(cert_blocks):
                                    if '-----BEGIN CERTIFICATE-----' in cert_block:
                                        full_cert = cert_block + '-----END CERTIFICATE-----'
                                        cert_info = parse_certificate(full_cert)

                                        if cert_info:
                                            cert_details = {
                                                "certificate_info": {
                                                    "name": f"{secret.metadata.name}_{key}_{i}" if i > 0 else f"{secret.metadata.name}_{key}",
                                                    "namespace": namespace,
                                                    "secret_name": secret.metadata.name,
                                                    "key_name": key,
                                                    "type": secret.type or "Opaque"
                                                },
                                                "certificate_data": cert_info,
                                                "validity": {
                                                    "not_before": cert_info['not_before'],
                                                    "not_after": cert_info['not_after'],
                                                    "days_remaining": cert_info['days_remaining'],
                                                    "status": categorize_certificate_status(
                                                        cert_info['days_remaining'],
                                                        warning_threshold_days,
                                                        critical_threshold_days
                                                    )
                                                },
                                                "usage": {
                                                    "is_ca": cert_info.get('is_ca', False) or 'ca' in key.lower(),
                                                    "is_client": 'client' in key.lower(),
                                                    "is_server": 'server' in key.lower() or 'tls' in key.lower(),
                                                    "san_domains": cert_info.get('san', [])
                                                },
                                                "chain_validation": {
                                                    "is_self_signed": cert_info.get('subject_cn') == cert_info.get('issuer_cn'),
                                                    "issuer": cert_info.get('issuer_cn', 'Unknown'),
                                                    "chain_length": len(cert_blocks) if len(cert_blocks) > 1 else 1
                                                }
                                            }

                                            certificates_found.append(cert_details)

                                            # Track CA certificates
                                            if cert_details["usage"]["is_ca"]:
                                                ca_name = cert_info.get('subject_cn', 'Unknown CA')
                                                if ca_name not in ca_certificates:
                                                    ca_certificates[ca_name] = {
                                                        "ca_name": ca_name,
                                                        "issued_certificates": 0,
                                                        "ca_expiry": cert_info['not_after'],
                                                        "trust_status": "trusted" if not cert_details["chain_validation"]["is_self_signed"] else "self-signed"
                                                    }
                                                ca_certificates[ca_name]["issued_certificates"] += 1

                            except Exception as e:
                                logger.debug(f"Could not parse certificate {key} in secret {secret.metadata.name}: {e}")
                                continue

            except ApiException as e:
                if e.status == 403:
                    logger.debug(f"Access denied to namespace {namespace}: {e.reason}")
                    if namespace not in skipped_namespaces_rbac:
                        skipped_namespaces_rbac.append(namespace)
                else:
                    logger.warning(f"Error scanning namespace {namespace}: {e.reason}")
                continue

        # Process OpenShift system certificates if requested
        # Always scan system cert namespaces when include_system_certs=True,
        # even when specific namespaces were provided (they may have been RBAC-blocked)
        if include_system_certs:
            try:
                # Try to get OpenShift cluster certificates
                system_cert_namespaces = [
                    'openshift-config',
                    'openshift-ingress',
                    'openshift-ingress-operator',
                    'openshift-kube-apiserver',
                    'openshift-etcd'
                ]

                for sys_ns in system_cert_namespaces:
                    if sys_ns not in scanned_namespaces:
                        try:
                            secrets = k8s_core_api.list_namespaced_secret(sys_ns)
                            scanned_namespaces.append(sys_ns)
                            for secret in secrets.items:
                                if secret.data:
                                    for key in ['tls.crt', 'ca.crt']:
                                        if key in secret.data:
                                            try:
                                                # Properly parse the certificate
                                                cert_data = base64.b64decode(secret.data[key]).decode('utf-8')
                                                if '-----BEGIN CERTIFICATE-----' in cert_data:
                                                    cert_info = parse_certificate(cert_data)
                                                    if cert_info:
                                                        status = categorize_certificate_status(
                                                            cert_info['days_remaining'],
                                                            warning_threshold_days,
                                                            critical_threshold_days
                                                        )
                                                        result["system_certificates"].append({
                                                            "component": sys_ns.replace('openshift-', ''),
                                                            "certificate_purpose": secret.metadata.name,
                                                            "subject_cn": cert_info.get('subject_cn', 'Unknown'),
                                                            "expiry_date": cert_info.get('not_after', 'Unknown'),
                                                            "days_remaining": cert_info.get('days_remaining', 0),
                                                            "status": status,
                                                            "auto_renewal": True,
                                                            "renewal_mechanism": "OpenShift Certificate Operator"
                                                        })
                                            except Exception as parse_err:
                                                logger.debug(f"Could not parse system cert {secret.metadata.name}/{key}: {parse_err}")
                        except ApiException as e:
                            if e.status == 403:
                                if sys_ns not in skipped_namespaces_rbac:
                                    skipped_namespaces_rbac.append(sys_ns)
                            continue

            except Exception as e:
                logger.debug(f"Could not scan system certificates: {e}")

        # Update scan summary
        total_certs = len(certificates_found)
        healthy_count = len([c for c in certificates_found if c["validity"]["status"] == "healthy"])
        warning_count = len([c for c in certificates_found if c["validity"]["status"] == "warning"])
        critical_count = len([c for c in certificates_found if c["validity"]["status"] == "critical"])
        expired_count = len([c for c in certificates_found if c["validity"]["status"] == "expired"])

        result["scan_summary"].update({
            "total_certificates": total_certs,
            "healthy_certificates": healthy_count,
            "warning_certificates": warning_count,
            "critical_certificates": critical_count,
            "expired_certificates": expired_count,
            "namespaces_scanned": len(scanned_namespaces),
            "namespaces_skipped_rbac": len(skipped_namespaces_rbac),
            "namespaces_total": len(target_namespaces)
        })

        # Update scan coverage
        result["scan_coverage"] = {
            "scanned_namespaces": scanned_namespaces,
            "skipped_namespaces_rbac": skipped_namespaces_rbac[:50]  # Limit to first 50 to avoid huge output
        }

        # Add RBAC warning if many namespaces were skipped
        if len(skipped_namespaces_rbac) > len(scanned_namespaces):
            result["security_findings"].append({
                "type": "rbac_limitation",
                "severity": "info",
                "message": f"RBAC restrictions prevented scanning {len(skipped_namespaces_rbac)} namespaces. "
                          f"Only {len(scanned_namespaces)} namespaces were accessible. "
                          "Consider granting 'list secrets' permission for comprehensive certificate scanning."
            })

        # Filter certificates by type if specified
        if certificate_types and "all" not in certificate_types:
            filtered_certs = []
            for cert in certificates_found:
                cert_usage = cert["usage"]
                if ("tls" in certificate_types and cert_usage["is_server"]) or \
                   ("ca" in certificate_types and cert_usage["is_ca"]) or \
                   ("client" in certificate_types and cert_usage["is_client"]) or \
                   ("server" in certificate_types and cert_usage["is_server"]):
                    filtered_certs.append(cert)
            certificates_found = filtered_certs

        result["certificate_details"] = certificates_found

        # Generate expiration timeline
        timeline_dict = defaultdict(list)
        for cert in certificates_found:
            if cert["validity"]["days_remaining"] >= 0:  # Don't include expired certs in timeline
                expiry_date = cert["certificate_data"]["not_after"][:10]  # Just the date part
                timeline_dict[expiry_date].append({
                    "name": cert["certificate_info"]["name"],
                    "namespace": cert["certificate_info"]["namespace"],
                    "days_remaining": cert["validity"]["days_remaining"],
                    "status": cert["validity"]["status"]
                })

        # Sort timeline by date
        sorted_timeline = []
        for date in sorted(timeline_dict.keys()):
            sorted_timeline.append({
                "date": date,
                "certificates_expiring": timeline_dict[date]
            })

        result["expiration_timeline"] = sorted_timeline[:30]  # Limit to next 30 expiration dates

        # Generate renewal recommendations
        for cert in certificates_found:
            if cert["validity"]["status"] in ["critical", "warning", "expired"]:
                urgency = "immediate" if cert["validity"]["status"] in ["critical", "expired"] else "soon"

                recommendation = {
                    "certificate": cert["certificate_info"]["name"],
                    "namespace": cert["certificate_info"]["namespace"],
                    "urgency": urgency,
                    "renewal_method": "manual",
                    "steps": [
                        f"Generate new certificate for {cert['certificate_data'].get('subject_cn', 'unknown subject')}",
                        f"Update secret {cert['certificate_info']['secret_name']} in namespace {cert['certificate_info']['namespace']}",
                        "Restart affected pods/services"
                    ],
                    "automation_available": cert["certificate_info"]["namespace"].startswith("openshift-")
                }

                if cert["certificate_info"]["namespace"].startswith("openshift-"):
                    recommendation["renewal_method"] = "OpenShift Certificate Operator"
                    recommendation["steps"] = [
                        "Certificate should auto-renew via OpenShift Certificate Operator",
                        "If not auto-renewing, check cluster operator status",
                        "Manual intervention may be required"
                    ]

                result["renewal_recommendations"].append(recommendation)

        # Generate security findings
        for cert in certificates_found:
            cert_data = cert["certificate_data"]

            # Check for weak algorithms
            if "sha1" in cert_data.get("signature_algorithm", "").lower():
                result["security_findings"].append({
                    "certificate": cert["certificate_info"]["name"],
                    "finding_type": "weak_algorithm",
                    "description": f"Certificate uses weak SHA-1 signature algorithm",
                    "severity": "medium",
                    "recommendation": "Replace with SHA-256 or stronger algorithm"
                })

            # Check for self-signed certificates
            if cert["chain_validation"]["is_self_signed"] and not cert["usage"]["is_ca"]:
                result["security_findings"].append({
                    "certificate": cert["certificate_info"]["name"],
                    "finding_type": "self_signed",
                    "description": "Self-signed certificate detected",
                    "severity": "low",
                    "recommendation": "Consider using CA-signed certificate for production"
                })

            # Check for short validity periods
            if cert["validity"]["days_remaining"] < critical_threshold_days and cert["validity"]["status"] != "expired":
                result["security_findings"].append({
                    "certificate": cert["certificate_info"]["name"],
                    "finding_type": "short_validity",
                    "description": f"Certificate expires in {cert['validity']['days_remaining']} days",
                    "severity": "high",
                    "recommendation": "Renew certificate immediately"
                })

        # Add CA information
        result["certificate_authorities"] = list(ca_certificates.values())

        logger.info(f"Certificate health scan completed: {total_certs} certificates found, {critical_count + expired_count} require immediate attention")
        return result

    except Exception as e:
        logger.error(f"Error during certificate health check: {str(e)}", exc_info=True)
        return {
            "scan_summary": {
                "total_certificates": 0,
                "healthy_certificates": 0,
                "warning_certificates": 0,
                "critical_certificates": 0,
                "expired_certificates": 0,
                "scan_timestamp": datetime.utcnow().isoformat(),
                "error": str(e)
            },
            "certificate_details": [],
            "system_certificates": [],
            "expiration_timeline": [],
            "renewal_recommendations": [],
            "security_findings": [],
            "certificate_authorities": []
        }


@mcp.tool()
async def ci_cd_performance_baselining_tool(
    pipeline_names: Optional[List[str]] = None,
    baseline_period: str = "30d",
    deviation_threshold: float = 2.0,
    include_task_level: bool = True
) -> Dict[str, Any]:
    """
    Establish performance baselines for pipelines and flag runs deviating from historical norms.

    Uses Prometheus metrics from Tekton controller for accurate historical performance data.

    Args:
        pipeline_names: Pipelines to analyze (default: all).
        baseline_period: "7d", "30d" (default), or "90d".
        deviation_threshold: Std deviations to trigger alerts (default: 2.0).
        include_task_level: Include task-level analysis (default: True).

    Returns:
        Dict: Baselines, recent runs analysis, trends, and optimization opportunities.
    """
    logger.info(f"Starting CI/CD performance baselining analysis with period: {baseline_period} using Prometheus metrics")

    try:
        # Initialize result structure
        result = {
            "pipeline_baselines": [],
            "performance_trends": {
                "improving_pipelines": [],
                "degrading_pipelines": [],
                "stable_pipelines": [],
                "most_variable_pipelines": []
            },
            "optimization_opportunities": [],
            "data_source": "prometheus"
        }

        # Define all Prometheus queries upfront
        duration_count_query = "sum by (namespace, status) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count)"
        duration_sum_query = "sum by (namespace, status) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum)"
        avg_duration_query = "sum by (namespace) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum) / sum by (namespace) (tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count)"
        p16_query = "histogram_quantile(0.16, sum by (namespace, le) (rate(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_bucket[1h])))"
        p84_query = "histogram_quantile(0.84, sum by (namespace, le) (rate(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_bucket[1h])))"
        recent_avg_query = "sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[24h])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[24h]))"
        historical_avg_query = f"sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[{baseline_period}])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"
        recent_success_query = "sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count{status='success'}[24h])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[24h])) * 100"
        historical_success_query = f"sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count{{status='success'}}[{baseline_period}])) / sum by (namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}])) * 100"
        reconcile_query = "sum by (namespace_name, success) (rate(tekton_pipelines_controller_reconcile_count[1h]))"

        logger.info("Querying Prometheus for Tekton pipeline metrics (10 queries in parallel)...")

        # Execute ALL queries in parallel for maximum performance
        (
            count_result,
            sum_result,
            avg_result,
            p16_result,
            p84_result,
            recent_avg_result,
            historical_avg_result,
            recent_success_result,
            historical_success_result,
            reconcile_result
        ) = await asyncio.gather(
            _execute_prometheus_query_internal(duration_count_query),
            _execute_prometheus_query_internal(duration_sum_query),
            _execute_prometheus_query_internal(avg_duration_query),
            _execute_prometheus_query_internal(p16_query),
            _execute_prometheus_query_internal(p84_query),
            _execute_prometheus_query_internal(recent_avg_query),
            _execute_prometheus_query_internal(historical_avg_query),
            _execute_prometheus_query_internal(recent_success_query),
            _execute_prometheus_query_internal(historical_success_query),
            _execute_prometheus_query_internal(reconcile_query)
        )

        logger.info("All Prometheus queries completed")

        if not count_result.get("success") or not sum_result.get("success"):
            logger.warning("Prometheus queries failed, falling back to Kubernetes API")
            result["data_source"] = "kubernetes_api_fallback"
            result["prometheus_error"] = count_result.get("error") or sum_result.get("error")
            # Return early with empty results if Prometheus fails
            return result

        # Parse Prometheus results into namespace-level statistics
        namespace_stats = {}

        # Process count data
        for item in count_result.get("data", []):
            metric = item.get("metric", {})
            namespace = metric.get("namespace", "unknown")
            status = metric.get("status", "unknown")
            count = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

            if namespace not in namespace_stats:
                namespace_stats[namespace] = {
                    "success_count": 0,
                    "failed_count": 0,
                    "total_duration_sum": 0,
                    "total_count": 0
                }

            if status == "success":
                namespace_stats[namespace]["success_count"] = count
            elif status == "failed":
                namespace_stats[namespace]["failed_count"] = count
            namespace_stats[namespace]["total_count"] += count

        # Process duration sum data
        for item in sum_result.get("data", []):
            metric = item.get("metric", {})
            namespace = metric.get("namespace", "unknown")
            duration_sum = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

            if namespace in namespace_stats:
                namespace_stats[namespace]["total_duration_sum"] += duration_sum

        # Process average duration data
        if avg_result.get("success"):
            for item in avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                avg_duration = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                if namespace in namespace_stats and not np.isnan(avg_duration):
                    namespace_stats[namespace]["avg_duration"] = avg_duration

        # Store percentile data for std deviation calculation (std ≈ (P84 - P16) / 2)
        percentile_data = {}
        if p16_result.get("success"):
            for item in p16_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                p16_val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in percentile_data:
                    percentile_data[namespace] = {"p16": 0, "p84": 0}
                if not np.isnan(p16_val) and not np.isinf(p16_val):
                    percentile_data[namespace]["p16"] = p16_val

        if p84_result.get("success"):
            for item in p84_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                p84_val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in percentile_data:
                    percentile_data[namespace] = {"p16": 0, "p84": 0}
                if not np.isnan(p84_val) and not np.isinf(p84_val):
                    percentile_data[namespace]["p84"] = p84_val

        # Store trend data for each namespace (recent vs historical comparison)
        trend_data = {}

        # Process recent average duration
        if recent_avg_result.get("success"):
            for item in recent_avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["recent_avg"] = val

        # Process historical average duration
        if historical_avg_result.get("success"):
            for item in historical_avg_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["historical_avg"] = val

        # Process recent success rate
        if recent_success_result.get("success"):
            for item in recent_success_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["recent_success"] = val

        # Process historical success rate
        if historical_success_result.get("success"):
            for item in historical_success_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace", "unknown")
                val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0
                if namespace not in trend_data:
                    trend_data[namespace] = {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0}
                if not np.isnan(val) and not np.isinf(val):
                    trend_data[namespace]["historical_success"] = val

        reconcile_stats = {}
        if reconcile_result.get("success"):
            for item in reconcile_result.get("data", []):
                metric = item.get("metric", {})
                namespace = metric.get("namespace_name", "unknown")
                success = metric.get("success", "false")
                rate_val = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                if namespace not in reconcile_stats:
                    reconcile_stats[namespace] = {"success_rate": 0, "failure_rate": 0}

                if success == "true":
                    reconcile_stats[namespace]["success_rate"] = rate_val
                else:
                    reconcile_stats[namespace]["failure_rate"] = rate_val

        # Filter namespaces by pipeline_names if specified
        filtered_namespaces = namespace_stats.keys()
        if pipeline_names:
            filtered_namespaces = [ns for ns in filtered_namespaces if any(pn in ns for pn in pipeline_names)]

        # Build baseline entries for each namespace
        for namespace in filtered_namespaces:
            stats = namespace_stats[namespace]

            # Skip namespaces with no data
            if stats["total_count"] == 0:
                continue

            # Calculate metrics
            total_count = stats["total_count"]
            success_count = stats["success_count"]
            failed_count = stats["failed_count"]
            avg_duration = stats.get("avg_duration", 0)

            # Calculate success rate
            success_rate = (success_count / total_count * 100) if total_count > 0 else 0

            # Calculate std deviation from histogram percentiles (P84 - P16) / 2
            # This provides actual statistical std deviation instead of an estimate
            pdata = percentile_data.get(namespace, {"p16": 0, "p84": 0})
            if pdata["p84"] > pdata["p16"] and pdata["p84"] > 0:
                # Calculate actual std deviation from percentile spread
                estimated_std = (pdata["p84"] - pdata["p16"]) / 2.0
            else:
                # Fallback: use coefficient of variation heuristic if percentile data unavailable
                estimated_std = avg_duration * 0.4

            # Get reconciliation health
            recon = reconcile_stats.get(namespace, {"success_rate": 0, "failure_rate": 0})
            reconcile_health = "healthy"
            if recon["failure_rate"] > recon["success_rate"]:
                reconcile_health = "degraded"
            elif recon["failure_rate"] > 0.5:
                reconcile_health = "warning"

            # Calculate success rate confidence interval using binomial standard error
            # SE = sqrt(p * (1-p) / n) where p is success rate as decimal
            p = success_rate / 100.0
            if total_count > 0 and 0 < p < 1:
                success_rate_se = np.sqrt(p * (1 - p) / total_count) * 100  # Convert to percentage
            else:
                success_rate_se = 0  # No variance for 0% or 100% success rate

            # Create baseline entry
            baseline_metrics = {
                "duration": {
                    "mean_seconds": avg_duration,
                    "std_seconds": estimated_std,
                    "upper_bound": avg_duration + (deviation_threshold * estimated_std),
                    "lower_bound": max(0, avg_duration - (deviation_threshold * estimated_std))
                },
                "success_rate": {
                    "mean_percent": success_rate,
                    "std_percent": success_rate_se,
                    "lower_bound": max(0, success_rate - (deviation_threshold * success_rate_se)),
                    "upper_bound": min(100, success_rate + (deviation_threshold * success_rate_se))
                },
                "reconciliation": {
                    "success_rate_per_second": recon["success_rate"],
                    "failure_rate_per_second": recon["failure_rate"],
                    "health": reconcile_health
                }
            }

            # Determine trend using actual time-series comparison (recent vs historical)
            ns_trend = trend_data.get(namespace, {"recent_avg": 0, "historical_avg": 0, "recent_success": 0, "historical_success": 0})
            recent_avg = ns_trend["recent_avg"]
            historical_avg = ns_trend["historical_avg"]
            recent_success = ns_trend["recent_success"]
            historical_success = ns_trend["historical_success"]

            # Calculate duration change percentage (positive = slower = degradation)
            if historical_avg > 0 and recent_avg > 0:
                duration_change_pct = ((recent_avg - historical_avg) / historical_avg) * 100
            else:
                duration_change_pct = 0

            # Calculate success rate change (positive = improvement)
            success_change = recent_success - historical_success if (recent_success > 0 or historical_success > 0) else 0

            # Determine trend based on actual metrics comparison
            # Use deviation_threshold to determine significance (default 2.0 = ~5% significance)
            significance_threshold = 10.0 / deviation_threshold  # ~5% change with default threshold

            # Check if there is any recent activity before classifying trends
            has_recent_data = (recent_avg > 0 or recent_success > 0)

            if not has_recent_data:
                trend = "No recent activity (inactive in last 24h)"
                trend_direction = "inactive"
            elif abs(duration_change_pct) < significance_threshold and abs(success_change) < significance_threshold:
                trend = "Stable performance (no significant trend)"
                trend_direction = "stable"
            elif duration_change_pct < -significance_threshold or success_change > significance_threshold:
                trend = f"Performance improving: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "improving"
            elif duration_change_pct > significance_threshold or success_change < -significance_threshold:
                trend = f"Performance degrading: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "degrading"
            else:
                trend = f"Slight variation: duration {duration_change_pct:+.1f}%, success rate {success_change:+.1f}%"
                trend_direction = "variable"

            pipeline_baseline = {
                "pipeline_name": namespace,  # Using namespace as pipeline identifier for Prometheus data
                "namespace": namespace,
                "cluster": "current-cluster",
                "baseline_metrics": baseline_metrics,
                "data_points": int(total_count),
                "success_count": int(success_count),
                "failed_count": int(failed_count),
                "last_updated": datetime.now().isoformat(),
                "trend": trend,
                "trend_metrics": {
                    "recent_avg_duration": recent_avg,
                    "historical_avg_duration": historical_avg,
                    "duration_change_pct": duration_change_pct,
                    "recent_success_rate": recent_success,
                    "historical_success_rate": historical_success,
                    "success_rate_change": success_change,
                    "comparison_period": f"24h vs {baseline_period}"
                }
            }

            result["pipeline_baselines"].append(pipeline_baseline)

            # Categorize pipeline trends using trend_direction
            if trend_direction == "improving":
                result["performance_trends"]["improving_pipelines"].append({
                    "pipeline": namespace,
                    "trend": trend,
                    "avg_duration": avg_duration,
                    "success_rate": success_rate,
                    "duration_change_pct": duration_change_pct,
                    "success_rate_change": success_change
                })
            elif trend_direction == "degrading":
                result["performance_trends"]["degrading_pipelines"].append({
                    "pipeline": namespace,
                    "trend": trend,
                    "avg_duration": avg_duration,
                    "success_rate": success_rate,
                    "duration_change_pct": duration_change_pct,
                    "success_rate_change": success_change
                })
            elif trend_direction in ("stable", "inactive"):
                result["performance_trends"]["stable_pipelines"].append({
                    "pipeline": namespace,
                    "trend": trend,
                    "avg_duration": avg_duration,
                    "success_rate": success_rate
                })

            # Check for high variability (using reconciliation failure rate as proxy)
            if recon["failure_rate"] > 1.0:  # More than 1 failure per second
                result["performance_trends"]["most_variable_pipelines"].append({
                    "pipeline": namespace,
                    "failure_rate": recon["failure_rate"],
                    "avg_duration": avg_duration
                })

            # Generate optimization opportunities
            if avg_duration > 600:  # Pipelines taking more than 10 minutes
                result["optimization_opportunities"].append({
                    "pipeline": namespace,
                    "opportunity": "Long execution time optimization",
                    "potential_improvement": f"Pipeline averages {avg_duration/60:.1f} minutes - consider task parallelization or caching",
                    "complexity": "medium",
                    "avg_duration_seconds": avg_duration
                })

            if success_rate < 80:
                result["optimization_opportunities"].append({
                    "pipeline": namespace,
                    "opportunity": "Reliability improvement",
                    "potential_improvement": f"Success rate is {success_rate:.1f}% - investigate common failure patterns",
                    "complexity": "high",
                    "current_success_rate": success_rate
                })

            if reconcile_health == "degraded":
                result["optimization_opportunities"].append({
                    "pipeline": namespace,
                    "opportunity": "Reconciliation health improvement",
                    "potential_improvement": f"High reconciliation failure rate ({recon['failure_rate']:.2f}/s) - check controller logs and resource limits",
                    "complexity": "high",
                    "failure_rate": recon["failure_rate"]
                })

        # Task-level analysis if requested
        if include_task_level:
            logger.info("Performing task-level analysis...")
            result["task_level_analysis"] = {
                "task_baselines": [],
                "slowest_tasks": [],
                "most_failed_tasks": []
            }

            # Query task-level duration metrics by task name
            task_duration_query = f"sum by (task, namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_sum[{baseline_period}])) / sum by (task, namespace) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"
            task_count_query = f"sum by (task, namespace, status) (increase(tekton_pipelines_controller_pipelinerun_taskrun_duration_seconds_count[{baseline_period}]))"

            task_duration_result = await _execute_prometheus_query_internal(task_duration_query)
            task_count_result = await _execute_prometheus_query_internal(task_count_query)

            task_stats = {}

            # Process task duration data
            if task_duration_result.get("success"):
                for item in task_duration_result.get("data", []):
                    metric = item.get("metric", {})
                    task_name = metric.get("task", "unknown")
                    namespace = metric.get("namespace", "unknown")
                    avg_duration = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                    if np.isnan(avg_duration) or np.isinf(avg_duration):
                        continue

                    key = f"{namespace}/{task_name}"
                    if key not in task_stats:
                        task_stats[key] = {"task": task_name, "namespace": namespace, "avg_duration": 0, "success_count": 0, "failed_count": 0, "total_count": 0}
                    task_stats[key]["avg_duration"] = avg_duration

            # Process task count data
            if task_count_result.get("success"):
                for item in task_count_result.get("data", []):
                    metric = item.get("metric", {})
                    task_name = metric.get("task", "unknown")
                    namespace = metric.get("namespace", "unknown")
                    status = metric.get("status", "unknown")
                    count = float(item.get("value", [0, 0])[1]) if isinstance(item.get("value"), list) else 0

                    if np.isnan(count) or np.isinf(count):
                        continue

                    key = f"{namespace}/{task_name}"
                    if key not in task_stats:
                        task_stats[key] = {"task": task_name, "namespace": namespace, "avg_duration": 0, "success_count": 0, "failed_count": 0, "total_count": 0}

                    if status == "success":
                        task_stats[key]["success_count"] = count
                    elif status == "failed":
                        task_stats[key]["failed_count"] = count
                    task_stats[key]["total_count"] += count

            # Build task baselines and identify problem tasks
            # Filter out "unknown" tasks - these indicate missing 'task' label in Prometheus metrics
            unknown_task_count = 0
            for key, stats in task_stats.items():
                if stats["total_count"] < 1:
                    continue

                # Skip entries where task name is "unknown" - this means the Prometheus metric
                # doesn't have a 'task' label, so the data is aggregated at namespace level only
                if stats["task"] == "unknown":
                    unknown_task_count += 1
                    continue

                task_success_rate = (stats["success_count"] / stats["total_count"] * 100) if stats["total_count"] > 0 else 0

                task_baseline = {
                    "task": stats["task"],
                    "namespace": stats["namespace"],
                    "avg_duration_seconds": stats["avg_duration"],
                    "total_runs": int(stats["total_count"]),
                    "success_count": int(stats["success_count"]),
                    "failed_count": int(stats["failed_count"]),
                    "success_rate": task_success_rate
                }
                result["task_level_analysis"]["task_baselines"].append(task_baseline)

            # Add note if task-level data is limited
            if unknown_task_count > 0 and len(result["task_level_analysis"]["task_baselines"]) == 0:
                result["task_level_analysis"]["note"] = (
                    f"Task-level analysis unavailable: Prometheus metrics do not include 'task' labels. "
                    f"Found {unknown_task_count} namespace-level aggregations. "
                    "For task-level details, query TaskRun resources directly via Kubernetes API."
                )
                logger.info(f"Task-level analysis: No task labels in Prometheus metrics ({unknown_task_count} namespaces without task granularity)")

            # Sort and get top slowest tasks
            result["task_level_analysis"]["task_baselines"].sort(key=lambda x: x.get("avg_duration_seconds", 0) or 0, reverse=True)
            result["task_level_analysis"]["slowest_tasks"] = result["task_level_analysis"]["task_baselines"][:10]

            # Get most failed tasks (by failure count)
            failed_tasks = [t for t in result["task_level_analysis"]["task_baselines"] if t["failed_count"] > 0]
            failed_tasks.sort(key=lambda x: x["failed_count"], reverse=True)
            result["task_level_analysis"]["most_failed_tasks"] = failed_tasks[:10]

            logger.info(f"Task-level analysis completed: {len(result['task_level_analysis']['task_baselines'])} tasks analyzed (filtered {unknown_task_count} 'unknown' entries)")

        # Sort results for better presentation
        result["pipeline_baselines"].sort(key=lambda x: x.get("data_points", 0), reverse=True)
        result["performance_trends"]["improving_pipelines"].sort(key=lambda x: x.get("avg_duration", 0))
        result["performance_trends"]["degrading_pipelines"].sort(key=lambda x: x.get("avg_duration", 0), reverse=True)
        result["performance_trends"]["most_variable_pipelines"].sort(key=lambda x: x.get("failure_rate", 0), reverse=True)

        # Add summary statistics
        result["summary"] = {
            "total_namespaces_analyzed": len(result["pipeline_baselines"]),
            "total_taskruns_tracked": sum(b.get("data_points", 0) for b in result["pipeline_baselines"]),
            "total_successes": sum(b.get("success_count", 0) for b in result["pipeline_baselines"]),
            "total_failures": sum(b.get("failed_count", 0) for b in result["pipeline_baselines"]),
            "namespaces_needing_attention": len([b for b in result["pipeline_baselines"]
                                                  if b.get("baseline_metrics", {}).get("success_rate", {}).get("mean_percent", 100) < 80]),
            "optimization_opportunities_count": len(result["optimization_opportunities"])
        }

        logger.info(f"Performance baselining completed. Analyzed {len(result['pipeline_baselines'])} namespaces, "
                   f"tracking {result['summary']['total_taskruns_tracked']} total TaskRuns")

        return result

    except Exception as e:
        logger.error(f"Error in CI/CD performance baselining: {str(e)}", exc_info=True)
        return {
            "pipeline_baselines": [],
            "performance_trends": {
                "improving_pipelines": [],
                "degrading_pipelines": [],
                "stable_pipelines": [],
                "most_variable_pipelines": []
            },
            "optimization_opportunities": [],
            "error": str(e)
        }


@mcp.tool()
async def pipeline_tracer(
    trace_identifier: str,
    trace_type: str,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    include_artifacts: bool = True,
    trace_depth: str = "deep",
    namespaces: Optional[List[str]] = None,
    max_namespaces: int = 50
) -> Dict[str, Any]:
    """
    Trace a logical operation (commit, PR, image) as it flows through pipelines.

    Correlates pipeline runs using labels, annotations, and artifact references.

    Args:
        trace_identifier: Commit SHA, PR number, image tag, or custom trace ID.
        trace_type: "commit", "pr", "image", or "custom".
        start_time: ISO 8601 start timestamp.
        end_time: ISO 8601 end timestamp.
        include_artifacts: Include artifact details (default: True).
        trace_depth: "shallow" or "deep" (default: "deep").
        namespaces: Specific namespaces to search (skips auto-detection).
        max_namespaces: Maximum namespaces to search when auto-detecting (default: 50).

    Returns:
        Dict: Pipeline flow, artifacts, bottlenecks, and summary.
    """
    try:
        logger.info(f"Starting pipeline trace for {trace_type}: {trace_identifier}")

        # Validate inputs
        valid_trace_types = ["commit", "pr", "image", "custom"]
        if trace_type not in valid_trace_types:
            return {
                "error": f"Invalid trace_type '{trace_type}'. Must be one of: {', '.join(valid_trace_types)}"
            }

        valid_depths = ["shallow", "deep"]
        if trace_depth not in valid_depths:
            return {
                "error": f"Invalid trace_depth '{trace_depth}'. Must be one of: {', '.join(valid_depths)}"
            }

        # Get multi-cluster clients
        cluster_clients = await get_multi_cluster_clients(k8s_core_api, k8s_custom_api, k8s_apps_api)

        if not cluster_clients:
            return {
                "error": "No cluster clients available for tracing"
            }

        # Detect tekton-active namespaces for prioritization (if not user-specified)
        tekton_ns_list = None
        if not namespaces:
            try:
                tekton_ns = await detect_tekton_namespaces()
                tekton_ns_list = []
                for category in tekton_ns.values():
                    tekton_ns_list.extend(category)
                tekton_ns_list = list(set(tekton_ns_list))
                logger.info(f"Detected {len(tekton_ns_list)} tekton-active namespaces for prioritization")
            except Exception as e:
                logger.debug(f"Failed to detect tekton namespaces: {e}")

        # Correlate pipeline events across clusters (parallelized)
        pipeline_flow = await correlate_pipeline_events(
            trace_identifier=trace_identifier,
            trace_type=trace_type,
            cluster_clients=cluster_clients,
            start_time=start_time,
            end_time=end_time,
            namespaces=namespaces,
            max_namespaces=max_namespaces,
            tekton_namespaces=tekton_ns_list,
            logger=logger
        )

        # Track artifacts if requested
        artifacts = await track_artifacts(pipeline_flow, include_artifacts, logger)

        # Analyze for bottlenecks
        bottlenecks = analyze_bottlenecks(pipeline_flow, logger)

        # Calculate summary metrics
        summary = {
            "total_duration": 0,
            "clusters_traversed": len(set(p["cluster"] for p in pipeline_flow)),
            "pipelines_executed": len(pipeline_flow)
        }

        # Calculate total duration if we have start and end times
        if pipeline_flow:
            first_start = pipeline_flow[0].get("start_time")
            last_completion = None

            for pipeline in reversed(pipeline_flow):
                if pipeline.get("completion_time"):
                    last_completion = pipeline["completion_time"]
                    break

            if first_start and last_completion:
                try:
                    start_dt = datetime.fromisoformat(first_start.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(last_completion.replace('Z', '+00:00'))
                    summary["total_duration"] = (end_dt - start_dt).total_seconds()
                except Exception as e:
                    logger.debug(f"Failed to calculate total duration: {e}")

        # Follow lifecycle chain (snapshots → tests → releases → release pipelines)
        lifecycle = {}
        if pipeline_flow:
            try:
                lifecycle = await follow_lifecycle_chain(
                    pipeline_flow=pipeline_flow,
                    custom_api=k8s_custom_api,
                    core_api=k8s_core_api,
                    trace_depth=trace_depth,
                    logger=logger
                )
                logger.info(
                    f"Lifecycle chain: {len(lifecycle.get('snapshots', []))} snapshots, "
                    f"{len(lifecycle.get('integration_tests', []))} tests, "
                    f"{len(lifecycle.get('releases', []))} releases, "
                    f"{len(lifecycle.get('release_pipelines', []))} release PLRs, "
                    f"{len(lifecycle.get('nudge_cascade', []))} nudge cascades"
                )
            except Exception as e:
                logger.warning(f"Failed to follow lifecycle chain: {e}")
                lifecycle = {"error": str(e)[:200]}

        # Determine overall status (include lifecycle in assessment)
        if not pipeline_flow:
            overall_status = "not_found"
        else:
            # Check build PLRs
            builds_ok = all(p["status"] in ["Succeeded", "Completed"] for p in pipeline_flow)
            builds_failed = any(p["status"] in ["Failed", "Error"] for p in pipeline_flow)

            # Check release status from lifecycle
            releases = lifecycle.get("releases", [])
            release_failed = any(r.get("status") == "Failed" for r in releases)
            release_succeeded = all(r.get("status") == "Succeeded" for r in releases) if releases else True

            if builds_failed:
                overall_status = "failed"
            elif release_failed:
                overall_status = "release_failed"
            elif builds_ok and release_succeeded:
                overall_status = "succeeded"
            else:
                overall_status = "in_progress"

        # Build stage-level summary
        stage_summary = {}
        if pipeline_flow:
            build_durations = [p.get("completion_time") for p in pipeline_flow if p.get("completion_time")]
            stage_summary["build"] = {
                "count": len(pipeline_flow),
                "status": "succeeded" if all(p["status"] in ["Succeeded", "Completed"] for p in pipeline_flow) else "failed",
            }
        if lifecycle.get("integration_tests"):
            tests = lifecycle["integration_tests"]
            stage_summary["integration_tests"] = {
                "count": len(tests),
                "passed": sum(1 for t in tests if t.get("status") == "TestPassed"),
                "failed": sum(1 for t in tests if t.get("status") == "TestFail"),
            }
        if lifecycle.get("releases"):
            rels = lifecycle["releases"]
            stage_summary["releases"] = {
                "count": len(rels),
                "succeeded": sum(1 for r in rels if r.get("status") == "Succeeded"),
                "failed": sum(1 for r in rels if r.get("status") == "Failed"),
            }
        if lifecycle.get("nudge_cascade"):
            stage_summary["nudge_cascade"] = {
                "count": len(lifecycle["nudge_cascade"]),
            }
        summary["stages"] = stage_summary

        result = {
            "trace_id": f"{trace_type}:{trace_identifier}",
            "trace_type": trace_type,
            "start_time": start_time or (pipeline_flow[0].get("start_time") if pipeline_flow else None),
            "end_time": end_time or (pipeline_flow[-1].get("completion_time") if pipeline_flow else None),
            "overall_status": overall_status,
            "pipeline_flow": pipeline_flow,
            "lifecycle": lifecycle,
            "artifacts": artifacts,
            "bottlenecks": bottlenecks,
            "summary": summary
        }

        logger.info(f"Trace completed: found {len(pipeline_flow)} pipelines across {summary['clusters_traversed']} clusters")

        return result

    except Exception as e:
        logger.error(f"Error in pipeline_tracer: {str(e)}", exc_info=True)
        return {
            "error": f"Failed to trace pipeline: {str(e)}",
            "trace_id": f"{trace_type}:{trace_identifier}",
            "trace_type": trace_type,
            "overall_status": "error",
            "pipeline_flow": [],
            "artifacts": [],
            "bottlenecks": [],
            "summary": {"total_duration": 0, "clusters_traversed": 0, "pipelines_executed": 0}
        }


@mcp.tool()
async def get_machine_config_pool_status(
    pool_names: Optional[List[str]] = None,
    include_node_details: bool = True,
    include_update_history: bool = True,
    filter_updating: bool = False
) -> Dict[str, Any]:
    """
    Monitor OpenShift Machine Config Pools for node configuration and update rollouts.

    Analyzes pool status, update progress, and configuration drift.

    Args:
        pool_names: Pools to monitor (default: all).
        include_node_details: Include node status per pool (default: True).
        include_update_history: Include update history (default: True).
        filter_updating: Only show updating pools (default: False).

    Returns:
        Dict: Keys: pools_overview, machine_config_pools, recent_config_changes, issues,
              update_recommendations.
    """
    logger.info("Starting machine config pool status analysis")

    try:
        # Query MachineConfigPool resources using Kubernetes Custom Resource API
        logger.info("Querying MachineConfigPool resources from OpenShift Machine Config Operator")

        pools_response = k8s_custom_api.list_cluster_custom_object(
            group="machineconfiguration.openshift.io",
            version="v1",
            plural="machineconfigpools"
        )

        all_pools = pools_response.get("items", [])
        logger.info(f"Found {len(all_pools)} machine config pools in cluster")

        # Filter pools if specific names requested
        if pool_names:
            filtered_pools = []
            for pool in all_pools:
                pool_name = pool.get("metadata", {}).get("name", "")
                if pool_name in pool_names:
                    filtered_pools.append(pool)
            pools_to_analyze = filtered_pools
            logger.info(f"Filtered to {len(pools_to_analyze)} requested pools: {pool_names}")
        else:
            pools_to_analyze = all_pools

        # Analyze each pool
        analyzed_pools = []
        for pool in pools_to_analyze:
            pool_analysis = analyze_machine_config_pool_status(pool)
            analyzed_pools.append(pool_analysis)

        # Filter for updating pools if requested
        if filter_updating:
            analyzed_pools = [pool for pool in analyzed_pools if pool.get("update_progress", {}).get("is_updating", False)]
            logger.info(f"Filtered to {len(analyzed_pools)} pools currently updating")

        # Generate pools overview
        total_pools = len(analyzed_pools)
        healthy_pools = len([pool for pool in analyzed_pools if pool.get("status") == "ready"])
        updating_pools = len([pool for pool in analyzed_pools if pool.get("update_progress", {}).get("is_updating", False)])
        degraded_pools = len([pool for pool in analyzed_pools if pool.get("status") == "degraded"])

        pools_overview = {
            "total_pools": total_pools,
            "healthy_pools": healthy_pools,
            "updating_pools": updating_pools,
            "degraded_pools": degraded_pools
        }

        # Get recent machine config changes if requested
        recent_config_changes = []
        if include_update_history:
            try:
                logger.info("Querying recent MachineConfig changes")
                machine_configs_response = k8s_custom_api.list_cluster_custom_object(
                    group="machineconfiguration.openshift.io",
                    version="v1",
                    plural="machineconfigs"
                )

                machine_configs = machine_configs_response.get("items", [])

                # Sort by creation time and get recent ones
                sorted_configs = sorted(
                    machine_configs,
                    key=lambda x: x.get("metadata", {}).get("creationTimestamp", ""),
                    reverse=True
                )[:10]  # Get last 10 configs

                for config in sorted_configs:
                    metadata = config.get("metadata", {})
                    recent_config_changes.append({
                        "config_name": metadata.get("name", "unknown"),
                        "created_time": metadata.get("creationTimestamp", "unknown"),
                        "changes": ["Configuration details would require detailed diff analysis"],
                        "affected_pools": metadata.get("labels", {}).get("machineconfiguration.openshift.io/role", "unknown")
                    })

            except Exception as e:
                logger.warning(f"Could not retrieve machine config history: {e}")
                recent_config_changes = []

        # Detect issues across all pools
        all_issues = []
        for pool in analyzed_pools:
            pool_issues = detect_pool_issues(pool)
            all_issues.extend(pool_issues)

        # Generate recommendations
        update_recommendations = generate_update_recommendations(analyzed_pools)

        # Add node details if requested and include_node_details is True
        if include_node_details:
            logger.info("Adding detailed node status to pool analysis")
            for pool in analyzed_pools:
                try:
                    # Query nodes that belong to this pool based on node selector
                    pool_config = pool.get("configuration", {})
                    node_selector = pool_config.get("node_selector", {})

                    # Get all nodes and filter by labels
                    nodes = k8s_core_api.list_node()
                    matching_nodes = []

                    for node in nodes.items:
                        node_labels = node.metadata.labels or {}
                        # Check if node matches the pool's node selector
                        matches = True
                        for key, value in node_selector.items():
                            if node_labels.get(key) != value:
                                matches = False
                                break

                        if matches:
                            node_status = {
                                "name": node.metadata.name,
                                "ready": False,
                                "machine_config": "unknown",
                                "last_update": "unknown"
                            }

                            # Check node readiness
                            for condition in node.status.conditions or []:
                                if condition.type == "Ready":
                                    node_status["ready"] = condition.status == "True"
                                    break

                            # Extract machine config info from annotations
                            annotations = node.metadata.annotations or {}
                            node_status["machine_config"] = annotations.get(
                                "machineconfiguration.openshift.io/currentConfig", "unknown"
                            )
                            node_status["last_update"] = annotations.get(
                                "machineconfiguration.openshift.io/lastAppliedDrift", "unknown"
                            )

                            matching_nodes.append(node_status)

                    pool["node_status"] = matching_nodes

                except Exception as e:
                    logger.warning(f"Could not retrieve node details for pool {pool.get('name')}: {e}")
                    pool["node_status"] = []

        result = {
            "pools_overview": pools_overview,
            "machine_config_pools": analyzed_pools,
            "recent_config_changes": recent_config_changes,
            "issues": all_issues,
            "update_recommendations": update_recommendations
        }

        logger.info(f"Machine config pool analysis complete: {total_pools} pools analyzed, "
                   f"{len(all_issues)} issues found, {len(update_recommendations)} recommendations generated")

        return result

    except ApiException as e:
        error_msg = f"Kubernetes API error while querying machine config pools: {e.status} - {e.reason}"
        logger.error(error_msg)
        return {
            "pools_overview": {"total_pools": 0, "healthy_pools": 0, "updating_pools": 0, "degraded_pools": 0},
            "machine_config_pools": [],
            "recent_config_changes": [],
            "issues": [{
                "pool": "api_error",
                "issue_type": "api_access",
                "description": error_msg,
                "affected_nodes": [],
                "severity": "high",
                "remediation": "Check RBAC permissions for machineconfiguration.openshift.io resources"
            }],
            "update_recommendations": []
        }

    except Exception as e:
        error_msg = f"Unexpected error during machine config pool analysis: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "pools_overview": {"total_pools": 0, "healthy_pools": 0, "updating_pools": 0, "degraded_pools": 0},
            "machine_config_pools": [],
            "recent_config_changes": [],
            "issues": [{
                "pool": "system_error",
                "issue_type": "analysis_failure",
                "description": error_msg,
                "affected_nodes": [],
                "severity": "high",
                "remediation": "Check system logs and OpenShift Machine Config Operator status"
            }],
            "update_recommendations": []
        }


async def _get_fallback_cluster_health() -> Dict[str, Any]:
    """
    Fallback cluster health analysis using standard Kubernetes resources.
    Used when OpenShift-specific cluster operators are not accessible.

    Note: This returns component health checks (namespaces, nodes) NOT actual
    ClusterOperator resources. The output is structured similarly for compatibility
    but clearly marked as fallback_mode=True.
    """
    logger.info("Performing fallback cluster health analysis using standard Kubernetes resources")

    cluster_info = {}
    component_health = []  # Not operators - these are namespace/node health checks
    critical_issues = []

    try:
        # Get basic cluster information
        try:
            from kubernetes.client import VersionApi
            api_server_url = k8s_core_api.api_client.configuration.host

            # Use the proper VersionApi to get cluster version
            try:
                version_api = VersionApi(k8s_core_api.api_client)
                version_info = version_api.get_code()
                cluster_info = {
                    "cluster_version": version_info.git_version or "unknown",
                    "platform": version_info.platform or "unknown",
                    "api_server": api_server_url,
                    "build_date": version_info.build_date or "unknown",
                    "go_version": version_info.go_version or "unknown"
                }
            except Exception as version_error:
                logger.warning(f"Could not get version via VersionApi: {version_error}")
                cluster_info = {
                    "cluster_version": "unknown",
                    "platform": "unknown",
                    "api_server": api_server_url,
                    "build_date": "unknown"
                }
        except Exception as e:
            logger.warning(f"Could not retrieve basic cluster info: {e}")
            cluster_info = {"error": "Could not access cluster information"}

        # Analyze core system components using standard Kubernetes resources
        try:
            # Check system namespaces and their health
            system_namespaces = ['kube-system', 'kube-public', 'default']

            for ns_name in system_namespaces:
                try:
                    # Get pods in system namespace
                    pods = k8s_core_api.list_namespaced_pod(namespace=ns_name)

                    total_pods = len(pods.items)
                    running_pods = 0
                    failed_pods = 0

                    for pod in pods.items:
                        if pod.status.phase == 'Running':
                            running_pods += 1
                        elif pod.status.phase in ['Failed', 'CrashLoopBackOff']:
                            failed_pods += 1

                    if total_pods == 0:
                        # No pods visible — likely managed cluster with restricted access
                        health_ratio = 1.0
                        status = "available"
                    else:
                        health_ratio = running_pods / total_pods
                        status = "available"
                        if health_ratio < 0.8:
                            status = "degraded"
                        elif health_ratio < 0.9:
                            status = "warning"

                    component_health.append({
                        "name": f"system-namespace:{ns_name}",
                        "type": "namespace_health",  # Clearly indicates this is NOT an operator
                        "namespace": ns_name,
                        "status": status,
                        "available": health_ratio >= 0.8,
                        "degraded": health_ratio < 0.8,
                        "progressing": False,
                        "version": "n/a",
                        "conditions_analysis": {
                            "total_pods": total_pods,
                            "running_pods": running_pods,
                            "failed_pods": failed_pods,
                            "health_ratio": round(health_ratio, 2)
                        }
                    })

                    if failed_pods > 0:
                        critical_issues.append({
                            "component": f"system-namespace:{ns_name}",
                            "severity": "warning" if failed_pods < 3 else "critical",
                            "issue": f"{failed_pods} failed pods in {ns_name} namespace",
                            "impact": f"Potential service disruption in {ns_name}",
                            "recommended_action": f"Check pod logs in {ns_name} namespace"
                        })

                except Exception as e:
                    logger.warning(f"Could not analyze namespace {ns_name}: {e}")
                    component_health.append({
                        "name": f"system-namespace:{ns_name}",
                        "type": "namespace_health",
                        "namespace": ns_name,
                        "status": "unknown",
                        "available": False,
                        "degraded": False,
                        "progressing": False,
                        "version": "n/a",
                        "conditions_analysis": {"error": str(e)}
                    })

        except Exception as e:
            logger.warning(f"Could not analyze system namespaces: {e}")
            critical_issues.append({
                "component": "namespace-analysis",
                "severity": "warning",
                "issue": f"Could not analyze system namespaces: {str(e)}",
                "impact": "Limited visibility into system component health",
                "recommended_action": "Check RBAC permissions for pod listing"
            })

        # Check node health
        try:
            nodes = k8s_core_api.list_node()
            total_nodes = len(nodes.items)
            ready_nodes = 0

            for node in nodes.items:
                if node.status.conditions:
                    ready_condition = next((c for c in node.status.conditions if c.type == 'Ready'), None)
                    if ready_condition and ready_condition.status == 'True':
                        ready_nodes += 1

            node_health_ratio = ready_nodes / total_nodes if total_nodes > 0 else 0
            node_status = "available" if node_health_ratio >= 0.8 else "degraded"

            component_health.append({
                "name": "cluster-nodes",
                "type": "node_health",  # Clearly indicates this is NOT an operator
                "namespace": "cluster-scoped",
                "status": node_status,
                "available": node_health_ratio >= 0.8,
                "degraded": node_health_ratio < 0.8,
                "progressing": False,
                "version": "n/a",
                "conditions_analysis": {
                    "total_nodes": total_nodes,
                    "ready_nodes": ready_nodes,
                    "health_ratio": round(node_health_ratio, 2)
                }
            })

            if ready_nodes < total_nodes:
                critical_issues.append({
                    "component": "cluster-nodes",
                    "severity": "critical" if node_health_ratio < 0.5 else "warning",
                    "issue": f"{total_nodes - ready_nodes} of {total_nodes} nodes not ready",
                    "impact": "Reduced cluster capacity and potential service disruption",
                    "recommended_action": "Check node status and system resources"
                })

        except Exception as e:
            logger.warning(f"Could not analyze node health: {e}")
            critical_issues.append({
                "component": "cluster-nodes",
                "severity": "warning",
                "issue": f"Could not analyze node health: {str(e)}",
                "impact": "No visibility into node status",
                "recommended_action": "Check RBAC permissions for node listing"
            })

        # Calculate health summary (for components, not operators)
        total_components = len(component_health)
        healthy_components = len([c for c in component_health if c.get("status") == "available"])
        degraded_components = len([c for c in component_health if c.get("degraded", False)])

        overall_health = "healthy"
        if degraded_components > 0:
            overall_health = "degraded"
        elif healthy_components < total_components:
            overall_health = "warning"

        health_summary = {
            "fallback_mode": True,  # Clearly indicates this is NOT OpenShift operator data
            "total_components": total_components,
            "healthy_components": healthy_components,
            "degraded_components": degraded_components,
            # Keep operator fields for backwards compatibility but set to 0
            "total_operators": 0,
            "healthy_operators": 0,
            "degraded_operators": 0,
            "overall_health": overall_health,
            "note": "ClusterOperator access denied. Showing system component health instead."
        }

        return {
            "fallback_mode": True,
            "cluster_info": cluster_info,
            "operator_status": [],  # Empty - we don't have operator access
            "component_health": component_health,  # New field with actual health data
            "health_summary": health_summary,
            "critical_issues": critical_issues,
            "dependencies": None
        }

    except Exception as e:
        logger.error(f"Fallback cluster health analysis failed: {e}")
        return {
            "fallback_mode": True,
            "cluster_info": {"error": "Fallback analysis failed"},
            "operator_status": [],
            "component_health": [],
            "health_summary": {
                "fallback_mode": True,
                "total_components": 0,
                "healthy_components": 0,
                "degraded_components": 0,
                "total_operators": 0,
                "healthy_operators": 0,
                "degraded_operators": 0,
                "overall_health": "unknown",
                "note": "Fallback analysis failed"
            },
            "critical_issues": [{
                "component": "fallback-analysis",
                "severity": "critical",
                "issue": f"Fallback cluster health analysis failed: {str(e)}",
                "impact": "No cluster health information available",
                "recommended_action": "Check cluster connectivity and basic RBAC permissions"
            }],
            "dependencies": None
        }


@mcp.tool()
async def get_openshift_cluster_operator_status(
    operator_names: Optional[List[str]] = None,
    include_conditions: bool = True,
    show_version_info: bool = True,
    filter_degraded: bool = False,
    include_dependencies: bool = False
) -> Dict[str, Any]:
    """
    Check health and status of OpenShift cluster operators for platform functionality.

    Analyzes operator conditions, versions, and dependencies.

    Args:
        operator_names: Operators to check (default: all).
        include_conditions: Include condition details (default: True).
        show_version_info: Include version info (default: True).
        filter_degraded: Only show operators with issues (default: False).
        include_dependencies: Show operator dependencies (default: False).

    Returns:
        Dict: Keys: cluster_info, operator_status, health_summary, critical_issues, dependencies.
    """
    logger.info("Starting OpenShift cluster operator status analysis")

    try:
        # Query ClusterOperator resources from OpenShift Config API
        logger.info("Querying ClusterOperator resources from OpenShift Config API")

        operators_response = k8s_custom_api.list_cluster_custom_object(
            group="config.openshift.io",
            version="v1",
            plural="clusteroperators"
        )

        all_operators = operators_response.get("items", [])
        logger.info(f"Found {len(all_operators)} cluster operators")

        # Filter operators if specific names requested
        if operator_names:
            filtered_operators = []
            for operator in all_operators:
                op_name = operator.get("metadata", {}).get("name", "")
                if op_name in operator_names:
                    filtered_operators.append(operator)
            operators_to_analyze = filtered_operators
            logger.info(f"Filtered to {len(operators_to_analyze)} requested operators: {operator_names}")
        else:
            operators_to_analyze = all_operators

        # Get cluster version information
        cluster_info = {}
        try:
            cluster_version_response = k8s_custom_api.list_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusterversions"
            )
            cluster_versions = cluster_version_response.get("items", [])
            if cluster_versions:
                cv = cluster_versions[0]  # There's typically only one
                cv_status = cv.get("status", {})
                cluster_info = {
                    "cluster_version": cv_status.get("desired", {}).get("version", "unknown"),
                    "cluster_id": cv.get("spec", {}).get("clusterID", "unknown"),
                    "infrastructure_status": cv_status.get("infrastructure", {}).get("status", "unknown"),
                    "update_available": len(cv_status.get("availableUpdates", [])) > 0,
                    "current_update": cv_status.get("history", [{}])[0] if cv_status.get("history") else {}
                }
        except Exception as e:
            logger.warning(f"Could not retrieve cluster version info: {e}")
            cluster_info = {
                "cluster_version": "unknown",
                "cluster_id": "unknown",
                "infrastructure_status": "unknown",
                "update_available": False,
                "current_update": {}
            }

        # Analyze each operator
        analyzed_operators = []
        for operator in operators_to_analyze:
            metadata = operator.get("metadata", {})
            status = operator.get("status", {})

            operator_analysis = {
                "name": metadata.get("name", "unknown"),
                "namespace": metadata.get("namespace", "cluster-scoped"),
                "status": "unknown",
                "available": False,
                "progressing": False,
                "degraded": False
            }

            # Analyze conditions - always parse for health assessment, only include raw in output if requested
            conditions = status.get("conditions", [])
            conditions_analysis = analyze_operator_conditions(conditions)
            operator_analysis["available"] = conditions_analysis["available"]
            operator_analysis["progressing"] = conditions_analysis["progressing"]
            operator_analysis["degraded"] = conditions_analysis["degraded"]
            if include_conditions:
                operator_analysis["conditions_analysis"] = conditions_analysis
                operator_analysis["conditions"] = conditions

            # Calculate overall status
            if operator_analysis["degraded"]:
                operator_analysis["status"] = "degraded"
            elif not operator_analysis["available"]:
                operator_analysis["status"] = "unavailable"
            elif operator_analysis["progressing"]:
                operator_analysis["status"] = "progressing"
            else:
                operator_analysis["status"] = "available"

            # Add version information
            if show_version_info:
                versions = status.get("versions", [])
                if versions:
                    # Find operator version (usually the first one or one named 'operator')
                    operator_version = "unknown"
                    for version in versions:
                        if version.get("name") == "operator" or len(versions) == 1:
                            operator_version = version.get("version", "unknown")
                            break
                    operator_analysis["version"] = operator_version
                else:
                    operator_analysis["version"] = "unknown"

            # Add related objects info
            operator_analysis["related_objects"] = status.get("relatedObjects", [])

            analyzed_operators.append(operator_analysis)

        # Calculate health summary from ALL operators before filtering
        total_operators = len(analyzed_operators)
        healthy_operators = len([op for op in analyzed_operators if op.get("status") == "available"])
        degraded_operators = len([op for op in analyzed_operators if op.get("degraded", False)])

        # Filter degraded operators if requested (after counting)
        if filter_degraded:
            analyzed_operators = [op for op in analyzed_operators if op.get("degraded", False) or op.get("status") != "available"]
            logger.info(f"Filtered to {len(analyzed_operators)} operators with issues")

        overall_health = "healthy"
        if degraded_operators > 0:
            overall_health = "degraded"
        elif healthy_operators < total_operators:
            overall_health = "warning"

        health_summary = {
            "total_operators": total_operators,
            "healthy_operators": healthy_operators,
            "degraded_operators": degraded_operators,
            "overall_health": overall_health
        }

        # Identify critical issues
        critical_issues = identify_critical_issues(analyzed_operators)

        # Build response
        response = {
            "cluster_info": cluster_info,
            "operator_status": analyzed_operators,
            "health_summary": health_summary,
            "critical_issues": critical_issues
        }

        # Add dependencies if requested
        if include_dependencies:
            dependencies = analyze_operator_dependencies(analyzed_operators)
            response["dependencies"] = dependencies

        logger.info(f"Cluster operator analysis complete. Health: {overall_health}, Issues: {len(critical_issues)}")
        return response

    except ApiException as e:
        error_msg = f"API error accessing cluster operators: {e.status} - {e.reason}"
        logger.error(error_msg)

        if e.status == 403:
            error_msg += ". Check RBAC permissions for config.openshift.io resources"
            logger.info("Attempting fallback analysis using standard Kubernetes resources...")

            # Fallback: Use standard Kubernetes resources to provide alternative health info
            try:
                fallback_result = await _get_fallback_cluster_health()
                fallback_result["critical_issues"].insert(0, {
                    "component": "openshift-api-access",
                    "severity": "warning",
                    "issue": "Limited permissions for OpenShift cluster operators. Using fallback analysis.",
                    "impact": "Reduced visibility into OpenShift-specific operator status",
                    "recommended_action": "Grant access to config.openshift.io resources for full OpenShift monitoring"
                })
                return fallback_result
            except Exception as fallback_error:
                logger.error(f"Fallback analysis also failed: {fallback_error}")

        elif e.status == 404:
            error_msg += ". ClusterOperator resource not found - may not be an OpenShift cluster"
            logger.info("Attempting fallback analysis for non-OpenShift cluster...")

            # Fallback for non-OpenShift clusters
            try:
                fallback_result = await _get_fallback_cluster_health()
                fallback_result["critical_issues"].insert(0, {
                    "component": "cluster-type-detection",
                    "severity": "info",
                    "issue": "Not an OpenShift cluster - using standard Kubernetes health analysis",
                    "impact": "OpenShift-specific operator monitoring not available",
                    "recommended_action": "Use standard Kubernetes monitoring tools for this cluster type"
                })
                return fallback_result
            except Exception as fallback_error:
                logger.error(f"Fallback analysis failed: {fallback_error}")

        return {
            "cluster_info": {},
            "operator_status": [],
            "health_summary": {"total_operators": 0, "healthy_operators": 0, "degraded_operators": 0, "overall_health": "unknown"},
            "critical_issues": [{"component": "api-access", "severity": "critical", "issue": error_msg, "impact": "Cannot assess cluster operator status", "recommended_action": "Check cluster access and RBAC permissions"}],
            "dependencies": [] if include_dependencies else None
        }

    except Exception as e:
        error_msg = f"Unexpected error analyzing cluster operators: {str(e)}"
        logger.error(error_msg, exc_info=True)

        return {
            "cluster_info": {},
            "operator_status": [],
            "health_summary": {"total_operators": 0, "healthy_operators": 0, "degraded_operators": 0, "overall_health": "unknown"},
            "critical_issues": [{"component": "system-error", "severity": "critical", "issue": error_msg, "impact": "Cannot assess cluster operator status", "recommended_action": "Check system logs and cluster connectivity"}],
            "dependencies": [] if include_dependencies else None
        }


async def _process_namespace_topology(
    namespace: str,
    cluster_name: str,
    component_types: List[str],
    core_api,
    apps_api,
    custom_api,
    include_metrics: bool,
    skip_on_permission_denied: bool,
    logger
) -> Dict[str, Any]:
    """Process a single namespace and return its topology data."""
    nodes = []
    edges = []
    permissions = {"accessible": [], "denied": [], "errors": []}
    stats = {"nodes": 0, "edges": 0}

    # Pre-fetch pods once to avoid N+1 queries in analyze_service_dependencies
    pods_list = None
    if "pods" in component_types or "services" in component_types:
        try:
            pods_result = await asyncio.to_thread(core_api.list_namespaced_pod, namespace=namespace)
            pods_list = pods_result.items
        except Exception as e:
            logger.debug(f"Could not pre-fetch pods for {namespace}: {e}")

    try:
        # Process Deployments
        if "deployments" in component_types:
            try:
                deployments = await asyncio.to_thread(apps_api.list_namespaced_deployment, namespace=namespace)
                permissions["accessible"].append(f"{cluster_name}/{namespace}/deployments")

                for deployment in deployments.items:
                    node_id = generate_node_id(cluster_name, namespace, "deployment", deployment.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "deployment",
                        "name": deployment.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": deployment.status.conditions[-1].type if deployment.status.conditions else "Unknown",
                        "metadata": {
                            "replicas": deployment.spec.replicas or 0,
                            "ready_replicas": deployment.status.ready_replicas or 0,
                            "labels": deployment.metadata.labels or {}
                        }
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(cluster_name, "deployment", namespace, deployment.metadata.name, logger)

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze dependencies
                    deployment_dict = deployment.to_dict()
                    owner_edges = await analyze_owner_references(deployment_dict, cluster_name, "deployment")
                    volume_edges = await analyze_volume_dependencies(deployment_dict, cluster_name, "deployment", logger)
                    edges.extend(owner_edges + volume_edges)
                    stats["edges"] += len(owner_edges + volume_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "deployments", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/deployments")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/deployments",
                        "error": error_info["error_message"]
                    })

        # Process ReplicaSets (needed for complete Deployment→ReplicaSet→Pod ownership chain)
        if "replicasets" in component_types:
            try:
                replicasets = await asyncio.to_thread(apps_api.list_namespaced_replica_set, namespace=namespace)
                permissions["accessible"].append(f"{cluster_name}/{namespace}/replicasets")

                for replicaset in replicasets.items:
                    node_id = generate_node_id(cluster_name, namespace, "replicaset", replicaset.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "replicaset",
                        "name": replicaset.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active" if (replicaset.status.ready_replicas or 0) > 0 else "Inactive",
                        "metadata": {
                            "replicas": replicaset.spec.replicas or 0,
                            "ready_replicas": replicaset.status.ready_replicas or 0,
                            "labels": replicaset.metadata.labels or {}
                        }
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(cluster_name, "replicaset", namespace, replicaset.metadata.name, logger)

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze dependencies (ReplicaSet→Deployment ownership)
                    replicaset_dict = replicaset.to_dict()
                    owner_edges = await analyze_owner_references(replicaset_dict, cluster_name, "replicaset")
                    edges.extend(owner_edges)
                    stats["edges"] += len(owner_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "replicasets", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/replicasets")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/replicasets",
                        "error": error_info["error_message"]
                    })

        # Process Services
        if "services" in component_types:
            try:
                services = await asyncio.to_thread(core_api.list_namespaced_service, namespace=namespace)
                permissions["accessible"].append(f"{cluster_name}/{namespace}/services")

                for service in services.items:
                    node_id = generate_node_id(cluster_name, namespace, "service", service.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "service",
                        "name": service.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "type": service.spec.type,
                            "cluster_ip": service.spec.cluster_ip,
                            "ports": [{"port": p.port, "target_port": p.target_port} for p in (service.spec.ports or [])],
                            "selector": service.spec.selector or {}
                        }
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(cluster_name, "service", namespace, service.metadata.name, logger)

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze service dependencies (pass pre-fetched pods to avoid N+1 queries)
                    service_dict = service.to_dict()
                    service_edges = await analyze_service_dependencies(service_dict, cluster_name, core_api, logger, pods_list=pods_list)
                    edges.extend(service_edges)
                    stats["edges"] += len(service_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "services", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/services")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/services",
                        "error": error_info["error_message"]
                    })

        # Process Pods (use pre-fetched pods_list if available)
        if "pods" in component_types:
            try:
                # Use pre-fetched pods if available, otherwise fetch
                if pods_list is not None:
                    pods_items = pods_list
                else:
                    pods_result = await asyncio.to_thread(core_api.list_namespaced_pod, namespace=namespace)
                    pods_items = pods_result.items
                for pod in pods_items:
                    node_id = generate_node_id(cluster_name, namespace, "pod", pod.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "pod",
                        "name": pod.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pod.status.phase or "Unknown",
                        "metadata": {
                            "node_name": pod.spec.node_name,
                            "labels": pod.metadata.labels or {},
                            "containers": len(pod.spec.containers or [])
                        }
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(cluster_name, "pod", namespace, pod.metadata.name, logger)

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Analyze pod dependencies
                    pod_dict = pod.to_dict()
                    owner_edges = await analyze_owner_references(pod_dict, cluster_name, "pod")
                    volume_edges = await analyze_volume_dependencies(pod_dict, cluster_name, "pod", logger)
                    edges.extend(owner_edges + volume_edges)
                    stats["edges"] += len(owner_edges + volume_edges)

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "pods", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/pods")
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/pods",
                        "error": error_info["error_message"]
                    })

        # Process PVCs
        if "persistentvolumeclaims" in component_types:
            try:
                pvcs = await asyncio.to_thread(core_api.list_namespaced_persistent_volume_claim, namespace=namespace)
                for pvc in pvcs.items:
                    node_id = generate_node_id(cluster_name, namespace, "persistentvolumeclaim", pvc.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "persistentvolumeclaim",
                        "name": pvc.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pvc.status.phase or "Unknown",
                        "metadata": {
                            "capacity": pvc.status.capacity.get("storage") if pvc.status.capacity else None,
                            "access_modes": pvc.spec.access_modes or [],
                            "storage_class": pvc.spec.storage_class_name
                        }
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(cluster_name, "persistentvolumeclaim", namespace, pvc.metadata.name, logger)

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "persistentvolumeclaims", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/persistentvolumeclaims")
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/persistentvolumeclaims",
                        "error": error_info["error_message"]
                    })

        # Process ConfigMaps
        if "configmaps" in component_types:
            try:
                configmaps = await asyncio.to_thread(core_api.list_namespaced_config_map, namespace=namespace)
                permissions["accessible"].append(f"{cluster_name}/{namespace}/configmaps")

                for cm in configmaps.items:
                    node_id = generate_node_id(cluster_name, namespace, "configmap", cm.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "configmap",
                        "name": cm.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "data_keys": list(cm.data.keys()) if cm.data else []
                        }
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "configmaps", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/configmaps")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/configmaps",
                        "error": error_info["error_message"]
                    })

        # Process Secrets (NOT included in defaults due to common RBAC restrictions)
        if "secrets" in component_types:
            try:
                secrets = await asyncio.to_thread(core_api.list_namespaced_secret, namespace=namespace)
                permissions["accessible"].append(f"{cluster_name}/{namespace}/secrets")

                for secret in secrets.items:
                    node_id = generate_node_id(cluster_name, namespace, "secret", secret.metadata.name)

                    node = {
                        "id": node_id,
                        "type": "secret",
                        "name": secret.metadata.name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "type": secret.type,
                            "data_keys": list(secret.data.keys()) if secret.data else []
                        }
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "secrets", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/secrets")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/secrets",
                        "error": error_info["error_message"]
                    })

        # Process Tekton PipelineRuns
        if "pipelineruns" in component_types:
            try:
                pipeline_runs = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="pipelineruns",
                    limit=200
                )

                for pr in pipeline_runs.get("items", []):
                    node_id = generate_node_id(cluster_name, namespace, "pipelinerun", pr.get("metadata", {}).get("name", ""))

                    node = {
                        "id": node_id,
                        "type": "pipelinerun",
                        "name": pr.get("metadata", {}).get("name", ""),
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": pr.get("status", {}).get("conditions", [{}])[-1].get("type", "Unknown"),
                        "metadata": {
                            "pipeline_ref": pr.get("spec", {}).get("pipelineRef", {}).get("name", ""),
                            "labels": pr.get("metadata", {}).get("labels", {})
                        }
                    }

                    if include_metrics:
                        node["metrics"] = await get_resource_metrics(cluster_name, "pipelinerun", namespace, node["name"], logger)

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Create edge to pipeline if referenced
                    pipeline_ref = pr.get("spec", {}).get("pipelineRef", {}).get("name")
                    if pipeline_ref:
                        pipeline_id = generate_node_id(cluster_name, namespace, "pipeline", pipeline_ref)
                        edges.append({
                            "source": node_id,
                            "target": pipeline_id,
                            "relationship": "runs",
                            "weight": calculate_dependency_weight("pipelinerun", "pipeline", "runs")
                        })
                        stats["edges"] += 1

            except Exception as e:
                logger.debug(f"Could not fetch PipelineRuns in {namespace}: {e}")

        # Process Tekton Pipelines
        if "pipelines" in component_types:
            try:
                pipelines = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="pipelines"
                )

                for pipeline in pipelines.get("items", []):
                    node_id = generate_node_id(cluster_name, namespace, "pipeline", pipeline.get("metadata", {}).get("name", ""))

                    node = {
                        "id": node_id,
                        "type": "pipeline",
                        "name": pipeline.get("metadata", {}).get("name", ""),
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "tasks": len(pipeline.get("spec", {}).get("tasks", [])),
                            "labels": pipeline.get("metadata", {}).get("labels", {})
                        }
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                logger.debug(f"Could not fetch Pipelines in {namespace}: {e}")

        # Process Tekton TaskRuns
        if "taskruns" in component_types:
            try:
                task_runs = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="taskruns",
                    limit=500
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/taskruns")

                for tr in task_runs.get("items", []):
                    tr_name = tr.get("metadata", {}).get("name", "")
                    node_id = generate_node_id(cluster_name, namespace, "taskrun", tr_name)

                    # Get status from conditions
                    conditions = tr.get("status", {}).get("conditions", [])
                    status = conditions[-1].get("reason", "Unknown") if conditions else "Unknown"

                    node = {
                        "id": node_id,
                        "type": "taskrun",
                        "name": tr_name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": status,
                        "metadata": {
                            "task_ref": tr.get("spec", {}).get("taskRef", {}).get("name", ""),
                            "pipeline_run": tr.get("metadata", {}).get("labels", {}).get("tekton.dev/pipelineRun", ""),
                            "labels": tr.get("metadata", {}).get("labels", {}),
                            "start_time": tr.get("status", {}).get("startTime")
                        }
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

                    # Create edge to PipelineRun if part of one
                    pipeline_run_name = tr.get("metadata", {}).get("labels", {}).get("tekton.dev/pipelineRun")
                    if pipeline_run_name:
                        pr_id = generate_node_id(cluster_name, namespace, "pipelinerun", pipeline_run_name)
                        edges.append({
                            "source": pr_id,
                            "target": node_id,
                            "relationship": "runs_task",
                            "weight": 0.85
                        })
                        stats["edges"] += 1

                    # Create edge to Task if referenced
                    task_ref = tr.get("spec", {}).get("taskRef", {}).get("name")
                    if task_ref:
                        task_id = generate_node_id(cluster_name, namespace, "task", task_ref)
                        edges.append({
                            "source": node_id,
                            "target": task_id,
                            "relationship": "uses",
                            "weight": calculate_dependency_weight("taskrun", "task", "uses")
                        })
                        stats["edges"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "taskruns", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/taskruns")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/taskruns",
                        "error": error_info["error_message"]
                    })

        # Process Tekton Tasks
        if "tasks" in component_types:
            try:
                tasks = await asyncio.to_thread(
                    custom_api.list_namespaced_custom_object,
                    group="tekton.dev",
                    version="v1",
                    namespace=namespace,
                    plural="tasks"
                )
                permissions["accessible"].append(f"{cluster_name}/{namespace}/tasks")

                for task in tasks.get("items", []):
                    task_name = task.get("metadata", {}).get("name", "")
                    node_id = generate_node_id(cluster_name, namespace, "task", task_name)

                    node = {
                        "id": node_id,
                        "type": "task",
                        "name": task_name,
                        "namespace": namespace,
                        "cluster": cluster_name,
                        "status": "Active",
                        "metadata": {
                            "steps": len(task.get("spec", {}).get("steps", [])),
                            "labels": task.get("metadata", {}).get("labels", {})
                        }
                    }

                    nodes.append(node)
                    stats["nodes"] += 1

            except Exception as e:
                error_info = handle_resource_fetch_error(e, "tasks", namespace, skip_on_permission_denied, logger)
                if error_info["permission_denied"]:
                    permissions["denied"].append(f"{cluster_name}/{namespace}/tasks")
                    if not skip_on_permission_denied:
                        raise
                else:
                    permissions["errors"].append({
                        "resource": f"{cluster_name}/{namespace}/tasks",
                        "error": error_info["error_message"]
                    })

    except Exception as e:
        logger.warning(f"Error processing namespace {namespace} in cluster {cluster_name}: {e}")

    return {
        "nodes": nodes,
        "edges": edges,
        "permissions": permissions,
        "stats": stats
    }


@mcp.tool()
async def live_system_topology_mapper(
    cluster_names: Optional[List[str]] = None,
    component_types: Optional[List[str]] = None,
    namespace_filter: Optional[str] = None,
    depth_limit: Optional[int] = 5,
    include_metrics: Optional[bool] = False,
    output_format: Optional[str] = "json",
    skip_on_permission_denied: Optional[bool] = True
) -> Dict[str, Any]:
    """
    Generate real-time dependency graph of Kubernetes/Tekton components and their interconnections.

    Maps Services, Deployments, Pipelines, PVCs, and their relationships via ownerReferences and selectors.

    Args:
        cluster_names: Clusters to map (default: all).
        component_types: Filter by types (services, deployments, pipelines, pvcs, etc.). Note: secrets are NOT included by default.
        namespace_filter: Regex pattern to filter namespaces.
        depth_limit: Max dependency depth (default: 5).
        include_metrics: Include resource metrics (default: False).
        output_format: "json" (default), "graphviz", or "mermaid".
        skip_on_permission_denied: Continue mapping other resources if permission denied (default: True).

    Returns:
        Dict: Topology graph with nodes, edges, summary, metadata, and permission report.
    """
    try:
        logger.info(f"Starting live system topology mapping with filters: clusters={cluster_names}, "
                   f"types={component_types}, namespace_filter={namespace_filter}")

        start_time = time.time()

        # Get multi-cluster clients
        cluster_clients = await get_multi_cluster_topology_clients(k8s_core_api, k8s_custom_api, k8s_apps_api, k8s_storage_api, k8s_batch_api)

        if not cluster_clients:
            return {
                "topology": {"nodes": [], "edges": []},
                "summary": {"total_nodes": 0, "total_relationships": 0, "clusters_mapped": 0, "potential_blast_radius": {}},
                "error": "No cluster clients available for topology mapping",
                "last_updated": datetime.now().isoformat()
            }

        # Filter clusters if specified
        if cluster_names:
            cluster_clients = {k: v for k, v in cluster_clients.items() if k in cluster_names}

        # Default component types if not specified
        # Note: secrets are NOT included by default due to common RBAC restrictions
        # ReplicaSets are included to show complete Deployment→ReplicaSet→Pod ownership chain
        if not component_types:
            component_types = ["deployments", "replicasets", "services", "pods", "persistentvolumeclaims",
                             "configmaps", "pipelineruns", "pipelines", "taskruns", "tasks"]

        nodes = []
        edges = []
        cluster_stats = {}

        # Track permission issues
        permissions_report = {
            "accessible": [],
            "denied": [],
            "errors": []
        }

        for cluster_name, clients in cluster_clients.items():
            logger.info(f"Mapping topology for cluster: {cluster_name}")
            cluster_stats[cluster_name] = {"nodes": 0, "edges": 0}

            try:
                core_api = clients["core_api"]
                apps_api = clients["apps_api"]
                custom_api = clients["custom_api"]
                storage_api = clients["storage_api"]

                # Get all namespaces
                all_namespaces = []
                try:
                    ns_list = await asyncio.to_thread(core_api.list_namespace)
                    all_namespaces = [ns.metadata.name for ns in ns_list.items]

                    # Apply namespace filter if specified
                    if namespace_filter:
                        try:
                            pattern = _safe_compile_namespace_filter(namespace_filter)
                            all_namespaces = [ns for ns in all_namespaces if pattern.search(ns)]
                        except (re.error, ValueError) as e:
                            logger.warning(f"Invalid namespace filter regex '{namespace_filter}': {e}")

                except Exception as e:
                    logger.warning(f"Failed to list namespaces in cluster {cluster_name}: {e}")
                    continue

                logger.info(f"Processing {len(all_namespaces)} namespaces in cluster {cluster_name} in parallel")

                # Process all namespaces in parallel using asyncio.gather
                namespace_tasks = [
                    _process_namespace_topology(
                        namespace=ns,
                        cluster_name=cluster_name,
                        component_types=component_types,
                        core_api=core_api,
                        apps_api=apps_api,
                        custom_api=custom_api,
                        include_metrics=include_metrics,
                        skip_on_permission_denied=skip_on_permission_denied,
                        logger=logger
                    )
                    for ns in all_namespaces
                ]

                namespace_results = await asyncio.gather(*namespace_tasks, return_exceptions=True)

                # Aggregate results from all namespaces
                for i, result in enumerate(namespace_results):
                    if isinstance(result, Exception):
                        logger.warning(f"Error processing namespace {all_namespaces[i]} in cluster {cluster_name}: {result}")
                        continue

                    nodes.extend(result["nodes"])
                    edges.extend(result["edges"])
                    cluster_stats[cluster_name]["nodes"] += result["stats"]["nodes"]
                    cluster_stats[cluster_name]["edges"] += result["stats"]["edges"]
                    permissions_report["accessible"].extend(result["permissions"]["accessible"])
                    permissions_report["denied"].extend(result["permissions"]["denied"])
                    permissions_report["errors"].extend(result["permissions"]["errors"])


            except Exception as e:
                logger.error(f"Error processing cluster {cluster_name}: {e}")
                continue

        # Calculate summary statistics
        total_nodes = len(nodes)
        total_edges = len(edges)
        clusters_mapped = len([c for c in cluster_stats.values() if c["nodes"] > 0])

        # Calculate potential blast radius using depth_limit
        blast_radius = {}
        if total_nodes > 0:
            # Create NetworkX graph for analysis
            G = nx.DiGraph()
            for node in nodes:
                G.add_node(node["id"], **node)
            for edge in edges:
                G.add_edge(edge["source"], edge["target"], **edge)

            # Calculate metrics using depth_limit for traversal analysis
            if G.nodes():
                # Find nodes reachable within depth_limit from each node
                max_reachable = 0
                critical_nodes_list = []

                for node_id in G.nodes():
                    # Use BFS with depth limit to find reachable nodes
                    reachable = set()
                    queue = [(node_id, 0)]
                    visited = {node_id}

                    while queue:
                        current, current_depth = queue.pop(0)
                        if current_depth >= depth_limit:
                            continue
                        for neighbor in G.neighbors(current):
                            if neighbor not in visited:
                                visited.add(neighbor)
                                reachable.add(neighbor)
                                queue.append((neighbor, current_depth + 1))

                    if len(reachable) > max_reachable:
                        max_reachable = len(reachable)

                    # Mark as critical if can affect many nodes within depth_limit
                    if len(reachable) > 5:
                        critical_nodes_list.append({
                            "node_id": node_id,
                            "affected_count": len(reachable)
                        })

                blast_radius = {
                    "depth_limit_used": depth_limit,
                    "most_connected_components": len(list(nx.connected_components(G.to_undirected()))),
                    "average_degree": sum(dict(G.degree()).values()) / len(G.nodes()) if G.nodes() else 0,
                    "critical_nodes": len(critical_nodes_list),
                    "max_blast_radius": max_reachable,
                    "critical_nodes_details": critical_nodes_list[:10]  # Top 10 critical nodes
                }

        execution_time = time.time() - start_time

        # Deduplicate permission report entries
        permissions_report["accessible"] = list(set(permissions_report["accessible"]))
        permissions_report["denied"] = list(set(permissions_report["denied"]))

        result = {
            "topology": {
                "nodes": nodes,
                "edges": edges
            },
            "summary": {
                "total_nodes": total_nodes,
                "total_relationships": total_edges,
                "clusters_mapped": clusters_mapped,
                "potential_blast_radius": blast_radius,
                "cluster_stats": cluster_stats,
                "execution_time_seconds": round(execution_time, 2)
            },
            "permissions": permissions_report,
            "last_updated": datetime.now().isoformat()
        }

        # Log permissions summary
        if permissions_report["denied"]:
            logger.warning(f"Permission denied for {len(permissions_report['denied'])} resource types")
        if permissions_report["errors"]:
            logger.warning(f"Errors encountered for {len(permissions_report['errors'])} resource types")

        logger.info(f"Topology mapping completed: {total_nodes} nodes, {total_edges} edges across {clusters_mapped} clusters in {execution_time:.2f}s")

        # Handle different output formats
        if output_format == "graphviz":
            result["graphviz"] = convert_to_graphviz(nodes, edges)
        elif output_format == "mermaid":
            result["mermaid"] = convert_to_mermaid(nodes, edges)

        return result

    except Exception as e:
        logger.error(f"Unexpected error during topology mapping: {str(e)}", exc_info=True)
        return {
            "topology": {"nodes": [], "edges": []},
            "summary": {"total_nodes": 0, "total_relationships": 0, "clusters_mapped": 0, "potential_blast_radius": {}},
            "error": f"Failed to generate topology: {str(e)}",
            "last_updated": datetime.now().isoformat()
        }


@mcp.tool()
@log_tool_execution
async def predictive_log_analyzer(
    prediction_window: str = "6h",
    confidence_threshold: float = 0.75,
    log_sources: Optional[List[str]] = None,
    namespaces: Optional[List[str]] = None,
    max_namespaces: int = 20,
    force_retrain: bool = False
) -> Dict[str, Any]:
    """
    Predict failures using ML analysis of historical log patterns before critical outages occur.

    Uses anomaly detection algorithms to correlate log patterns with failure events.
    Supports persistent model storage for faster subsequent calls.

    Args:
        prediction_window: Time window - "1h", "6h", "24h", "7d" (default: "6h").
        confidence_threshold: Min confidence for predictions 0.0-1.0 (default: 0.75).
        log_sources: Sources to analyze - pods, services, nodes (default: all).
        namespaces: Specific namespaces to analyze (default: auto-detect active namespaces).
        max_namespaces: Maximum namespaces to scan when auto-detecting (default: 20).
        force_retrain: Force model retraining even if cached model is valid (default: False).

    Returns:
        Dict: Keys: predictions, model_performance, anomaly_scores, trend_analysis, model_info.
    """
    try:
        logger.info(f"Starting predictive log analysis with window: {prediction_window}, threshold: {confidence_threshold}")

        # Validate parameters
        valid_windows = ["1h", "6h", "24h", "7d"]
        if prediction_window not in valid_windows:
            raise ValueError(f"Invalid prediction_window. Must be one of: {valid_windows}")

        if not 0.0 <= confidence_threshold <= 1.0:
            raise ValueError("confidence_threshold must be between 0.0 and 1.0")

        # Initialize persistence components (lazy loading)
        try:
            from helpers.ml_persistence import (
                ModelPersistenceManager,
                TrainingDataStore,
                FailureEventCollector,
                ModelVersionManager,
                build_labels_from_correlations
            )
            model_manager = ModelPersistenceManager()
            training_store = TrainingDataStore()
            failure_collector = FailureEventCollector(training_store)
            version_manager = ModelVersionManager(model_manager, training_store)
            persistence_available = True
        except Exception as e:
            logger.warning(f"ML persistence not available, using ephemeral training: {e}")
            persistence_available = False
            model_manager = None
            training_store = None
            failure_collector = None
            version_manager = None

        # Initialize result structure
        result = {
            "predictions": [],
            "model_performance": {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "last_training_time": datetime.now().isoformat()
            },
            "anomaly_scores": [],
            "trend_analysis": {
                "error_rate_trend": "stable",
                "resource_trend": "stable",
                "performance_trend": "stable"
            },
            "model_info": {
                "model_id": None,
                "loaded_from_cache": False,
                "training_samples": 0,
                "has_failure_labels": False,
                "persistence_enabled": persistence_available
            }
        }

        # Get recent logs from various sources
        log_sources = log_sources or ["pods", "services", "nodes"]
        all_logs = []

        for source in log_sources:
            try:
                if source == "pods":
                    # Determine target namespaces
                    if namespaces:
                        # Use user-provided namespaces
                        target_namespaces = namespaces
                        logger.info(f"Using user-specified namespaces: {target_namespaces}")
                    else:
                        # Auto-detect active namespaces, prioritizing those with tekton/pipeline activity
                        all_ns = await list_namespaces()
                        try:
                            tekton_ns = await detect_tekton_namespaces()
                            active_ns = []
                            for category in tekton_ns.values():
                                active_ns.extend(category)
                            # Deduplicate and limit
                            target_namespaces = list(set(active_ns))[:max_namespaces] if active_ns else all_ns[:max_namespaces]
                        except Exception:
                            # Fallback to alphabetical if tekton detection fails
                            target_namespaces = all_ns[:max_namespaces]
                        logger.info(f"Auto-detected {len(target_namespaces)} active namespaces")

                    for ns in target_namespaces:
                        try:
                            pods = k8s_core_api.list_namespaced_pod(namespace=ns, limit=50)
                            for pod in pods.items:
                                # Include Running pods for proactive analysis, plus Failed/Succeeded for historical
                                if pod.status.phase in ["Running", "Failed", "Succeeded"]:
                                    try:
                                        pod_logs = k8s_core_api.read_namespaced_pod_log(
                                            name=pod.metadata.name,
                                            namespace=ns,
                                            tail_lines=100
                                        )
                                        all_logs.extend(pod_logs.split('\n'))
                                    except ApiException:
                                        continue  # Skip pods without accessible logs
                        except ApiException:
                            continue  # Skip inaccessible namespaces

            except Exception as e:
                logger.warning(f"Failed to collect logs from {source}: {str(e)}")
                continue

        # Return early if no logs collected - never fabricate analysis from fake data
        if not all_logs:
            logger.warning("No logs collected from any source - returning insufficient data")
            result["trend_analysis"]["error_rate_trend"] = "no_data"
            result["model_performance"] = {
                "accuracy": None,
                "precision": None,
                "recall": None,
                "note": "No log data available for analysis"
            }
            return result

        # Filter out empty lines
        all_logs = [log for log in all_logs if log.strip()]

        if len(all_logs) < 10:
            logger.warning(f"Insufficient log data for analysis: {len(all_logs)} lines")
            result["trend_analysis"]["error_rate_trend"] = "insufficient_data"
            return result

        logger.info(f"Analyzing {len(all_logs)} log lines for predictive patterns")

        # Preprocess log data
        log_df = preprocess_log_data(all_logs)

        # Extract features for ML analysis
        features = extract_log_features(log_df)

        # Collect failure events and correlate with logs if persistence is available
        labels = None
        if persistence_available and failure_collector and training_store:
            try:
                # Collect failure events from the target namespaces
                for ns in target_namespaces:
                    try:
                        # Collect from Kubernetes events - use dict format for FailureEventCollector
                        events_as_dicts = await _get_namespace_events_as_dicts(ns, limit=100)
                        if events_as_dicts:
                            count = failure_collector.collect_from_events(events_as_dicts, ns)
                            logger.debug(f"Collected {count} failure labels from events in {ns}")

                        # Collect from pod statuses
                        pods = k8s_core_api.list_namespaced_pod(namespace=ns, limit=50)
                        failure_collector.collect_from_pod_status(pods.items, ns)
                    except Exception as e:
                        logger.debug(f"Failed to collect failure events from {ns}: {e}")

                # Get recent failure labels for correlation
                from datetime import timedelta
                historical_failures = training_store.get_failure_labels_in_window(
                    start_time=datetime.now() - timedelta(hours=2),
                    end_time=datetime.now()
                )

                # Store log samples first so they get database IDs for correlation
                stored_samples = []
                for idx, row in log_df.iterrows():
                    if idx < 500:  # Limit to avoid excessive storage
                        sample_data = {
                            "timestamp": row.get("timestamp"),
                            "namespace": target_namespaces[0] if target_namespaces else "unknown",
                            "features": features[idx].tolist() if idx < len(features) else [],
                            "raw_message": str(row.get("raw_message", ""))[:500],
                            "log_level": row.get("log_level"),
                            "error_indicators": int(row.get("error_indicators", 0)),
                            "message_entropy": float(row.get("message_entropy", 0.0))
                        }
                        sample_id = training_store.store_log_sample(sample_data)
                        if sample_id:
                            stored_samples.append({
                                "id": sample_id,
                                "timestamp": sample_data["timestamp"],
                                "namespace": sample_data["namespace"]
                            })

                # Correlate stored log samples with failures using database IDs
                if historical_failures and stored_samples:
                    correlations = failure_collector.correlate_logs_with_failures(
                        stored_samples, historical_failures, time_window_minutes=30
                    )
                    if correlations:
                        labels = build_labels_from_correlations(correlations, len(log_df))
                        logger.info(f"Created {len(correlations)} log-failure correlations")

            except Exception as e:
                logger.warning(f"Failed to collect/correlate failure events: {e}")

        # Train or load model with persistence
        if persistence_available and model_manager and version_manager:
            try:
                anomaly_model, model_id, model_metadata = train_or_load_model(
                    features=features,
                    model_manager=model_manager,
                    version_manager=version_manager,
                    labels=labels,
                    force_retrain=force_retrain
                )

                # Update result with model info
                result["model_info"].update({
                    "model_id": model_id,
                    "loaded_from_cache": model_metadata.get("loaded_from_cache", False),
                    "training_samples": model_metadata.get("training_samples", len(features)),
                    "has_failure_labels": labels is not None and len(labels) > 0,
                    "created_at": model_metadata.get("created_at")
                })

                # Use performance metrics from model if available
                perf = model_metadata.get("performance_metrics", {})
                if perf:
                    result["model_performance"].update({
                        "accuracy": perf.get("accuracy", 0.0),
                        "precision": perf.get("precision", 0.0),
                        "recall": perf.get("recall", 0.0),
                        "last_training_time": model_metadata.get("created_at", datetime.now().isoformat())
                    })
            except Exception as e:
                logger.warning(f"Persistence-based training failed, falling back to ephemeral: {e}")
                anomaly_model = train_anomaly_model(features)
        else:
            # Fallback to ephemeral training
            anomaly_model = train_anomaly_model(features)

        anomaly_scores = anomaly_model.decision_function(features)
        anomaly_predictions = anomaly_model.predict(features)

        # Update model performance if not already set by persistence
        # Note: without labeled validation data, precision/recall cannot be computed
        if result["model_performance"]["accuracy"] == 0.0:
            normal_predictions = anomaly_predictions == 1
            accuracy = np.mean(normal_predictions) if len(normal_predictions) > 0 else 0.0
            result["model_performance"].update({
                "accuracy": float(accuracy),
                "precision": None,
                "recall": None,
                "note": "Precision/recall require labeled validation data - not available"
            })

        # Generate aggregate anomaly scores per namespace
        # anomaly_scores are per-log-line, so aggregate by namespace using mean score
        if target_namespaces:
            # Calculate per-namespace aggregated scores from per-line scores
            lines_per_ns = max(1, len(anomaly_scores) // max(1, len(target_namespaces)))
            threshold = -0.5  # Typical anomaly threshold for Isolation Forest

            for i, ns in enumerate(target_namespaces[:min(10, len(target_namespaces))]):
                start_idx = i * lines_per_ns
                end_idx = min(start_idx + lines_per_ns, len(anomaly_scores))
                if start_idx < len(anomaly_scores):
                    ns_scores = anomaly_scores[start_idx:end_idx]
                    mean_score = float(np.mean(ns_scores))
                    anomalous_lines = int(np.sum(ns_scores < threshold))
                    status = "anomalous" if mean_score < threshold else "normal"

                    result["anomaly_scores"].append({
                        "component": ns,
                        "score": mean_score,
                        "threshold": threshold,
                        "status": status,
                        "anomalous_log_lines": anomalous_lines,
                        "total_log_lines": len(ns_scores)
                    })

        # Analyze patterns for failure prediction - pass historical failures for correlation
        historical_failures_for_analysis = []
        if persistence_available and training_store:
            try:
                from datetime import timedelta
                historical_failures_for_analysis = training_store.get_failure_labels_in_window(
                    start_time=datetime.now() - timedelta(hours=24),
                    end_time=datetime.now()
                )
            except Exception as e:
                logger.debug(f"Could not retrieve historical failures: {e}")

        pattern_analysis = analyze_log_patterns_for_failure_prediction(
            log_df, historical_failures_for_analysis
        )

        # Generate predictions using both pattern analysis and labeled data
        predictions = generate_failure_predictions(
            pattern_analysis,
            confidence_threshold,
            prediction_window,
            historical_failures=historical_failures_for_analysis,
            labels=labels
        )
        result["predictions"] = predictions

        # Analyze trends
        error_logs = log_df[log_df['log_level'].isin(['ERROR', 'FATAL', 'PANIC'])]
        error_rate = len(error_logs) / len(log_df) if len(log_df) > 0 else 0.0

        if error_rate > 0.15:
            result["trend_analysis"]["error_rate_trend"] = "increasing"
        elif error_rate < 0.05:
            result["trend_analysis"]["error_rate_trend"] = "decreasing"
        else:
            result["trend_analysis"]["error_rate_trend"] = "stable"

        # Resource trend based on log patterns
        resource_indicators = log_df['raw_message'].str.contains(
            r'memory|cpu|disk|storage|resource', case=False, na=False
        ).sum()

        if resource_indicators > len(log_df) * 0.1:
            result["trend_analysis"]["resource_trend"] = "concerning"
        else:
            result["trend_analysis"]["resource_trend"] = "stable"

        # Performance trend based on response times and timeouts
        performance_indicators = log_df['raw_message'].str.contains(
            r'timeout|slow|latency|performance|delay', case=False, na=False
        ).sum()

        if performance_indicators > len(log_df) * 0.08:
            result["trend_analysis"]["performance_trend"] = "degrading"
        else:
            result["trend_analysis"]["performance_trend"] = "stable"

        # Update has_failure_labels to correctly reflect historical failures used
        result["model_info"]["has_failure_labels"] = (
            (labels is not None and len(labels) > 0) or
            (historical_failures_for_analysis and len(historical_failures_for_analysis) > 0)
        )

        logger.info(f"Predictive analysis complete: {len(predictions)} predictions generated")
        return result

    except Exception as e:
        logger.error(f"Error in predictive log analysis: {str(e)}", exc_info=True)
        return {
            "predictions": [],
            "model_performance": {
                "accuracy": 0.0,
                "precision": 0.0,
                "recall": 0.0,
                "last_training_time": datetime.now().isoformat()
            },
            "anomaly_scores": [],
            "trend_analysis": {
                "error_rate_trend": "error",
                "resource_trend": "error",
                "performance_trend": "error"
            },
            "model_info": {
                "model_id": None,
                "loaded_from_cache": False,
                "training_samples": 0,
                "has_failure_labels": False,
                "persistence_enabled": False
            },
            "error": str(e)
        }


@mcp.tool()
@log_tool_execution
async def manage_prediction_training_data(
    action: str = "stats",
    failure_type: Optional[str] = None,
    namespace: Optional[str] = None,
    resource_name: Optional[str] = None,
    severity: Optional[str] = None,
    collect_from_namespaces: Optional[List[str]] = None,
    max_namespaces: int = 10
) -> Dict[str, Any]:
    """
    Manage training data for the predictive log analyzer.

    This tool allows viewing, collecting, and managing failure labels used for
    supervised learning in the predictive_log_analyzer.

    Args:
        action: Action to perform:
            - "stats": Get training data statistics (default)
            - "list_failures": List recent failure labels
            - "add_failure": Manually add a failure label
            - "collect": Trigger failure collection from namespaces
            - "cleanup": Remove old training data
        failure_type: For add_failure - type of failure (e.g., "crash", "oom", "image", "timeout")
        namespace: For add_failure/list_failures - namespace filter
        resource_name: For add_failure - name of the affected resource
        severity: For add_failure - severity level ("critical", "high", "medium", "low")
        collect_from_namespaces: For collect - specific namespaces to collect from
        max_namespaces: For collect - maximum namespaces when auto-detecting

    Returns:
        Dict with action results: statistics, failure list, or operation status.
    """
    try:
        from helpers.ml_persistence import (
            TrainingDataStore,
            FailureEventCollector,
            ModelPersistenceManager
        )

        training_store = TrainingDataStore()
        model_manager = ModelPersistenceManager()

        result = {
            "action": action,
            "success": True,
            "timestamp": datetime.now().isoformat()
        }

        if action == "stats":
            # Get comprehensive training data statistics
            stats = training_store.get_statistics()

            # Get model info
            models = model_manager.list_models()
            current_model_id = model_manager.get_current_model_id()

            result["statistics"] = {
                "training_data": stats,
                "models": {
                    "total_models": len(models),
                    "current_model_id": current_model_id,
                    "recent_models": [
                        {
                            "model_id": m.get("model_id"),
                            "created_at": m.get("created_at"),
                            "training_samples": m.get("training_samples", 0)
                        }
                        for m in models[-5:]
                    ] if models else []
                },
                "recommendations": []
            }

            # Add recommendations
            if stats.get("total_failure_labels", 0) < 10:
                result["statistics"]["recommendations"].append(
                    "Collect more failure labels using action='collect' to improve predictions"
                )
            if stats.get("total_log_samples", 0) < 100:
                result["statistics"]["recommendations"].append(
                    "Run predictive_log_analyzer to collect more log samples"
                )
            if stats.get("total_correlations", 0) == 0:
                result["statistics"]["recommendations"].append(
                    "No log-failure correlations yet. Labels will be correlated during analysis."
                )

        elif action == "list_failures":
            # List recent failure labels
            from datetime import timedelta
            failures = training_store.get_failure_labels_in_window(
                start_time=datetime.now() - timedelta(days=7),
                end_time=datetime.now()
            )

            # Filter by namespace if specified
            if namespace:
                failures = [f for f in failures if f.get("namespace") == namespace]

            result["failures"] = failures[:50]  # Limit to 50
            result["total_count"] = len(failures)
            result["filter_applied"] = {"namespace": namespace} if namespace else None

        elif action == "add_failure":
            # Manually add a failure label
            if not failure_type:
                result["success"] = False
                result["error"] = "failure_type is required for add_failure action"
                return result

            label = {
                "failure_type": failure_type,
                "severity": severity or "medium",
                "namespace": namespace or "unknown",
                "resource_name": resource_name or "manual_entry",
                "resource_type": "manual",
                "failure_time": datetime.now().isoformat(),
                "detection_source": "manual",
                "error_category": failure_type,
                "metadata": {
                    "source": "manage_prediction_training_data",
                    "added_by": "user"
                }
            }

            label_id = training_store.store_failure_label(label)

            if label_id:
                result["label_id"] = label_id
                result["message"] = f"Successfully added failure label for '{failure_type}'"
            else:
                result["success"] = False
                result["message"] = "Failed to add label (may be duplicate)"

        elif action == "collect":
            # Trigger failure collection from namespaces
            failure_collector = FailureEventCollector(training_store)
            collected_counts = {
                "from_events": 0,
                "from_pods": 0,
                "from_pipelines": 0,
                "namespaces_scanned": 0
            }

            # Determine namespaces to scan
            if collect_from_namespaces:
                target_namespaces = collect_from_namespaces
            else:
                # Auto-detect active namespaces
                try:
                    all_ns = await list_namespaces()
                    tekton_ns = await detect_tekton_namespaces()
                    active_ns = []
                    for category in tekton_ns.values():
                        active_ns.extend(category)
                    target_namespaces = list(set(active_ns))[:max_namespaces] if active_ns else all_ns[:max_namespaces]
                except Exception:
                    target_namespaces = []

            for ns in target_namespaces:
                try:
                    collected_counts["namespaces_scanned"] += 1

                    # Collect from events - use dict format for FailureEventCollector
                    try:
                        events_as_dicts = await _get_namespace_events_as_dicts(ns, limit=200)
                        if events_as_dicts:
                            count = failure_collector.collect_from_events(events_as_dicts, ns)
                            collected_counts["from_events"] += count
                    except Exception as e:
                        logger.debug(f"Failed to collect events from {ns}: {e}")

                    # Collect from pod statuses
                    try:
                        pods = k8s_core_api.list_namespaced_pod(namespace=ns, limit=100)
                        count = failure_collector.collect_from_pod_status(pods.items, ns)
                        collected_counts["from_pods"] += count
                    except Exception as e:
                        logger.debug(f"Failed to collect pod statuses from {ns}: {e}")

                    # Collect from pipeline runs
                    try:
                        prs = await list_pipelineruns(namespace=ns)
                        if prs and isinstance(prs, list):
                            # Filter to failed pipelines
                            failed_prs = [pr for pr in prs if pr.get("status") in ["Failed", "Error", "CouldntGetTask"]]
                            count = failure_collector.collect_from_pipeline_runs(
                                [{"status": {"conditions": [{"type": "Succeeded", "status": "False", "message": pr.get("status", "")}]},
                                  "metadata": {"name": pr.get("name"), "creationTimestamp": pr.get("started_at")},
                                  "spec": {"pipelineRef": {"name": pr.get("pipeline", "")}}}
                                 for pr in failed_prs],
                                ns
                            )
                            collected_counts["from_pipelines"] += count
                    except Exception as e:
                        logger.debug(f"Failed to collect pipeline runs from {ns}: {e}")

                except Exception as e:
                    logger.debug(f"Failed to scan namespace {ns}: {e}")

            result["collected"] = collected_counts
            result["total_collected"] = sum([
                collected_counts["from_events"],
                collected_counts["from_pods"],
                collected_counts["from_pipelines"]
            ])
            result["message"] = f"Collected {result['total_collected']} failure labels from {collected_counts['namespaces_scanned']} namespaces"

        elif action == "cleanup":
            # Clean up old training data
            deleted_data = training_store.cleanup_old_data(max_age_days=90)
            deleted_models = model_manager.cleanup_old_models(max_age_days=30, keep_min=3)

            result["cleanup_results"] = {
                "training_data_deleted": deleted_data,
                "models_deleted": deleted_models
            }
            result["message"] = f"Cleaned up {deleted_data} old data records and {deleted_models} old models"

        else:
            result["success"] = False
            result["error"] = f"Unknown action: {action}. Valid actions: stats, list_failures, add_failure, collect, cleanup"

        return result

    except ImportError as e:
        return {
            "action": action,
            "success": False,
            "error": f"ML persistence module not available: {e}",
            "message": "Install required dependencies: pip install joblib scikit-learn"
        }
    except Exception as e:
        logger.error(f"Error in manage_prediction_training_data: {e}", exc_info=True)
        return {
            "action": action,
            "success": False,
            "error": str(e)
        }


# ============================================================================
# RESOURCE BOTTLENECK FORECASTER HELPER FUNCTIONS
# ============================================================================


def _get_active_node_names() -> set:
    """Get the set of currently active (Ready) node names from Kubernetes API."""
    try:
        nodes = k8s_core_api.list_node()
        active_nodes = set()
        for node in nodes.items:
            # Check if node is Ready
            is_ready = False
            if node.status and node.status.conditions:
                for condition in node.status.conditions:
                    if condition.type == "Ready" and condition.status == "True":
                        is_ready = True
                        break
            if is_ready:
                # Add node name and possible instance name variations
                node_name = node.metadata.name
                active_nodes.add(node_name)
                # Also add IP-based names that Prometheus might use
                if node.status and node.status.addresses:
                    for addr in node.status.addresses:
                        if addr.type in ["InternalIP", "ExternalIP", "Hostname"]:
                            active_nodes.add(addr.address)
                            # Prometheus often uses IP:port format
                            active_nodes.add(f"{addr.address}:9100")
        return active_nodes
    except Exception as e:
        logger.warning(f"Could not get active nodes from K8s API: {e}")
        return set()  # Return empty set - will not filter anything


def _is_node_active(node_identifier: str, active_nodes: set) -> bool:
    """Check if a node identifier matches any active node."""
    if not active_nodes:
        return True  # If we couldn't get active nodes, don't filter

    # Direct match
    if node_identifier in active_nodes:
        return True

    # Try matching without port suffix
    node_without_port = node_identifier.split(':')[0] if ':' in node_identifier else node_identifier
    if node_without_port in active_nodes:
        return True

    # Try matching the hostname part (e.g., ip-10-206-24-150.ec2.internal)
    for active_node in active_nodes:
        if node_identifier.startswith(active_node) or active_node.startswith(node_identifier):
            return True
        # Handle case where Prometheus uses IP and K8s uses hostname
        if node_without_port in active_node or active_node in node_without_port:
            return True

    return False


async def _analyze_node_resources_new(trend_period: str, forecast_horizon: str, log) -> List[Dict]:
    """Analyze node-level resource utilization using Prometheus query method."""
    try:
        from datetime import timedelta

        # Get currently active nodes to filter out historical/terminated nodes
        active_nodes = _get_active_node_names()
        log.info(f"Found {len(active_nodes)} active nodes from Kubernetes API")

        # Calculate time range for trend analysis
        end_time = datetime.now()
        start_time = end_time - parse_time_period(trend_period)

        # Convert to ISO format for the query method
        start_time_iso = start_time.isoformat() + "Z"
        end_time_iso = end_time.isoformat() + "Z"

        forecasts = []
        filtered_count = 0
        forecast_points = calculate_forecast_intervals(forecast_horizon)

        # Node CPU usage query - aggregate to avoid series explosion from pod restarts
        cpu_query = 'max by (instance) (100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))'

        try:
            cpu_result = await prometheus_query(
                query=cpu_query,
                query_type="range",
                start_time=start_time_iso,
                end_time=end_time_iso,
                step="300s",
                limit=100  # Limit to top 100 nodes
            )

            if cpu_result.get("status") == "success" and cpu_result.get("data"):
                for metric in cpu_result["data"]:
                    node = metric.get('metric', {}).get('instance', 'unknown')

                    # Filter out nodes that are no longer active
                    if not _is_node_active(node, active_nodes):
                        filtered_count += 1
                        continue

                    values = [float(point[1]) for point in metric.get('values', [])]

                    if values:
                        forecast_result = simple_linear_forecast(values, forecast_points)
                        current_usage = values[-1] if values else 0

                        # Predict exhaustion time
                        predicted_exhaustion = None
                        if forecast_result['growth_rate'] > 0:
                            # Calculate when it might reach 90%
                            points_to_90 = (90 - current_usage) / forecast_result['growth_rate']
                            if points_to_90 > 0:
                                exhaustion_time = end_time + timedelta(minutes=5 * points_to_90)
                                predicted_exhaustion = exhaustion_time.isoformat()

                        forecasts.append({
                            'resource_type': 'cpu',
                            'resource_identifier': {'node': node, 'metric': 'cpu_utilization_percent'},
                            'current_usage': {'value': current_usage, 'unit': 'percent'},
                            'predicted_exhaustion': predicted_exhaustion,
                            'growth_rate': {'value': forecast_result['growth_rate'], 'unit': 'percent_per_5min'},
                            'contributing_factors': ['workload_scaling', 'baseline_usage_trend']
                        })
        except Exception as e:
            log.warning(f"Error fetching CPU metrics: {str(e)}")

        # Node memory usage query - aggregate by instance to avoid series explosion from pod restarts
        memory_query = 'max by (instance) ((1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100)'

        try:
            memory_result = await prometheus_query(
                query=memory_query,
                query_type="range",
                start_time=start_time_iso,
                end_time=end_time_iso,
                step="300s",
                limit=100  # Limit to top 100 nodes
            )

            if memory_result.get("status") == "success" and memory_result.get("data"):
                for metric in memory_result["data"]:
                    node = metric.get('metric', {}).get('instance', 'unknown')

                    # Filter out nodes that are no longer active
                    if not _is_node_active(node, active_nodes):
                        filtered_count += 1
                        continue

                    values = [float(point[1]) for point in metric.get('values', [])]

                    if values:
                        forecast_result = simple_linear_forecast(values, forecast_points)
                        current_usage = values[-1] if values else 0

                        predicted_exhaustion = None
                        if forecast_result['growth_rate'] > 0:
                            points_to_90 = (90 - current_usage) / forecast_result['growth_rate']
                            if points_to_90 > 0:
                                exhaustion_time = end_time + timedelta(minutes=5 * points_to_90)
                                predicted_exhaustion = exhaustion_time.isoformat()

                        forecasts.append({
                            'resource_type': 'memory',
                            'resource_identifier': {'node': node, 'metric': 'memory_utilization_percent'},
                            'current_usage': {'value': current_usage, 'unit': 'percent'},
                            'predicted_exhaustion': predicted_exhaustion,
                            'growth_rate': {'value': forecast_result['growth_rate'], 'unit': 'percent_per_5min'},
                            'contributing_factors': ['memory_leaks', 'workload_growth', 'cache_usage']
                        })
        except Exception as e:
            log.warning(f"Error fetching memory metrics: {str(e)}")

        # Node disk usage query - filter out kubelet pod volumes and aggregate by instance/mountpoint
        # to avoid series explosion from node-exporter pod restarts
        disk_query = '''max by (instance, mountpoint) (
            (1 - (node_filesystem_avail_bytes{fstype!="tmpfs", mountpoint!~"/var/lib/kubelet/pods.*|/run/.*"}
                / node_filesystem_size_bytes{fstype!="tmpfs", mountpoint!~"/var/lib/kubelet/pods.*|/run/.*"})) * 100
        )'''

        try:
            disk_result = await prometheus_query(
                query=disk_query,
                query_type="range",
                start_time=start_time_iso,
                end_time=end_time_iso,
                step="300s",
                limit=200  # Limit disk filesystems to top 200
            )

            if disk_result.get("status") == "success" and disk_result.get("data"):
                for metric in disk_result["data"]:
                    node = metric.get('metric', {}).get('instance', 'unknown')

                    # Filter out nodes that are no longer active
                    if not _is_node_active(node, active_nodes):
                        filtered_count += 1
                        continue

                    mountpoint = metric.get('metric', {}).get('mountpoint', 'unknown')
                    values = [float(point[1]) for point in metric.get('values', [])]

                    if values:
                        forecast_result = simple_linear_forecast(values, forecast_points)
                        current_usage = values[-1] if values else 0

                        predicted_exhaustion = None
                        if forecast_result['growth_rate'] > 0:
                            points_to_90 = (90 - current_usage) / forecast_result['growth_rate']
                            if points_to_90 > 0:
                                exhaustion_time = end_time + timedelta(minutes=5 * points_to_90)
                                predicted_exhaustion = exhaustion_time.isoformat()

                        forecasts.append({
                            'resource_type': 'disk',
                            'resource_identifier': {'node': node, 'mountpoint': mountpoint, 'metric': 'disk_utilization_percent'},
                            'current_usage': {'value': current_usage, 'unit': 'percent'},
                            'predicted_exhaustion': predicted_exhaustion,
                            'growth_rate': {'value': forecast_result['growth_rate'], 'unit': 'percent_per_5min'},
                            'contributing_factors': ['log_growth', 'cache_accumulation', 'temporary_files']
                        })
        except Exception as e:
            log.warning(f"Error fetching disk metrics: {str(e)}")

        if filtered_count > 0:
            log.info(f"Filtered out {filtered_count} metrics from inactive/historical nodes")

        return forecasts

    except Exception as e:
        log.error(f"Error analyzing node resources: {str(e)}")
        return []


async def _analyze_cluster_capacity_new(core_api, log) -> Dict[str, Any]:
    """Analyze overall cluster capacity and health using Prometheus query method."""
    try:
        # Get current cluster resource allocation from Kubernetes API
        nodes = core_api.list_node()

        total_cpu = 0
        total_memory = 0
        total_nodes = len(nodes.items)

        for node in nodes.items:
            if node.status and node.status.capacity:
                cpu_str = node.status.capacity.get('cpu', '0')
                memory_str = node.status.capacity.get('memory', '0Ki')

                # Parse CPU (cores)
                if 'm' in cpu_str:
                    total_cpu += int(cpu_str.replace('m', '')) / 1000
                else:
                    total_cpu += int(cpu_str)

                # Parse Memory (bytes)
                if memory_str.endswith('Ki'):
                    total_memory += int(memory_str[:-2]) * 1024
                elif memory_str.endswith('Mi'):
                    total_memory += int(memory_str[:-2]) * 1024 * 1024
                elif memory_str.endswith('Gi'):
                    total_memory += int(memory_str[:-2]) * 1024 * 1024 * 1024

        # Get current cluster resource usage via Prometheus
        cpu_usage_percent = 0
        memory_usage_percent = 0

        try:
            # Cluster CPU usage
            cpu_usage_result = await prometheus_query(
                'avg(100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))'
            )
            if cpu_usage_result.get("status") == "success" and cpu_usage_result.get("data"):
                data = cpu_usage_result["data"]
                if data and len(data) > 0 and 'value' in data[0]:
                    cpu_usage_percent = float(data[0]['value'][1])
        except Exception as e:
            log.warning(f"Could not fetch cluster CPU usage: {str(e)}")

        try:
            # Cluster memory usage
            memory_usage_result = await prometheus_query(
                'avg(100 - (avg by (instance) (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100)'
            )
            if memory_usage_result.get("status") == "success" and memory_usage_result.get("data"):
                data = memory_usage_result["data"]
                if data and len(data) > 0 and 'value' in data[0]:
                    memory_usage_percent = float(data[0]['value'][1])
        except Exception as e:
            log.warning(f"Could not fetch cluster memory usage: {str(e)}")

        # Determine overall health
        overall_health = "healthy"
        if cpu_usage_percent > 80 or memory_usage_percent > 80:
            overall_health = "degraded"
        elif cpu_usage_percent > 90 or memory_usage_percent > 90:
            overall_health = "critical"

        # Identify most constrained resources
        constrained_resources = []
        if cpu_usage_percent > 70:
            constrained_resources.append(f"CPU ({cpu_usage_percent:.1f}%)")
        if memory_usage_percent > 70:
            constrained_resources.append(f"Memory ({memory_usage_percent:.1f}%)")

        return {
            "overall_health": overall_health,
            "total_nodes": total_nodes,
            "total_cpu_cores": total_cpu,
            "total_memory_gb": round(total_memory / (1024**3), 1),
            "current_cpu_usage": f"{cpu_usage_percent:.1f}%",
            "current_memory_usage": f"{memory_usage_percent:.1f}%",
            "most_constrained_resources": constrained_resources,
            "fastest_growing_consumers": [],  # Would need historical analysis
            "capacity_runway": {
                "cpu_runway_days": max(0, int((90 - cpu_usage_percent) / max(0.1, cpu_usage_percent / 30))),
                "memory_runway_days": max(0, int((90 - memory_usage_percent) / max(0.1, memory_usage_percent / 30)))
            }
        }

    except Exception as e:
        log.error(f"Error analyzing cluster capacity: {str(e)}")
        return {
            "overall_health": "unknown",
            "total_nodes": 0,
            "total_cpu_cores": 0,
            "total_memory_gb": 0,
            "current_cpu_usage": "unknown",
            "current_memory_usage": "unknown",
            "most_constrained_resources": [],
            "fastest_growing_consumers": [],
            "capacity_runway": {}
        }


@mcp.tool()
@log_tool_execution
async def resource_bottleneck_forecaster(
    forecast_horizon: str = "24h",
    resource_types: Optional[List[str]] = None,
    namespaces: Optional[List[str]] = None,
    trend_analysis_period: str = "7d"
) -> Dict[str, Any]:
    """
    Forecast resource bottlenecks by analyzing utilization trends and predicting exhaustion points.

    Uses time-series analysis to predict CPU, memory, disk, and network capacity constraints.

    Args:
        forecast_horizon: Forecast window - "1h", "6h", "24h", "7d", "30d" (default: "24h").
        resource_types: Resources to analyze - cpu, memory, disk, network, pvc (default: all).
        namespaces: Specific namespaces to focus on.
        trend_analysis_period: Historical period for trends (default: "7d").

    Returns:
        Dict: Keys: forecasts, capacity_recommendations, cluster_overview, historical_accuracy.
    """
    try:
        logger.info(f"Starting resource bottleneck forecasting for horizon: {forecast_horizon}")

        # Default resource types if not specified
        if resource_types is None:
            resource_types = ["cpu", "memory", "disk", "network", "pvc"]

        # Test Prometheus connectivity using the tool
        try:
            test_query_result = await prometheus_query("up")
            if test_query_result.get("status") != "success":
                logger.warning("Could not connect to Prometheus endpoint, using mock data")
                return {
                    "forecasts": [],
                    "capacity_recommendations": [{
                        "resource": "monitoring",
                        "current_capacity": "unavailable",
                        "recommended_capacity": "install_prometheus_or_check_connectivity",
                        "scaling_urgency": "high",
                        "implementation_options": ["Check Prometheus deployment", "Verify RBAC permissions", "Check cluster connectivity"]
                    }],
                    "cluster_overview": {
                        "overall_health": "monitoring_unavailable",
                        "most_constrained_resources": [],
                        "fastest_growing_consumers": [],
                        "capacity_runway": {}
                    },
                    "historical_accuracy": {
                        "previous_predictions": 0,
                        "accuracy_rate": 0.0,
                        "last_validation": datetime.now().isoformat()
                    }
                }
        except Exception as e:
            logger.warning(f"Error testing Prometheus connectivity: {str(e)}")
            return {
                "forecasts": [],
                "capacity_recommendations": [{
                    "resource": "monitoring",
                    "current_capacity": "error",
                    "recommended_capacity": "fix_monitoring_setup",
                    "scaling_urgency": "high",
                    "implementation_options": ["Check Prometheus deployment", "Verify authentication", "Review cluster configuration"]
                }],
                "cluster_overview": {
                    "overall_health": "monitoring_error",
                    "most_constrained_resources": [],
                    "fastest_growing_consumers": [],
                    "capacity_runway": {}
                },
                "historical_accuracy": {
                    "previous_predictions": 0,
                    "accuracy_rate": 0.0,
                    "last_validation": datetime.now().isoformat()
                }
            }

        # Analyze node-level resources
        forecasts = []
        if "cpu" in resource_types or "memory" in resource_types or "disk" in resource_types:
            node_forecasts = await _analyze_node_resources_new(trend_analysis_period, forecast_horizon, logger)

            if namespaces:
                # When specific namespaces are requested, limit node output to prevent
                # bloated responses (56+ nodes * 3 resource types * mountpoints = 200+ entries).
                # Strategy: keep top 5 nodes by max utilization across all resource types.
                MAX_NODES = 5

                # Group forecasts by node
                node_max_usage = {}
                node_has_exhaustion = {}
                for f in node_forecasts:
                    node = f.get('resource_identifier', {}).get('node', f.get('resource_identifier', {}).get('instance', 'unknown'))
                    usage = f.get('current_usage', {}).get('value', 0)
                    node_max_usage[node] = max(node_max_usage.get(node, 0), usage)
                    if f.get('predicted_exhaustion'):
                        node_has_exhaustion[node] = True

                # Select top nodes: exhaustion-approaching first, then highest utilization
                sorted_nodes = sorted(
                    node_max_usage.keys(),
                    key=lambda n: (node_has_exhaustion.get(n, False), node_max_usage[n]),
                    reverse=True
                )
                keep_nodes = set(sorted_nodes[:MAX_NODES])

                # Filter forecasts to only keep selected nodes
                trimmed = [f for f in node_forecasts
                           if f.get('resource_identifier', {}).get('node', f.get('resource_identifier', {}).get('instance', '')) in keep_nodes]
                forecasts.extend(trimmed)

                if len(node_forecasts) > len(trimmed):
                    logger.info(f"Trimmed node forecasts from {len(node_forecasts)} entries ({len(node_max_usage)} nodes) "
                                f"to {len(trimmed)} entries ({len(keep_nodes)} nodes)")
            else:
                forecasts.extend(node_forecasts)

        # Analyze namespace-specific resources if specified
        if namespaces:
            for namespace in namespaces:
                try:
                    # Namespace CPU usage
                    namespace_cpu_query = f'sum(rate(container_cpu_usage_seconds_total{{namespace="{namespace}"}}[5m])) * 100'

                    # Get current namespace resource usage
                    cpu_result = await prometheus_query(namespace_cpu_query)
                    if cpu_result.get("status") == "success" and cpu_result.get("data"):
                        data = cpu_result["data"]
                        if data and len(data) > 0 and 'value' in data[0]:
                            cpu_usage = float(data[0]['value'][1])

                            # Add namespace-specific forecast
                            forecasts.append({
                                'resource_type': 'namespace_cpu',
                                'resource_identifier': {'namespace': namespace, 'metric': 'cpu_usage_cores'},
                                'current_usage': {'value': cpu_usage, 'unit': 'cores'},
                                'predicted_exhaustion': None,  # Would need trend analysis
                                'growth_rate': {'value': 0, 'unit': 'cores_per_5min'},
                                'contributing_factors': ['pod_scaling', 'workload_changes']
                            })

                    # Namespace memory usage — try primary metric, then fallback
                    memory_queries = [
                        f'sum(container_memory_working_set_bytes{{namespace="{namespace}"}}) / 1024 / 1024 / 1024',
                        f'sum(container_memory_usage_bytes{{namespace="{namespace}"}}) / 1024 / 1024 / 1024'
                    ]
                    memory_usage_gb = 0
                    for memory_query in memory_queries:
                        memory_result = await prometheus_query(memory_query)
                        if memory_result.get("status") == "success" and memory_result.get("data"):
                            data = memory_result["data"]
                            if data and len(data) > 0:
                                raw_val = data[0].get('value', [0, '0'])
                                if isinstance(raw_val, list) and len(raw_val) >= 2:
                                    memory_usage_gb = float(raw_val[1])
                                elif isinstance(raw_val, (str, int, float)):
                                    memory_usage_gb = float(raw_val)
                                if memory_usage_gb > 0:
                                    break

                    if memory_usage_gb > 0:
                        forecasts.append({
                            'resource_type': 'namespace_memory',
                            'resource_identifier': {'namespace': namespace, 'metric': 'memory_usage_gb'},
                            'current_usage': {'value': memory_usage_gb, 'unit': 'GB'},
                            'predicted_exhaustion': None,  # Would need trend analysis
                            'growth_rate': {'value': 0, 'unit': 'GB_per_5min'},
                            'contributing_factors': ['pod_scaling', 'memory_leaks', 'cache_growth']
                        })

                except Exception as e:
                    logger.warning(f"Could not analyze namespace {namespace}: {str(e)}")

        # Generate capacity recommendations
        capacity_recommendations = []
        critical_forecasts = [f for f in forecasts if f.get('predicted_exhaustion')]

        for forecast in critical_forecasts:
            resource_type = forecast['resource_type']
            current_usage = forecast['current_usage']['value']

            urgency = "low"
            if forecast['predicted_exhaustion']:
                try:
                    exhaustion_time = datetime.fromisoformat(forecast['predicted_exhaustion'].replace('Z', '+00:00'))
                    time_to_exhaustion = exhaustion_time - datetime.now(exhaustion_time.tzinfo)

                    if time_to_exhaustion.total_seconds() < 3600:  # 1 hour
                        urgency = "critical"
                    elif time_to_exhaustion.total_seconds() < 86400:  # 24 hours
                        urgency = "high"
                    elif time_to_exhaustion.total_seconds() < 604800:  # 7 days
                        urgency = "medium"
                except:
                    urgency = "medium"

            if resource_type == "cpu":
                capacity_recommendations.append({
                    "resource": f"cpu_{forecast['resource_identifier']['node']}",
                    "current_capacity": f"{current_usage:.1f}%",
                    "recommended_capacity": "scale_up_nodes" if current_usage > 70 else "optimize_workloads",
                    "scaling_urgency": urgency,
                    "implementation_options": [
                        "Add worker nodes",
                        "Implement CPU limits",
                        "Optimize container resource requests",
                        "Consider pod autoscaling"
                    ]
                })
            elif resource_type == "memory":
                capacity_recommendations.append({
                    "resource": f"memory_{forecast['resource_identifier']['node']}",
                    "current_capacity": f"{current_usage:.1f}%",
                    "recommended_capacity": "increase_memory" if current_usage > 80 else "review_memory_usage",
                    "scaling_urgency": urgency,
                    "implementation_options": [
                        "Upgrade node memory",
                        "Implement memory limits",
                        "Review memory-intensive workloads",
                        "Enable memory optimization"
                    ]
                })

        # Analyze cluster overview
        cluster_overview = await _analyze_cluster_capacity_new(k8s_core_api, logger)

        # Historical accuracy - not tracked (requires prediction validation pipeline)
        historical_accuracy = {
            "previous_predictions": len(forecasts),
            "accuracy_rate": None,
            "last_validation": None,
            "note": "Prediction validation not implemented - accuracy not tracked"
        }

        result = {
            "forecasts": forecasts,
            "capacity_recommendations": capacity_recommendations,
            "cluster_overview": cluster_overview,
            "historical_accuracy": historical_accuracy
        }

        logger.info(f"Completed resource bottleneck forecasting. Generated {len(forecasts)} forecasts and {len(capacity_recommendations)} recommendations")
        return result

    except Exception as e:
        logger.error(f"Error in resource bottleneck forecasting: {str(e)}", exc_info=True)
        return {
            "forecasts": [],
            "capacity_recommendations": [{
                "resource": "error",
                "current_capacity": "unknown",
                "recommended_capacity": "check_monitoring_setup",
                "scaling_urgency": "medium",
                "implementation_options": ["Verify Prometheus deployment", "Check RBAC permissions"]
            }],
            "cluster_overview": {
                "overall_health": "error",
                "most_constrained_resources": [],
                "fastest_growing_consumers": [],
                "capacity_runway": {}
            },
            "historical_accuracy": {
                "previous_predictions": 0,
                "accuracy_rate": 0.0,
                "last_validation": datetime.now().isoformat()
            }
        }


# Tool 19: Semantic Log Search
@mcp.tool()
async def semantic_log_search(
    query: str,
    time_range: str = "1h",
    namespaces: Optional[List[str]] = None,
    severity_levels: Optional[List[str]] = None,
    max_results: int = 100,
    context_lines: int = 3,
    group_similar: bool = True
) -> Dict[str, Any]:
    """
    Search logs using natural language queries with semantic understanding beyond keyword matching.

    Uses NLP for query interpretation, Kubernetes/Tekton entity recognition, and relevance ranking.

    Args:
        query: Natural language query describing what to search for.
        time_range: Time range - "1h", "6h", "24h", "7d" (default: "1h").
        namespaces: Specific namespaces to search (default: auto-detect relevant namespaces).
        severity_levels: Log severity levels to include.
        max_results: Maximum results to return (default: 100).
        context_lines: Surrounding lines per match (default: 3).
        group_similar: Group similar log entries (default: True).

    Returns:
        Dict: Keys: query_interpretation, search_results, result_summary, suggestions.
    """
    logger.info(f"Starting semantic log search for query: '{query}' with time_range: {time_range}")

    try:
        # === Query Understanding and Interpretation ===
        query_interpretation = interpret_semantic_query(query, time_range)
        logger.info(f"Query interpreted as: {query_interpretation['interpreted_intent']}")

        # === Determine Search Strategy ===
        search_strategy = determine_search_strategy(query_interpretation)
        logger.info(f"Using search strategy: {search_strategy['strategy']}")

        # === Entity Recognition and Context Building ===
        identified_components = extract_k8s_entities(query)
        logger.info(f"Identified components: {identified_components}")

        # === Build Search Parameters ===
        search_params = {
            'namespaces': await _get_target_namespaces(namespaces, identified_components, list_namespaces, detect_tekton_namespaces),
            'time_range': time_range,
            'severity_levels': severity_levels or ['error', 'warn', 'info', 'debug'],
            'max_results': max_results,
            'context_lines': context_lines
        }

        # === Execute Semantic Search ===
        search_results = []
        sources_searched = 0

        # Search across identified namespaces with fixed function calls
        for namespace in search_params['namespaces']:
            logger.info(f"Searching namespace: {namespace}")
            try:
                namespace_results = []

                # Get pods in namespace
                pods_info = await list_pods_in_namespace(namespace)

                # Search pod logs with correct arguments
                for pod_info in pods_info[:5]:  # Limit to 5 pods per namespace for performance
                    if isinstance(pod_info, dict) and 'error' not in pod_info:
                        try:
                            pod_logs_result = await _search_pod_logs_semantically(
                                pod_info, namespace, query_interpretation, search_params,
                                get_pod_logs, _build_log_params, find_semantic_matches
                            )
                            if pod_logs_result:
                                namespace_results.extend(pod_logs_result)
                        except Exception as e:
                            logger.debug(f"Error searching pod logs in {namespace}: {e}")
                            continue

                # Search events with correct arguments
                try:
                    events_result = await _search_events_semantically(
                        namespace, query_interpretation, search_params,
                        smart_get_namespace_events, calculate_semantic_relevance,
                        identify_match_reasons, extract_log_metadata
                    )
                    if events_result:
                        namespace_results.extend(events_result)
                except Exception as e:
                    logger.debug(f"Error searching events in {namespace}: {e}")

                # Search Tekton resources if relevant
                if any(comp in ['pipelinerun', 'taskrun', 'pipeline'] for comp in query_interpretation.get('semantic_keywords', [])):
                    try:
                        tekton_results = await _search_tekton_resources_semantically(
                            namespace, query_interpretation, search_params,
                            list_pipelineruns, calculate_semantic_relevance
                        )
                        if tekton_results:
                            namespace_results.extend(tekton_results)
                    except Exception as e:
                        logger.debug(f"Error searching Tekton resources in {namespace}: {e}")

                search_results.extend(namespace_results)

            except Exception as e:
                logger.warning(f"Error searching namespace {namespace}: {e}")
                continue

            sources_searched += 1

            # Respect max_results limit
            if len(search_results) >= max_results:
                search_results = search_results[:max_results]
                break

        # === Semantic Ranking and Relevance Scoring ===
        ranked_results = rank_results_by_semantic_relevance(
            search_results, query_interpretation, group_similar
        )

        # === Pattern Analysis ===
        common_patterns = identify_common_patterns(ranked_results)
        severity_distribution = analyze_severity_distribution(ranked_results)

        # === Generate Suggestions ===
        suggestions = generate_semantic_suggestions(
            query_interpretation, ranked_results
        )

        # === Build Final Response ===
        return {
            "query_interpretation": {
                "original_query": query,
                "interpreted_intent": query_interpretation['interpreted_intent'],
                "search_strategy": search_strategy['strategy'],
                "identified_components": identified_components,
                "time_scope": time_range
            },
            "search_results": ranked_results,
            "result_summary": {
                "total_matches": len(ranked_results),
                "sources_searched": sources_searched,
                "common_patterns": common_patterns,
                "severity_distribution": severity_distribution
            },
            "suggestions": suggestions
        }

    except Exception as e:
        logger.error(f"Error in semantic log search: {str(e)}", exc_info=True)
        return {
            "query_interpretation": {
                "original_query": query,
                "interpreted_intent": "Error processing query",
                "search_strategy": "error",
                "identified_components": [],
                "time_scope": time_range
            },
            "search_results": [],
            "result_summary": {
                "total_matches": 0,
                "sources_searched": 0,
                "common_patterns": [],
                "severity_distribution": {}
            },
            "suggestions": {
                "related_queries": [],
                "broader_search": "Try simplifying your query",
                "narrower_search": "Add more specific terms"
            },
            "error": str(e)
        }


# NEW TOOL: SIMULATION SCENARIOS
@mcp.tool()
async def what_if_scenario_simulator(
    scenario_type: str,
    changes: Dict[str, Any],
    scope: Optional[Dict[str, Any]] = None,
    simulation_duration: str = "24h",
    load_profile: str = "current",
    risk_tolerance: str = "moderate"
) -> Dict[str, Any]:
    """
    Simulate impact of configuration changes before applying to live system with risk assessment.

    Uses Monte Carlo simulation and load modeling based on historical data.

    Args:
        scenario_type: Type - "resource_limits", "scaling", "configuration", "deployment".
        changes: Changes to simulate with before/after values.
        scope: Simulation scope - clusters, namespaces, components.
        simulation_duration: Duration - "1h", "24h", "7d" (default: "24h").
        load_profile: Expected load - "current", "peak", "custom" (default: "current").
        risk_tolerance: Risk level - "conservative", "moderate", "aggressive" (default: "moderate").

    Returns:
        Dict: Keys: simulation_id, impact_analysis, risk_assessment, affected_components, recommendations.
    """
    try:
        # Generate unique simulation ID
        import uuid
        from datetime import datetime

        simulation_id = f"sim-{uuid.uuid4().hex[:8]}-{int(datetime.now().timestamp())}"

        logger.info(f"Starting what-if scenario simulation {simulation_id} for {scenario_type}")

        # Validate input parameters
        valid_scenario_types = ["resource_limits", "scaling", "configuration", "deployment"]
        if scenario_type not in valid_scenario_types:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid scenario_type '{scenario_type}'. Must be one of: {valid_scenario_types}"
            }

        valid_durations = ["1h", "24h", "7d"]
        if simulation_duration not in valid_durations:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid simulation_duration '{simulation_duration}'. Must be one of: {valid_durations}"
            }

        valid_load_profiles = ["current", "peak", "custom"]
        if load_profile not in valid_load_profiles:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid load_profile '{load_profile}'. Must be one of: {valid_load_profiles}"
            }

        valid_risk_levels = ["conservative", "moderate", "aggressive"]
        if risk_tolerance not in valid_risk_levels:
            return {
                "simulation_id": simulation_id,
                "error": f"Invalid risk_tolerance '{risk_tolerance}'. Must be one of: {valid_risk_levels}"
            }

        if not changes or not isinstance(changes, dict):
            return {
                "simulation_id": simulation_id,
                "error": "Changes parameter must be a non-empty dictionary with before/after values"
            }

        # Set default scope if not provided
        if scope is None:
            scope = {
                "clusters": ["current"],
                "namespaces": ["all"],
                "components": ["all"]
            }

        # Collect baseline system data
        baseline_data = await collect_baseline_system_data(scope, k8s_core_api, list_namespaces, list_pods)

        # Build system behavior models
        behavior_models = await build_system_behavior_models(baseline_data, scenario_type)

        # Load historical performance data for calibration (using real Prometheus data)
        historical_data = await load_historical_performance_data(
            scope,
            simulation_duration,
            prometheus_query_fn=_execute_prometheus_query_internal
        )

        # Calibrate simulation models with historical data
        calibrated_models = calibrate_simulation_models(behavior_models, historical_data, load_profile)

        # Run Monte Carlo simulation for uncertainty quantification
        simulation_results = await run_monte_carlo_simulation(
            calibrated_models,
            changes,
            scenario_type,
            simulation_duration,
            risk_tolerance
        )

        # Analyze impact on different system aspects
        impact_analysis = analyze_system_impact(simulation_results, baseline_data, scenario_type)

        # Identify affected components and their dependency graph
        affected_components = await identify_affected_components(
            changes, scope, scenario_type, k8s_core_api, k8s_apps_api, list_pods, list_namespaces
        )

        # Perform risk assessment
        risk_assessment = perform_risk_assessment(
            simulation_results,
            impact_analysis,
            affected_components,
            risk_tolerance
        )

        # Calculate simulation quality metrics
        simulation_quality = calculate_simulation_quality(
            baseline_data,
            historical_data,
            calibrated_models,
            logger
        )

        # Generate recommendations
        recommendations = generate_simulation_recommendations(
            impact_analysis,
            risk_assessment,
            simulation_quality,
            scenario_type,
            logger
        )

        # Compile final results
        result = {
            "simulation_id": simulation_id,
            "scenario_description": f"{scenario_type.replace('_', ' ').title()} simulation over {simulation_duration}",
            "simulation_parameters": {
                "scenario_type": scenario_type,
                "duration": simulation_duration,
                "load_profile": load_profile,
                "risk_tolerance": risk_tolerance,
                "scope": scope,
                "changes": changes
            },
            "impact_analysis": impact_analysis,
            "affected_components": affected_components,
            "risk_assessment": risk_assessment,
            "simulation_quality": simulation_quality,
            "recommendations": recommendations,
            "timestamp": datetime.now().isoformat(),
            "simulation_duration_seconds": convert_duration_to_seconds(simulation_duration)
        }

        logger.info(f"Completed simulation {simulation_id} with {len(affected_components)} affected components")
        return result

    except Exception as e:
        logger.error(f"Error in what-if scenario simulation: {str(e)}", exc_info=True)
        return {
            "simulation_id": simulation_id if 'simulation_id' in locals() else "unknown",
            "error": f"Simulation failed: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }

@mcp.tool()
async def query_kubearchive(
    resource_type: str,
    namespace: str,
    name: Optional[str] = None,
    label_selector: Optional[str] = None,
    field_selector: Optional[str] = None,
    since_time: Optional[str] = None,
    until_time: Optional[str] = None,
    include_logs: bool = False,
    container: Optional[str] = None,
    limit: int = 100,
    output_format: str = "summary",
) -> Dict[str, Any]:
    """
    Query archived Kubernetes resources from KubeArchive (historical data no longer on the cluster).

    Single entry point for archived resources and their logs: set include_logs=True to attach logs
    for pipelinerun, taskrun, and pod results (use an exact name when you only need one resource).
    Optional container selects a container for multi-container pods.

    Args:
        resource_type: One of pipelinerun, taskrun, pod, release, snapshot (case-insensitive).
        namespace: Kubernetes namespace to search.
        name: Optional resource name; wildcards supported (e.g. my-pipeline-*).
        label_selector: Kubernetes label selector string.
        field_selector: Kubernetes field selector string (KubeArchive support may vary).
        since_time: Lower bound for creation time (RFC3339 or ISO date).
        until_time: Upper bound for creation time (RFC3339 or ISO date).
        include_logs: If True, fetch logs for each matching pod, taskrun, or pipelinerun.
        container: Optional container name (pods; passed to KubeArchive when include_logs=True).
        limit: Max resources to return (1-1000; out-of-range values are clamped).
        output_format: summary, detailed, or yaml.

    Returns:
        Dict with kubearchive_status, kubearchive_endpoint, resources, total_count, time_range,
        filters_applied, message, and error when applicable.
    """
    ka_logger = logging.getLogger("lumino-mcp.query_kubearchive")
    valid_types = ["pipelinerun", "taskrun", "pod", "release", "snapshot"]
    valid_formats = ["summary", "detailed", "yaml"]
    filters_applied = {
        "resource_type": resource_type,
        "namespace": namespace,
        "name": name,
        "label_selector": label_selector,
        "field_selector": field_selector,
        "container": container,
    }

    def _base_response(
        status: str,
        resources: Optional[List[Any]] = None,
        total: int = 0,
        endpoint: Optional[str] = None,
        error: Optional[str] = None,
        message: Optional[str] = None,
    ) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "kubearchive_status": status,
            "resources": resources if resources is not None else [],
            "total_count": total,
            "time_range": {"since": since_time, "until": until_time},
            "filters_applied": filters_applied,
        }
        if endpoint is not None:
            out["kubearchive_endpoint"] = endpoint
        if message:
            out["message"] = message
        if error:
            out["error"] = error
        return out

    ka_logger.info(
        "query_kubearchive resource_type=%s namespace=%s include_logs=%s container=%s limit=%s output_format=%s",
        resource_type,
        namespace,
        include_logs,
        container,
        limit,
        output_format,
    )

    try:
        if resource_type.lower() not in valid_types:
            return _base_response(
                "error",
                error=f"Invalid resource_type '{resource_type}'. Must be one of: {', '.join(valid_types)}",
                message="Validation failed",
            )

        if output_format not in valid_formats:
            return _base_response(
                "error",
                error=f"Invalid output_format '{output_format}'. Must be one of: {', '.join(valid_formats)}",
                message="Validation failed",
            )

        orig_limit = limit
        if limit < 1 or limit > 1000:
            limit = min(max(1, limit), 1000)
            ka_logger.warning("limit adjusted from %s to %s (valid range 1-1000)", orig_limit, limit)

        if since_time:
            try:
                since_time = normalize_to_rfc3339(since_time.strip())
            except ValueError as e:
                return _base_response(
                    "error",
                    error=f"Invalid since_time: {e}. Use RFC3339 or ISO date, e.g. 2024-01-15T10:30:00Z or 2024-01-15",
                    message="Validation failed",
                )

        if until_time:
            try:
                until_time = normalize_to_rfc3339(until_time.strip())
            except ValueError as e:
                return _base_response(
                    "error",
                    error=f"Invalid until_time: {e}. Use RFC3339 or ISO date, e.g. 2024-01-15T10:30:00Z or 2024-01-15",
                    message="Validation failed",
                )

        if kubearchive_endpoint_discovery is None:
            return _base_response(
                "error",
                error="Kubernetes API clients are not initialized. Load kubeconfig or run in-cluster.",
                message="KubeArchive discovery requires CoreV1Api and CustomObjectsApi",
            )

        availability = await check_kubearchive_availability(kubearchive_endpoint_discovery)
        ka_endpoint = availability.get("endpoint")
        if not availability.get("available"):
            msg = availability.get("message", "KubeArchive not available")
            ka_logger.warning("KubeArchive availability check failed: %s", msg)
            sug = [
                "Deploy KubeArchive (https://github.com/kubearchive/kubearchive)",
                "Set KUBEARCHIVE_HOST to your KubeArchive API base URL",
                "Example: export KUBEARCHIVE_HOST='https://kubearchive-api-server.kubearchive.svc.cluster.local:8081'",
            ]
            out = _base_response(
                "error",
                error=msg,
                message="KubeArchive unavailable",
            )
            if ka_endpoint:
                out["kubearchive_endpoint"] = ka_endpoint
            out["suggestions"] = sug
            return out

        ka_client = await setup_kubearchive_client(
            endpoint_discovery=kubearchive_endpoint_discovery,
            k8s_core_api=k8s_core_api,
        )

        result = await query_kubearchive_resources(
            kubearchive_client=ka_client,
            resource_type=resource_type,
            namespace=namespace,
            name=name,
            label_selector=label_selector,
            field_selector=field_selector,
            since_time=since_time,
            until_time=until_time,
            include_logs=include_logs,
            container=container,
            limit=limit,
            output_format=output_format,
        )

        result["filters_applied"] = filters_applied
        result["kubearchive_endpoint"] = ka_endpoint

        if result.get("kubearchive_status") == "success":
            n = result.get("total_count", 0)
            result["message"] = (
                f"Found {n} archived resource(s)" if n else "No archived resources found matching criteria"
            )
        else:
            if "message" not in result:
                result["message"] = result.get("error", "KubeArchive query failed")

        ka_logger.info("query_kubearchive completed status=%s count=%s", result.get("kubearchive_status"), result.get("total_count"))
        return result

    except Exception as e:
        ka_logger.error("query_kubearchive failed: %s", e, exc_info=True)
        out = _base_response("error", error=str(e), message="Unexpected error")
        if kubearchive_endpoint_discovery is not None:
            try:
                ep = await kubearchive_endpoint_discovery.discover_endpoint()
                if ep:
                    out["kubearchive_endpoint"] = ep
            except Exception:
                pass
        return out

try:
    import logan
    _LOGAN_AVAILABLE = True
except ImportError:
    _LOGAN_AVAILABLE = False

if _LOGAN_AVAILABLE:

    async def _run_logan_pipeline(namespace,pod_name,container_name,tail_lines,since_seconds,debug_mode="false"):

        """Shared logic for fetch logs,templatize,and return raw objects."""
        from logan.preprocessing.preprocessing import Preprocessing
        from logan.drain.run_drain import Templatizer


        logs_result=await get_pod_logs(namespace, pod_name, container_name, tail_lines=tail_lines, since_seconds=since_seconds)
        if logs_result.get("error"):
            return None,None,None,logs_result

        log_text="\n".join(logs_result["logs"].values())
        log_lines=log_text.strip().split("\n")

        tmpdir=tempfile.mkdtemp()
        input_file=os.path.join(tmpdir, "pod_logs.log")
        with open(input_file, "w") as f:
            f.write(log_text)


        output_dir=os.path.join(tmpdir, "output")
        os.makedirs(output_dir)
        os.makedirs(os.path.join(output_dir, "developer_debug_files"),exist_ok=True)
        os.makedirs(os.path.join(output_dir, "metrics"),exist_ok=True)

        os.environ["LOGAN_DISABLE_PANDARALLEL"]="1"
        os.environ["TOKENIZERS_PARALLELISM"]="false"

        pp=Preprocessing(debug_mode=debug_mode)
        pp.preprocess([input_file],"all-data",output_dir,False,True,False)

        drain_config=os.path.join(os.path.dirname(logan.__file__), "drain", "drain3.ini")

        templatizer=Templatizer(debug_mode=debug_mode,config_path=drain_config)
        templatizer.miner(pp.df,output_dir)

        return templatizer.df,output_dir,tmpdir,None

    @mcp.tool()
    async def templatize_pod_logs(
        namespace: str,
        pod_name: str,
        container_name: Optional[str]=None,
        tail_lines: int=5000,
        since_seconds: Optional[int]=None,
        debug_mode: str="false",
    )-> Dict[str,Any]:
        """Cluster pod logs into unique structural templates using Drain3 parse-tree mining.

        Unlike analyze_logs or analyze_pod_logs_hybrid which scan each line individually
        with regex, this tool groups thousands of repetitive log lines into a handful of
        distinct patterns (e.g. 10,000 lines → 25 templates). Use this when:
        - Logs are high-volume and noisy — the user needs to see the forest, not every tree
        - The user asks to "summarize", "deduplicate", "find patterns", or "reduce" logs
        - You need to know WHAT kinds of log lines exist and HOW MANY of each, not just errors

        Do NOT use this for quick error scanning — the existing analyze_logs tool is faster
        for that. Use this when volume reduction and pattern discovery matter.

        Args:
            namespace: Kubernetes namespace of the pod.
            pod_name: Name of the pod whose logs to templatize.
            container_name: Specific container (if the pod has multiple).
            tail_lines: Number of recent log lines to fetch (default 5000).
            since_seconds: Only fetch logs newer than this many seconds.
            debug_mode: "true" to write extra debug artifacts, "false" (default) for lean output.

        Returns:
            Dict with unique templates sorted by frequency, each with a sample log line.
        """
        tmpdir=None
        try:
            df,output_dir,tmpdir,error= await _run_logan_pipeline(namespace, pod_name, container_name, tail_lines=tail_lines, since_seconds=since_seconds,debug_mode=debug_mode)
            if error:
                return {
                    "status": "error",
                    "error": error,
                    "message": "Failed to templatize pod logs",
                }

            groups=df.groupby("test_ids").agg(
                template=("template_str","first"),
                count=("test_ids","count"),
                sample=("text","first"),
            ).reset_index().sort_values(by="count",ascending=False)

            return {
                "status": "success",
                "pod":f"{namespace}/{pod_name}",
                "total_unique_templates": len(groups),
                "templates": [
                    {
                        "template_id": str(row["test_ids"]),
                        "template_str": row["template"],
                        "occurences": int(row["count"]),
                        "sample":str(row["sample"])[:200],
                    } for _, row in groups.iterrows()
                ]
            }
        except Exception as e:
            return {"status": "error", "error": str(e), "message": str(e)}
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir,ignore_errors=True)

    @mcp.tool()
    async def deep_analyze_pod_logs(
        namespace: str,
        pod_name: str,
        container_name: Optional[str]=None,
        tail_lines: int=5000,
        since_seconds: Optional[int]=None,
        debug_mode: str="true",
    )-> Dict[str,Any]:
        """Classify pod logs into golden signals and fault categories using ML (zero-shot transformer).

        Goes beyond regex error scanning: first clusters logs into templates (Drain3), then
        classifies each template into a golden signal (Error, Latency, Saturation, Availability,
        Traffic, Info) and fault category (io, authentication, network, application, device)
        using a cross-encoder BART model. Use this when:
        - The user asks for root-cause hints, anomaly classification, or "what's wrong"
        - You need to distinguish between TYPES of problems (network vs auth vs saturation)
        - The user wants a golden-signal breakdown of pod health, not just an error list
        - analyze_logs found errors but the user needs deeper categorization of those errors

        Do NOT use this for quick error checks — it loads an ML model on first call (~30-60s).
        Prefer analyze_logs or analyze_pod_logs_hybrid for fast, lightweight scanning.

        Args:
            namespace: Kubernetes namespace of the pod.
            pod_name: Name of the pod to analyze.
            container_name: Specific container (if the pod has multiple).
            tail_lines: Number of recent log lines to fetch (default 5000).
            since_seconds: Only fetch logs newer than this many seconds.
            debug_mode: "true" (default) to generate signal map artifacts needed for classification.

        Returns:
            Dict with golden_signal_distribution (signal → count) and per-template results
            including golden signal, fault category, occurrence count, and sample log line.
        """
        from logan.log_diagnosis.anomaly import Anomaly
        from logan.log_diagnosis.models import ModelManager, ModelType
        from logan.log_diagnosis.models.model_zero_shot_classifer import ZeroShotModels

        tmpdir=None
        try:
            df,output_dir,tmpdir,error= await _run_logan_pipeline(namespace, pod_name, container_name, tail_lines=tail_lines, since_seconds=since_seconds,debug_mode=debug_mode)

            if error:
                return {
                    "status": "error",
                    "error": error,
                    "message": "Failed to deep analyze pod logs",
                }

            model_enum=ZeroShotModels.CROSSENCODER
            model_mgr=ModelManager(ModelType.ZERO_SHOT,model_enum)

            anomaly=object.__new__(Anomaly)
            anomaly.debug_mode="true"
            anomaly.model_manager=model_mgr

            anomaly_report=anomaly.get_anomaly_report(df,output_dir)
            signal_map_path=os.path.join(output_dir, "developer_debug_files","temp_id_to_signal_map.json")

            signal_map={}
            if os.path.exists(signal_map_path):
                with open(signal_map_path, "r") as f:
                    signal_map=json.load(f)

            gs_distribution={}
            results=[]

            counts= df.groupby(["test_ids","file_names"]).agg(
                log_lines=("text","first"),
                occurrences=("text","size"),
            ).reset_index()

            for _,row in counts.iterrows():
                t_id=str(row["test_ids"])
                f_name=str(row["file_names"])
                key=str((t_id,f_name))
                gs,fault=signal_map.get(key,["Info",["other"]])

                gs_distribution[gs]=gs_distribution.get(gs,0)+int(row["occurrences"])

                results.append({
                    "template_id": t_id,
                    "file_name": f_name,
                    "log_lines": str(row["log_lines"])[:200],
                    "occurrences": int(row["occurrences"]),
                    "gs": gs,
                    "fault": fault[0] if isinstance(fault,list) and fault else fault,
                })

            results.sort(key=lambda x: x["occurrences"],reverse=True)
            return {
                "status": "success",
                "pod":f"{namespace}/{pod_name}",
                "total_log_lines":len(df),
                "total_unique_templates": df["test_ids"].nunique(),
                "golden_signal_distribution": gs_distribution,
                "results": results,
            }

        except Exception as e:
            return {"status": "error", "error": str(e), "message": str(e)}
        finally:
            if tmpdir:
                shutil.rmtree(tmpdir,ignore_errors=True)