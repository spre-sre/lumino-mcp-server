# ============================================================================
# FAILURE ANALYSIS HELPER MODULE
# ============================================================================
#
# This module contains all failure analysis related classes, functions, and utilities
# used by the MCP server for Root Cause Analysis (RCA), failure detection, and remediation planning.
# ============================================================================

from datetime import datetime
from typing import Dict, List, Any, Optional
from kubernetes.client.rest import ApiException

from .constants import PIPELINE_ANALYSIS_CONFIG

# These functions will need K8s API clients and other function dependencies passed as parameters
# k8s_core_api, k8s_custom_api, and various analysis functions are expected to be available

# ============================================================================
# FAILURE CONTEXT IDENTIFICATION
# ============================================================================

async def identify_failure_context(
    failure_identifier: str,
    detect_tekton_namespaces_func,
    k8s_custom_api,
    k8s_core_api,
    logger
) -> Dict[str, Any]:
    """Identify the type and context of the failure."""
    try:
        # Try to find in different namespaces and types
        tekton_namespaces = await detect_tekton_namespaces_func()
        all_namespaces = []
        for category in tekton_namespaces.values():
            all_namespaces.extend(category)

        # Check if it's a pipeline run
        for namespace in all_namespaces:
            try:
                pipeline_run = k8s_custom_api.get_namespaced_custom_object(
                    group="tekton.dev", version="v1", namespace=namespace,
                    plural="pipelineruns", name=failure_identifier
                )
                return {"found": True, "type": "pipelinerun", "namespace": namespace, "object": pipeline_run}
            except ApiException:
                continue

        # Check if it's a pod
        for namespace in all_namespaces:
            try:
                pod = k8s_core_api.read_namespaced_pod(name=failure_identifier, namespace=namespace)
                return {"found": True, "type": "pod", "namespace": namespace, "object": pod}
            except ApiException:
                continue

        # Check if it's a task run
        for namespace in all_namespaces:
            try:
                task_run = k8s_custom_api.get_namespaced_custom_object(
                    group="tekton.dev", version="v1", namespace=namespace,
                    plural="taskruns", name=failure_identifier
                )
                return {"found": True, "type": "taskrun", "namespace": namespace, "object": task_run}
            except ApiException:
                continue

        return {"found": False, "type": "unknown", "namespace": None, "object": None}

    except Exception as e:
        logger.error(f"Error identifying failure context: {str(e)}")
        return {"found": False, "type": "error", "namespace": None, "object": None}

# ============================================================================
# SPECIFIC FAILURE ANALYSIS FUNCTIONS
# ============================================================================

async def analyze_pipeline_failure(
    namespace: str,
    pipeline_run: str,
    depth: str,
    analyze_failed_pipeline_func,
    analyze_pipeline_performance_func,
    get_pod_logs_func,
    analyze_logs_func,
    detect_log_anomalies_func,
    analyze_pipeline_dependencies_func,
    logger
) -> Dict[str, Any]:
    """Perform detailed analysis of a failed pipeline."""
    try:
        # Use existing pipeline analysis function
        basic_analysis = await analyze_failed_pipeline_func(namespace, pipeline_run)

        # Enhanced analysis based on depth
        enhanced_data = {
            "basic_analysis": basic_analysis,
            "logs_analyzed": {},
            "performance_data": {},
            "dependency_analysis": {}
        }

        if depth in ["standard", "deep"]:
            # Get performance context
            performance_data = await analyze_pipeline_performance_func(namespace, 50)
            enhanced_data["performance_data"] = performance_data

            # Analyze logs from all failed tasks
            for task in basic_analysis.get("failed_tasks", []):
                pod_name = task.get("pod", "unknown")
                if pod_name != "unknown":
                    pod_logs = await get_pod_logs_func(namespace, pod_name)

                    # Extract log content as string for analysis
                    log_content = extract_log_content_string(pod_logs)

                    log_analysis = await analyze_logs_func(log_content)
                    anomaly_analysis = await detect_log_anomalies_func(log_content)
                    enhanced_data["logs_analyzed"][pod_name] = {
                        "log_analysis": log_analysis,
                        "anomaly_analysis": anomaly_analysis
                    }

        if depth == "deep":
            # Deep dependency analysis
            enhanced_data["dependency_analysis"] = await analyze_pipeline_dependencies_func(namespace, pipeline_run)

        return enhanced_data

    except Exception as e:
        logger.error(f"Error analyzing pipeline failure: {str(e)}")
        return {"error": str(e), "logs_analyzed": {}}

async def analyze_pod_failure(
    namespace: str,
    pod_name: str,
    depth: str,
    k8s_core_api,
    get_pod_logs_func,
    analyze_logs_func,
    detect_log_anomalies_func,
    get_namespace_events_func,
    logger
) -> Dict[str, Any]:
    """Perform detailed analysis of a failed pod."""
    try:
        # Get pod details
        pod = k8s_core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
        pod_logs = await get_pod_logs_func(namespace, pod_name)

        analysis = {
            "pod_status": pod.status.phase,
            "container_statuses": [],
            "logs_analyzed": {},
            "events_analysis": {}
        }

        # Analyze container statuses
        if pod.status.container_statuses:
            for container_status in pod.status.container_statuses:
                status_info = {
                    "name": container_status.name,
                    "ready": container_status.ready,
                    "restart_count": container_status.restart_count,
                    "state": str(container_status.state)
                }
                analysis["container_statuses"].append(status_info)

        # Extract log content as string for analysis
        log_content = extract_log_content_string(pod_logs)

        # Analyze logs
        log_analysis = await analyze_logs_func(log_content)
        anomaly_analysis = await detect_log_anomalies_func(log_content)
        analysis["logs_analyzed"][pod_name] = {
            "log_analysis": log_analysis,
            "anomaly_analysis": anomaly_analysis
        }

        if depth in ["standard", "deep"]:
            # Get related events
            events = await get_namespace_events_func(namespace)
            analysis["events_analysis"] = events

        return analysis

    except Exception as e:
        logger.error(f"Error analyzing pod failure: {str(e)}")
        return {"error": str(e), "logs_analyzed": {}}

async def analyze_generic_failure(
    namespace: str,
    identifier: str,
    depth: str,
    get_namespace_events_func,
    logger
) -> Dict[str, Any]:
    """Perform generic failure analysis."""
    try:
        analysis = {
            "identifier": identifier,
            "namespace": namespace,
            "events_analysis": {},
            "logs_analyzed": {},
            "resource_analysis": {}
        }

        # Get namespace events
        events = await get_namespace_events_func(namespace)
        analysis["events_analysis"] = events

        return analysis

    except Exception as e:
        logger.error(f"Error in generic failure analysis: {str(e)}")
        return {"error": str(e), "logs_analyzed": {}}

# ============================================================================
# TIMELINE AND RELATED FAILURE ANALYSIS
# ============================================================================

async def build_failure_timeline(
    namespace: str,
    identifier: str,
    time_hours: int,
    get_namespace_events_func,
    logger
) -> List[Dict[str, str]]:
    """Build a detailed timeline of events leading to failure."""
    try:
        timeline = []

        # Get namespace events
        events_data = await get_namespace_events_func(namespace)

        # Convert events to timeline format
        if "events" in events_data:
            for event in events_data["events"][:20]:  # Limit to 20 most recent
                timeline.append({
                    "timestamp": datetime.now().isoformat(),  # Would extract from event
                    "event_type": "kubernetes_event",
                    "component": "cluster",
                    "description": str(event),
                    "severity": "medium"
                })

        # Sort by timestamp
        timeline.sort(key=lambda x: x["timestamp"], reverse=True)
        return timeline[:10]  # Return top 10 events

    except Exception as e:
        logger.error(f"Error building timeline: {str(e)}")
        return []

async def find_related_failures(
    namespace: str,
    identifier: str,
    time_hours: int,
    depth: str,
    list_pipelineruns_func,
    logger
) -> List[Dict[str, Any]]:
    """Find related failures in the time window."""
    try:
        related = []

        # Get recent pipeline runs
        pipeline_runs = await list_pipelineruns_func(namespace)

        # Find failed runs in time window
        for pr in pipeline_runs[:10]:  # Limit search
            if pr.get("status") != "Succeeded":
                related.append({
                    "incident_id": pr.get("name", "unknown"),
                    "similarity_score": 0.7,  # Would calculate based on error patterns
                    "resolution_applied": "Investigation needed"
                })

        return related[:5]  # Return top 5 related incidents

    except Exception as e:
        logger.error(f"Error finding related failures: {str(e)}")
        return []

# ============================================================================
# ADVANCED ROOT CAUSE ANALYSIS
# ============================================================================

async def perform_advanced_rca(
    primary_analysis: Dict,
    timeline: List,
    related: List,
    depth: str,
    categorize_errors_func,
    logger
) -> Dict[str, Any]:
    """Perform advanced root cause analysis using correlation algorithms."""
    try:
        # Extract error patterns from primary analysis
        all_errors = []
        for log_data in primary_analysis.get("logs_analyzed", {}).values():
            if "log_analysis" in log_data:
                all_errors.extend(log_data["log_analysis"].get("error_patterns", []))

        # Categorize errors
        error_text = " ".join(all_errors)
        categories = categorize_errors_func(error_text, all_errors)

        # Determine primary cause
        primary_cause = {}
        if categories:
            top_category = max(categories.items(), key=lambda x: x[1])
            primary_cause = {
                "category": top_category[0],
                "confidence": min(0.9, top_category[1] / 10.0),
                "description": get_category_description(top_category[0]),
                "evidence": all_errors[:3]
            }

        # Find contributing factors
        contributing_factors = []
        for category, count in categories.items():
            if category != primary_cause.get("category") and count > 0:
                contributing_factors.append({
                    "factor": category,
                    "impact_level": "high" if count > 3 else "medium",
                    "description": get_category_description(category)
                })

        # Identify affected systems
        affected_systems = ["tekton-pipelines", "kubernetes-cluster"]
        if "network" in categories:
            affected_systems.append("cluster-networking")
        if "resource_limits" in categories:
            affected_systems.append("resource-management")

        return {
            "root_cause_analysis": {
                "primary_cause": primary_cause,
                "contributing_factors": contributing_factors,
                "affected_systems": affected_systems
            },
            "dependency_failures": []
        }

    except Exception as e:
        logger.error(f"Error in advanced RCA: {str(e)}")
        return {
            "root_cause_analysis": {
                "primary_cause": {"error": str(e)},
                "contributing_factors": [],
                "affected_systems": []
            },
            "dependency_failures": []
        }

# ============================================================================
# RESOURCE AND CONFIGURATION ANALYSIS
# ============================================================================

async def analyze_resource_constraints(
    namespace: str,
    identifier: str,
    k8s_core_api,
    logger
) -> Dict[str, Any]:
    """Analyze resource constraints and usage patterns."""
    try:
        # Get namespace resource quotas
        try:
            quotas = k8s_core_api.list_namespaced_resource_quota(namespace=namespace)
            quota_info = []
            for quota in quotas.items:
                quota_info.append({
                    "name": quota.metadata.name,
                    "used": dict(quota.status.used) if quota.status.used else {},
                    "hard": dict(quota.status.hard) if quota.status.hard else {}
                })
        except ApiException:
            quota_info = []

        return {
            "resource_quotas": quota_info,
            "memory_pressure": False,  # Would calculate from metrics
            "cpu_pressure": False,
            "storage_issues": False
        }

    except Exception as e:
        logger.error(f"Error analyzing resource constraints: {str(e)}")
        return {}

async def analyze_configuration_issues(
    namespace: str,
    identifier: str,
    logger
) -> List[Dict[str, str]]:
    """Analyze configuration issues."""
    try:
        issues = []

        # Check for common configuration problems
        # This would involve checking ConfigMaps, Secrets, RBAC, etc.

        return issues

    except Exception as e:
        logger.error(f"Error analyzing configuration: {str(e)}")
        return []

async def analyze_pipeline_dependencies(
    namespace: str,
    pipeline_run: str,
    logger
) -> Dict[str, Any]:
    """Analyze pipeline dependencies for deep analysis."""
    try:
        return {
            "external_dependencies": [],
            "internal_dependencies": [],
            "version_conflicts": []
        }

    except Exception as e:
        logger.error(f"Error analyzing dependencies: {str(e)}")
        return {}

# ============================================================================
# REMEDIATION PLANNING
# ============================================================================

async def generate_remediation_plan(
    root_cause_data: Dict,
    primary_analysis: Dict,
    resource_analysis: Dict,
    config_analysis: List,
    recommend_actions_func,
    logger
) -> Dict[str, List[str]]:
    """Generate specific remediation recommendations."""
    try:
        immediate_actions = []
        preventive_measures = []

        # Use existing recommendation logic
        analysis_for_recommendations = {
            "probable_root_cause": root_cause_data["root_cause_analysis"]["primary_cause"].get("description", "Unknown"),
            "failed_tasks": primary_analysis.get("basic_analysis", {}).get("failed_tasks", [])
        }

        existing_recommendations = recommend_actions_func(analysis_for_recommendations)
        immediate_actions.extend(existing_recommendations[:5])

        # Add preventive measures based on root cause
        primary_cause = root_cause_data["root_cause_analysis"]["primary_cause"]
        if primary_cause.get("category") == "resource_limits":
            preventive_measures.extend([
                "Implement resource monitoring and alerting",
                "Review and adjust resource requests/limits",
                "Set up horizontal pod autoscaling where appropriate"
            ])
        elif primary_cause.get("category") == "network":
            preventive_measures.extend([
                "Implement network connectivity monitoring",
                "Review and test network policies regularly",
                "Set up dependency health checks"
            ])

        return {
            "immediate_actions": immediate_actions,
            "preventive_measures": preventive_measures
        }

    except Exception as e:
        logger.error(f"Error generating remediation plan: {str(e)}")
        return {"immediate_actions": [], "preventive_measures": []}

# ============================================================================
# CONFIDENCE AND SCORING FUNCTIONS
# ============================================================================

def calculate_confidence_score(primary_analysis: Dict, root_cause_data: Dict, timeline: List) -> float:
    """Calculate confidence score for the RCA."""
    try:
        base_score = 0.5

        # Increase confidence based on available data
        if primary_analysis.get("logs_analyzed"):
            base_score += 0.2

        if root_cause_data["root_cause_analysis"]["primary_cause"]:
            base_score += 0.2

        if timeline:
            base_score += 0.1

        return min(1.0, base_score)

    except Exception:
        return 0.3  # Low confidence fallback

def calculate_failure_impact_score(primary_analysis: Dict, timeline: List, related: List) -> Dict[str, Any]:
    """Calculate the impact score of the failure."""

    # Base impact from failure type
    impact_score = 5.0  # Medium baseline

    # Increase based on number of affected components
    affected_tasks = len(primary_analysis.get("basic_analysis", {}).get("failed_tasks", []))
    impact_score += affected_tasks * 0.5

    # Increase based on related failures
    related_count = len(related)
    impact_score += related_count * 1.0

    # Timeline density (more events = higher impact)
    timeline_count = len(timeline)
    impact_score += timeline_count * 0.2

    # Normalize to 1-10 scale
    impact_score = min(10.0, max(1.0, impact_score))

    # Determine impact level
    if impact_score >= 8.0:
        impact_level = "CRITICAL"
    elif impact_score >= 6.0:
        impact_level = "HIGH"
    elif impact_score >= 4.0:
        impact_level = "MEDIUM"
    else:
        impact_level = "LOW"

    return {
        "impact_score": round(impact_score, 1),
        "impact_level": impact_level,
        "affected_components": affected_tasks,
        "related_incidents": related_count,
        "timeline_density": timeline_count
    }

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def extract_log_content_string(pod_logs: Any) -> str:
    """Extract log content as string for analysis."""
    if isinstance(pod_logs, dict) and "logs" in pod_logs:
        log_content = ""
        for pod, logs in pod_logs["logs"].items():
            if isinstance(logs, list):
                log_content += "\n".join(logs)
            else:
                log_content += str(logs)
    else:
        log_content = str(pod_logs) if pod_logs else "No pod logs available"

    return log_content

def get_category_description(category: str) -> str:
    """Get human-readable description for error categories."""

    descriptions = {
        "resource_limits": "Resource constraints (CPU, memory, or storage limits exceeded)",
        "network": "Network connectivity or DNS resolution issues",
        "authentication": "Authentication or authorization failures",
        "configuration": "Configuration errors or missing settings",
        "dependency": "External dependency or service unavailability",
        "timeout": "Operation timeouts or deadline exceeded",
        "permission": "Permission denied or access control issues",
        "image": "Container image pull or registry issues",
        "volume": "Volume mount or storage access problems",
        "application": "Application-specific errors or bugs",
        "unknown": "Unspecified or unclassified errors"
    }

    return descriptions.get(category, f"Issues related to {category}")

def analyze_error_patterns(errors: List[str]) -> Dict[str, Any]:
    """Analyze patterns in error messages."""

    if not errors:
        return {"patterns": [], "frequency": {}, "severity": "UNKNOWN"}

    # Count error types
    error_counts = {}
    severity_indicators = {
        "FATAL": ["fatal", "panic", "crash", "abort"],
        "ERROR": ["error", "failed", "failure", "exception"],
        "WARNING": ["warning", "warn", "deprecated"],
        "INFO": ["info", "notice", "debug"]
    }

    for error in errors:
        error_lower = error.lower()

        # Categorize by content
        if "memory" in error_lower or "oom" in error_lower:
            error_counts["memory"] = error_counts.get("memory", 0) + 1
        elif "network" in error_lower or "dns" in error_lower:
            error_counts["network"] = error_counts.get("network", 0) + 1
        elif "timeout" in error_lower:
            error_counts["timeout"] = error_counts.get("timeout", 0) + 1
        elif "permission" in error_lower or "denied" in error_lower:
            error_counts["permission"] = error_counts.get("permission", 0) + 1
        else:
            error_counts["general"] = error_counts.get("general", 0) + 1

    # Determine overall severity
    overall_severity = "INFO"
    for severity, indicators in severity_indicators.items():
        if any(indicator in " ".join(errors).lower() for indicator in indicators):
            overall_severity = severity
            break

    # Extract common patterns
    patterns = []
    if error_counts.get("memory", 0) > 1:
        patterns.append("Recurring memory issues detected")
    if error_counts.get("network", 0) > 1:
        patterns.append("Multiple network-related failures")
    if error_counts.get("timeout", 0) > 1:
        patterns.append("Timeout pattern indicating latency issues")

    return {
        "patterns": patterns,
        "frequency": error_counts,
        "severity": overall_severity,
        "total_errors": len(errors)
    }

# ============================================================================
# FAILURE TREND ANALYSIS
# ============================================================================

def analyze_failure_trends(related_failures: List[Dict[str, Any]], timeline: List[Dict[str, str]]) -> Dict[str, Any]:
    """Analyze trends in failure patterns."""

    trends = {
        "failure_frequency": "stable",
        "escalation_pattern": "none",
        "recurring_issues": [],
        "trend_analysis": {}
    }

    if not related_failures and not timeline:
        return trends

    # Analyze failure frequency
    total_incidents = len(related_failures)
    if total_incidents > 5:
        trends["failure_frequency"] = "high"
    elif total_incidents > 2:
        trends["failure_frequency"] = "increasing"

    # Look for escalation patterns
    timeline_events = len(timeline)
    if timeline_events > 10:
        trends["escalation_pattern"] = "rapid_escalation"
    elif timeline_events > 5:
        trends["escalation_pattern"] = "gradual_escalation"

    # Identify recurring issues
    if related_failures:
        # Simple pattern matching for recurring issue detection
        issue_patterns = {}
        for failure in related_failures:
            incident_id = failure.get("incident_id", "")
            # Extract pattern (simplified)
            if "build" in incident_id.lower():
                issue_patterns["build_failures"] = issue_patterns.get("build_failures", 0) + 1
            elif "test" in incident_id.lower():
                issue_patterns["test_failures"] = issue_patterns.get("test_failures", 0) + 1

        for pattern, count in issue_patterns.items():
            if count > 1:
                trends["recurring_issues"].append(f"{pattern}: {count} occurrences")

    trends["trend_analysis"] = {
        "total_related_incidents": total_incidents,
        "timeline_events": timeline_events,
        "analysis_confidence": calculate_trend_confidence(total_incidents, timeline_events)
    }

    return trends

def calculate_trend_confidence(incidents: int, events: int) -> float:
    """Calculate confidence in trend analysis."""

    # More data = higher confidence
    data_points = incidents + events

    if data_points >= 15:
        return 0.9
    elif data_points >= 10:
        return 0.7
    elif data_points >= 5:
        return 0.5
    else:
        return 0.3

# ============================================================================
# FAILURE SEVERITY ASSESSMENT
# ============================================================================

def assess_failure_severity(
    primary_analysis: Dict,
    root_cause_data: Dict,
    resource_analysis: Dict,
    config_analysis: List
) -> Dict[str, Any]:
    """Assess the overall severity of the failure."""

    severity_score = 0
    severity_factors = []

    # Analyze primary failure impact
    failed_tasks = len(primary_analysis.get("basic_analysis", {}).get("failed_tasks", []))
    if failed_tasks > 3:
        severity_score += 3
        severity_factors.append(f"Multiple task failures ({failed_tasks})")
    elif failed_tasks > 1:
        severity_score += 2
        severity_factors.append("Multiple component failures")

    # Root cause severity
    primary_cause = root_cause_data.get("root_cause_analysis", {}).get("primary_cause", {})
    cause_category = primary_cause.get("category", "")

    if cause_category in ["resource_limits", "network"]:
        severity_score += 3
        severity_factors.append(f"Critical system issue: {cause_category}")
    elif cause_category in ["configuration", "permission"]:
        severity_score += 2
        severity_factors.append(f"Configuration issue: {cause_category}")

    # Resource constraints impact
    if resource_analysis.get("memory_pressure") or resource_analysis.get("cpu_pressure"):
        severity_score += 2
        severity_factors.append("Resource pressure detected")

    # Configuration issues
    if config_analysis:
        severity_score += 1
        severity_factors.append("Configuration issues present")

    # Determine severity level
    if severity_score >= 7:
        severity_level = "CRITICAL"
        priority = "P1"
    elif severity_score >= 5:
        severity_level = "HIGH"
        priority = "P2"
    elif severity_score >= 3:
        severity_level = "MEDIUM"
        priority = "P3"
    else:
        severity_level = "LOW"
        priority = "P4"

    return {
        "severity_level": severity_level,
        "severity_score": severity_score,
        "priority": priority,
        "severity_factors": severity_factors,
        "recommended_response_time": get_response_time_recommendation(severity_level)
    }

def get_response_time_recommendation(severity_level: str) -> str:
    """Get recommended response time based on severity."""

    response_times = {
        "CRITICAL": "Immediate (within 15 minutes)",
        "HIGH": "Urgent (within 1 hour)",
        "MEDIUM": "Standard (within 4 hours)",
        "LOW": "Normal (within 24 hours)"
    }

    return response_times.get(severity_level, "Normal (within 24 hours)")


# ============================================================================
# SIMULATION IMPACT ANALYSIS FUNCTIONS
# ============================================================================


def categorize_impact_severity(impact_value: float) -> str:
    """Categorize impact severity based on numeric value."""
    abs_impact = abs(impact_value)
    if abs_impact < 0.05:
        return "minimal"
    elif abs_impact < 0.15:
        return "low"
    elif abs_impact < 0.3:
        return "medium"
    elif abs_impact < 0.5:
        return "high"
    else:
        return "critical"


def generate_performance_impact_description(impact: float, scenario_type: str) -> str:
    """Generate human-readable description of performance impact."""
    if impact > 0:
        direction = "degradation"
        severity = "slower response times and reduced throughput"
    else:
        direction = "improvement"
        severity = "faster response times and increased throughput"
        impact = abs(impact)

    magnitude = categorize_impact_severity(impact)
    return f"{magnitude.title()} performance {direction} expected from {scenario_type.replace('_', ' ')} changes, with {severity}"


def generate_reliability_impact_description(impact: float, scenario_type: str) -> str:
    """Generate human-readable description of reliability impact."""
    if impact > 0:
        direction = "decrease"
        effects = "increased error rates and potential service disruptions"
    else:
        direction = "improvement"
        effects = "better fault tolerance and stability"
        impact = abs(impact)

    magnitude = categorize_impact_severity(impact)
    return f"{magnitude.title()} reliability {direction} anticipated from {scenario_type.replace('_', ' ')} changes, with {effects}"


def generate_cost_impact_description(impact: float, scenario_type: str) -> str:
    """Generate human-readable description of cost impact."""
    if impact > 0:
        direction = "increase"
        effects = "higher resource consumption and operational costs"
    else:
        direction = "reduction"
        effects = "lower resource usage and cost savings"
        impact = abs(impact)

    magnitude = categorize_impact_severity(impact)
    return f"{magnitude.title()} cost {direction} projected from {scenario_type.replace('_', ' ')} changes, with {effects}"


def analyze_system_impact(
    simulation_results: Dict[str, Any],
    baseline_data: Dict[str, Any],
    scenario_type: str
) -> Dict[str, Any]:
    """Analyze the simulated impact on different system aspects."""
    try:
        impact_analysis = {}

        # Analyze performance impact
        perf_stats = simulation_results.get("performance_impact", {})
        if perf_stats:
            impact_analysis["performance_impact"] = {
                "expected_change": f"{perf_stats.get('mean', 0):.1%}",
                "worst_case": f"{perf_stats.get('max', 0):.1%}",
                "best_case": f"{perf_stats.get('min', 0):.1%}",
                "confidence_interval": f"{perf_stats.get('p5', 0):.1%} to {perf_stats.get('p95', 0):.1%}",
                "severity": categorize_impact_severity(perf_stats.get('mean', 0)),
                "description": generate_performance_impact_description(perf_stats.get('mean', 0), scenario_type)
            }

        # Analyze reliability impact
        rel_stats = simulation_results.get("reliability_impact", {})
        if rel_stats:
            impact_analysis["reliability_impact"] = {
                "expected_change": f"{rel_stats.get('mean', 0):.1%}",
                "worst_case": f"{rel_stats.get('max', 0):.1%}",
                "best_case": f"{rel_stats.get('min', 0):.1%}",
                "confidence_interval": f"{rel_stats.get('p5', 0):.1%} to {rel_stats.get('p95', 0):.1%}",
                "severity": categorize_impact_severity(rel_stats.get('mean', 0)),
                "description": generate_reliability_impact_description(rel_stats.get('mean', 0), scenario_type)
            }

        # Analyze cost impact
        cost_stats = simulation_results.get("cost_impact", {})
        if cost_stats:
            impact_analysis["cost_impact"] = {
                "expected_change": f"{cost_stats.get('mean', 0):.1%}",
                "worst_case": f"{cost_stats.get('max', 0):.1%}",
                "best_case": f"{cost_stats.get('min', 0):.1%}",
                "confidence_interval": f"{cost_stats.get('p5', 0):.1%} to {cost_stats.get('p95', 0):.1%}",
                "severity": categorize_impact_severity(abs(cost_stats.get('mean', 0))),
                "description": generate_cost_impact_description(cost_stats.get('mean', 0), scenario_type)
            }

        return impact_analysis

    except Exception as e:
        return {"error": str(e)}


def perform_risk_assessment(
    simulation_results: Dict[str, Any],
    impact_analysis: Dict[str, Any],
    affected_components: List[Dict[str, Any]],
    risk_tolerance: str
) -> Dict[str, Any]:
    """Perform comprehensive risk assessment of the proposed changes."""
    try:
        risk_factors = []
        risk_scores = []

        # Assess performance risk
        perf_impact = impact_analysis.get("performance_impact", {})
        perf_mean = 0
        if perf_impact:
            perf_mean = simulation_results.get("performance_impact", {}).get("mean", 0)
            if abs(perf_mean) > 0.2:
                risk_factors.append(f"Significant performance impact: {perf_impact.get('expected_change', 'unknown')}")
                risk_scores.append(min(100, abs(perf_mean) * 100))

        # Assess reliability risk
        rel_impact = impact_analysis.get("reliability_impact", {})
        rel_mean = 0
        if rel_impact:
            rel_mean = simulation_results.get("reliability_impact", {}).get("mean", 0)
            if abs(rel_mean) > 0.1:
                risk_factors.append(f"Reliability impact: {rel_impact.get('expected_change', 'unknown')}")
                risk_scores.append(min(100, abs(rel_mean) * 150))

        # Assess component risk
        critical_components = 0
        for component in affected_components:
            severity = component.get("severity", "LOW")
            if severity in ["CRITICAL", "HIGH", "critical", "high"]:
                critical_components += 1
                risk_factors.append(f"Critical component affected: {component.get('component', 'unknown')}")
                risk_scores.append(75 if severity.lower() == "high" else 100)

        # Calculate overall risk score
        if risk_scores:
            avg_risk = sum(risk_scores) / len(risk_scores)
            max_risk = max(risk_scores)
            overall_risk_score = (avg_risk * 0.7) + (max_risk * 0.3)
        else:
            overall_risk_score = 0

        # Determine risk level based on tolerance and score
        risk_thresholds = {
            "conservative": {"high": 30, "medium": 15},
            "moderate": {"high": 50, "medium": 25},
            "aggressive": {"high": 70, "medium": 40}
        }

        thresholds = risk_thresholds.get(risk_tolerance.lower(), risk_thresholds["moderate"])

        if overall_risk_score >= thresholds["high"]:
            overall_risk = "HIGH"
        elif overall_risk_score >= thresholds["medium"]:
            overall_risk = "MEDIUM"
        else:
            overall_risk = "LOW"

        # Rollback complexity assessment
        rollback_factors = []
        if critical_components > 2:
            rollback_factors.append("Multiple critical components involved")
        if perf_impact and abs(perf_mean) > 0.3:
            rollback_factors.append("Significant performance changes")

        rollback_complexity = "HIGH" if len(rollback_factors) > 1 else "MEDIUM" if rollback_factors else "LOW"

        # Generate testing recommendations
        testing_recommendations = []
        if overall_risk in ["HIGH", "MEDIUM"]:
            testing_recommendations.append("Conduct staged rollout with monitoring")
            testing_recommendations.append("Implement comprehensive health checks")
        if critical_components > 0:
            testing_recommendations.append("Perform component-specific integration tests")
        if not testing_recommendations:
            testing_recommendations.append("Standard testing procedures sufficient")

        return {
            "risk_score": round(overall_risk_score, 2),
            "overall_risk": overall_risk,
            "risk_factors": risk_factors,
            "affected_critical_components": critical_components,
            "rollback_complexity": rollback_complexity,
            "testing_recommendations": testing_recommendations,
            "risk_breakdown": {
                "performance_risk": perf_mean,
                "reliability_risk": rel_mean,
                "component_risk": critical_components
            }
        }

    except Exception as e:
        return {
            "risk_score": 100,
            "overall_risk": "unknown",
            "risk_factors": [f"Risk assessment error: {str(e)}"],
            "rollback_complexity": "unknown",
            "testing_recommendations": ["Perform manual risk assessment due to simulation error"]
        }


def calculate_simulation_quality(
    baseline_data: Dict[str, Any],
    historical_data: Dict[str, Any],
    models: Dict[str, Any],
    logger=None
) -> Dict[str, Any]:
    """Calculate quality metrics for the simulation."""
    try:
        # Model accuracy based on available data
        data_points = len(historical_data.get("cpu_utilization", []))
        if data_points >= 168:
            model_accuracy = 0.9
        elif data_points >= 24:
            model_accuracy = 0.75
        elif data_points >= 1:
            model_accuracy = 0.5
        else:
            model_accuracy = 0.3

        # Data completeness based on baseline collection
        namespaces_analyzed = len(baseline_data.get("resource_usage", {}))
        nodes_analyzed = len(baseline_data.get("cluster_nodes", []))

        data_completeness = min(1.0, (namespaces_analyzed * 0.1 + nodes_analyzed * 0.05))

        # Identify assumptions and limitations
        assumptions = [
            "Linear scaling relationships assumed for resource consumption",
            "Historical patterns representative of future behavior",
            "No external dependencies or constraints considered"
        ]

        limitations = [
            f"Simulation based on {data_points} historical data points",
            f"Analysis limited to {namespaces_analyzed} namespaces",
            "Monte Carlo simulation uses simplified models"
        ]

        if model_accuracy < 0.7:
            limitations.append("Limited historical data may reduce prediction accuracy")

        if data_completeness < 0.5:
            limitations.append("Incomplete baseline data may affect simulation quality")

        return {
            "model_accuracy": round(model_accuracy, 2),
            "data_completeness": round(data_completeness, 2),
            "assumptions": assumptions,
            "limitations": limitations,
            "overall_quality": round((model_accuracy + data_completeness) / 2, 2)
        }

    except Exception as e:
        if logger:
            logger.error(f"Error calculating simulation quality: {e}")
        return {
            "model_accuracy": 0.0,
            "data_completeness": 0.0,
            "assumptions": ["Simulation quality calculation failed"],
            "limitations": [f"Quality assessment error: {str(e)}"],
            "overall_quality": 0.0
        }


def generate_simulation_recommendations(
    impact_analysis: Dict[str, Any],
    risk_assessment: Dict[str, Any],
    simulation_quality: Dict[str, Any],
    scenario_type: str,
    logger=None
) -> Dict[str, Any]:
    """Generate actionable recommendations based on simulation results."""
    try:
        overall_quality = simulation_quality.get("overall_quality", 0.0)
        overall_risk = risk_assessment.get("overall_risk", "unknown")

        # Determine if we should proceed
        proceed = True
        conditions = []

        if overall_risk in ["critical", "high", "CRITICAL", "HIGH"]:
            proceed = False
            conditions.append("Reduce risk factors before proceeding")
            conditions.append("Implement additional safeguards and monitoring")

        if overall_quality < 0.5:
            conditions.append("Gather more historical data for better predictions")
            conditions.append("Validate simulation with smaller-scale testing")

        # Generate alternative approaches
        alternative_approaches = []

        if scenario_type == "scaling":
            alternative_approaches.extend([
                "Implement gradual scaling instead of immediate changes",
                "Use horizontal pod autoscaling with conservative thresholds",
                "Consider blue-green deployment for scaling changes"
            ])
        elif scenario_type == "resource_limits":
            alternative_approaches.extend([
                "Implement changes during low-traffic periods",
                "Use resource quota increases instead of direct limit changes",
                "Deploy changes to staging environment first"
            ])
        elif scenario_type == "configuration":
            alternative_approaches.extend([
                "Use canary deployments for configuration changes",
                "Implement feature flags to control configuration rollout",
                "Validate configurations in non-production environments"
            ])
        elif scenario_type == "deployment":
            alternative_approaches.extend([
                "Use rolling deployments with smaller batch sizes",
                "Implement blue-green deployment strategy",
                "Schedule deployment during maintenance windows"
            ])

        # Generate monitoring requirements
        monitoring_requirements = [
            "Monitor key performance metrics during and after changes",
            "Set up alerts for resource utilization thresholds",
            "Track error rates and response times continuously"
        ]

        # Add specific monitoring based on risk factors
        risk_factors = risk_assessment.get("risk_factors", [])
        for factor in risk_factors:
            if "performance" in factor.lower():
                monitoring_requirements.append("Implement detailed application performance monitoring")
            elif "reliability" in factor.lower():
                monitoring_requirements.append("Set up comprehensive health checks and SLA monitoring")
            elif "component" in factor.lower():
                monitoring_requirements.append("Monitor individual component health and dependencies")

        return {
            "proceed": proceed,
            "conditions": conditions,
            "alternative_approaches": alternative_approaches,
            "monitoring_requirements": monitoring_requirements,
            "quality_score": overall_quality,
            "risk_level": overall_risk
        }

    except Exception as e:
        if logger:
            logger.error(f"Error generating recommendations: {e}")
        return {
            "proceed": False,
            "conditions": ["Manual review required due to simulation error"],
            "alternative_approaches": ["Conduct manual impact analysis"],
            "monitoring_requirements": ["Implement comprehensive monitoring"],
            "error": str(e)
        }
