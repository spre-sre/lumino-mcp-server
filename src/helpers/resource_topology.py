# ============================================================================
# RESOURCE TOPOLOGY HELPER MODULE
# ============================================================================
#
# This module contains all resource topology related classes, functions, and utilities
# used by the MCP server for dependency analysis, topology mapping, multi-cluster
# coordination, and artifact tracking.
# ============================================================================

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


# ============================================================================
# MULTI-CLUSTER CLIENT MANAGEMENT
# ============================================================================

async def get_multi_cluster_clients(k8s_core_api, k8s_custom_api, k8s_apps_api) -> Dict[str, Dict[str, Any]]:
    """Get authenticated clients for multiple clusters."""
    # For now, return the current cluster - extend this for actual multi-cluster setups
    return {
        "current": {
            "core_api": k8s_core_api,
            "custom_api": k8s_custom_api,
            "apps_api": k8s_apps_api
        }
    }


# ============================================================================
# PIPELINE CORRELATION AND TRACKING
# ============================================================================

async def correlate_pipeline_events(
    trace_identifier: str,
    trace_type: str,
    cluster_clients: Dict[str, Dict[str, Any]],
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    logger=None
) -> List[Dict[str, Any]]:
    """Correlate pipeline runs across clusters using labels, annotations, and artifact references."""
    pipeline_flow = []

    for cluster_name, clients in cluster_clients.items():
        try:
            custom_api = clients["custom_api"]

            # Get all namespaces in the cluster
            namespaces = []
            try:
                ns_list = clients["core_api"].list_namespace()
                namespaces = [ns.metadata.name for ns in ns_list.items]
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to list namespaces in cluster {cluster_name}: {e}")
                continue

            for namespace in namespaces:
                try:
                    # Query PipelineRuns in each namespace
                    pipeline_runs = custom_api.list_namespaced_custom_object(
                        group="tekton.dev",
                        version="v1beta1",
                        namespace=namespace,
                        plural="pipelineruns"
                    )

                    for pr in pipeline_runs.get("items", []):
                        if matches_trace_identifier(pr, trace_identifier, trace_type):
                            pipeline_info = {
                                "cluster": cluster_name,
                                "namespace": namespace,
                                "pipeline_name": pr.get("metadata", {}).get("name", "unknown"),
                                "pipeline_run_name": pr.get("metadata", {}).get("name", "unknown"),
                                "status": get_pipeline_status(pr),
                                "start_time": pr.get("status", {}).get("startTime"),
                                "completion_time": pr.get("status", {}).get("completionTime"),
                                "tasks": extract_task_info(pr),
                                "labels": pr.get("metadata", {}).get("labels", {}),
                                "annotations": pr.get("metadata", {}).get("annotations", {})
                            }

                            # Filter by time range if specified
                            if in_time_range(pipeline_info, start_time, end_time):
                                pipeline_flow.append(pipeline_info)

                except Exception as e:
                    if logger:
                        logger.debug(f"Failed to query PipelineRuns in {cluster_name}/{namespace}: {e}")
                    continue

        except Exception as e:
            if logger:
                logger.error(f"Failed to query cluster {cluster_name}: {e}")
            continue

    return pipeline_flow


def matches_trace_identifier(pipeline_run: Dict[str, Any], trace_identifier: str, trace_type: str) -> bool:
    """Check if a pipeline run matches the trace identifier."""
    metadata = pipeline_run.get("metadata", {})
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})

    if trace_type == "commit":
        # Look for commit SHA in labels and annotations
        commit_keys = ["git.commit", "tekton.dev/git-commit", "pipelinesascode.tekton.dev/sha"]
        for key in commit_keys:
            if labels.get(key, "").startswith(trace_identifier) or annotations.get(key, "").startswith(trace_identifier):
                return True
        # Also check in all values
        return any(trace_identifier in str(v) for v in labels.values()) or \
               any(trace_identifier in str(v) for v in annotations.values())

    elif trace_type == "pr":
        # Look for PR number in labels and annotations
        pr_keys = ["pipelinesascode.tekton.dev/pull-request", "pull-request", "pr"]
        for key in pr_keys:
            if labels.get(key) == trace_identifier or annotations.get(key) == trace_identifier:
                return True
        return False

    elif trace_type == "image":
        # Look for image reference in labels, annotations, or results
        return any(trace_identifier in str(v) for v in labels.values()) or \
               any(trace_identifier in str(v) for v in annotations.values())

    elif trace_type == "custom":
        # Search across all labels and annotations
        name = metadata.get("name", "")
        return trace_identifier in name or \
               any(trace_identifier in str(v) for v in labels.values()) or \
               any(trace_identifier in str(v) for v in annotations.values())

    return False


def get_pipeline_status(pipeline_run: Dict[str, Any]) -> str:
    """Extract pipeline status from PipelineRun."""
    conditions = pipeline_run.get("status", {}).get("conditions", [])
    if conditions:
        latest_condition = conditions[-1]
        if latest_condition.get("status") == "True":
            return latest_condition.get("reason", "Unknown")
        else:
            return latest_condition.get("reason", "Failed")
    return "Unknown"


def extract_task_info(pipeline_run: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract task information from PipelineRun status."""
    tasks = []
    task_runs = pipeline_run.get("status", {}).get("taskRuns", {})

    for task_run_name, task_run_status in task_runs.items():
        task_info = {
            "name": task_run_name,
            "status": task_run_status.get("status", {}).get("conditions", [{}])[-1].get("reason", "Unknown"),
            "start_time": task_run_status.get("status", {}).get("startTime"),
            "completion_time": task_run_status.get("status", {}).get("completionTime")
        }
        tasks.append(task_info)

    return tasks


def in_time_range(pipeline_info: Dict[str, Any], start_time: Optional[str], end_time: Optional[str]) -> bool:
    """Check if pipeline execution falls within the specified time range."""
    if not start_time and not end_time:
        return True

    pipeline_start = pipeline_info.get("start_time")
    if not pipeline_start:
        return True

    try:
        pipeline_dt = datetime.fromisoformat(pipeline_start.replace('Z', '+00:00'))

        if start_time:
            start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            if pipeline_dt < start_dt:
                return False

        if end_time:
            end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            if pipeline_dt > end_dt:
                return False

        return True
    except Exception:
        return True


# ============================================================================
# ARTIFACT TRACKING AND ANALYSIS
# ============================================================================

async def track_artifacts(pipeline_flow: List[Dict[str, Any]],
                          include_artifacts: bool = True,
                          logger=None) -> List[Dict[str, Any]]:
    """Track artifacts through container registries and pipeline results."""
    if not include_artifacts:
        return []

    artifacts = []
    seen_artifacts = set()

    for pipeline in pipeline_flow:
        try:
            # Extract artifacts from pipeline results and parameters
            pipeline_artifacts = extract_pipeline_artifacts(pipeline)

            for artifact in pipeline_artifacts:
                artifact_id = artifact.get("artifact_id", "")
                if artifact_id and artifact_id not in seen_artifacts:
                    artifacts.append(artifact)
                    seen_artifacts.add(artifact_id)

        except Exception as e:
            if logger:
                logger.debug(f"Failed to track artifacts for pipeline {pipeline.get('pipeline_name', '')}: {e}")
            continue

    return artifacts


def extract_pipeline_artifacts(pipeline: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract artifact information from pipeline metadata."""
    artifacts = []

    # Look for image references in labels and annotations
    labels = pipeline.get("labels", {})
    annotations = pipeline.get("annotations", {})

    # Common patterns for artifact IDs
    potential_images = []

    # Check for common image label patterns
    for key, value in labels.items():
        if any(keyword in key.lower() for keyword in ["image", "container", "artifact"]):
            potential_images.append(value)

    for key, value in annotations.items():
        if any(keyword in key.lower() for keyword in ["image", "container", "artifact"]):
            potential_images.append(value)

    for image in potential_images:
        if image and ":" in image:  # Basic image validation
            artifacts.append({
                "artifact_id": image,
                "type": "container_image",
                "registry": image.split("/")[0] if "/" in image else "unknown",
                "propagation_path": [
                    {
                        "cluster": pipeline["cluster"],
                        "namespace": pipeline["namespace"],
                        "pipeline": pipeline["pipeline_name"],
                        "timestamp": pipeline.get("start_time", "")
                    }
                ]
            })

    return artifacts


# ============================================================================
# PERFORMANCE ANALYSIS AND BOTTLENECK DETECTION
# ============================================================================

def analyze_bottlenecks(pipeline_flow: List[Dict[str, Any]], logger=None) -> List[Dict[str, Any]]:
    """Analyze pipeline flow for bottlenecks and performance issues."""
    bottlenecks = []

    try:
        for i, pipeline in enumerate(pipeline_flow):
            start_time = pipeline.get("start_time")
            completion_time = pipeline.get("completion_time")

            if start_time and completion_time:
                try:
                    start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(completion_time.replace('Z', '+00:00'))
                    duration = (end_dt - start_dt).total_seconds()

                    # Flag pipelines that take unusually long (> 30 minutes)
                    if duration > 1800:
                        bottlenecks.append({
                            "location": f"{pipeline['cluster']}/{pipeline['namespace']}/{pipeline['pipeline_name']}",
                            "type": "long_duration",
                            "duration": duration,
                            "description": f"Pipeline execution took {duration/60:.1f} minutes"
                        })

                    # Check task-level bottlenecks
                    for task in pipeline.get("tasks", []):
                        task_start = task.get("start_time")
                        task_end = task.get("completion_time")

                        if task_start and task_end:
                            try:
                                task_start_dt = datetime.fromisoformat(task_start.replace('Z', '+00:00'))
                                task_end_dt = datetime.fromisoformat(task_end.replace('Z', '+00:00'))
                                task_duration = (task_end_dt - task_start_dt).total_seconds()

                                # Flag tasks that take more than 15 minutes
                                if task_duration > 900:
                                    bottlenecks.append({
                                        "location": f"{pipeline['cluster']}/{pipeline['namespace']}/{task['name']}",
                                        "type": "slow_task",
                                        "duration": task_duration,
                                        "description": f"Task '{task['name']}' took {task_duration/60:.1f} minutes"
                                    })
                            except Exception:
                                continue

                except Exception:
                    continue

        # Look for patterns across the flow
        if len(pipeline_flow) > 1:
            # Check for frequent failures
            failed_pipelines = [p for p in pipeline_flow if p.get("status", "").lower() in ["failed", "error"]]
            if len(failed_pipelines) / len(pipeline_flow) > 0.3:
                bottlenecks.append({
                    "location": "cross_cluster",
                    "type": "high_failure_rate",
                    "description": f"High failure rate: {len(failed_pipelines)}/{len(pipeline_flow)} pipelines failed"
                })

    except Exception as e:
        if logger:
            logger.error(f"Error analyzing bottlenecks: {e}")

    return bottlenecks


# ============================================================================
# MACHINE CONFIG POOL ANALYSIS
# ============================================================================

def analyze_machine_config_pool_status(pool: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze machine config pool status and extract key information."""
    try:
        metadata = pool.get("metadata", {})
        spec = pool.get("spec", {})
        status = pool.get("status", {})

        # Extract basic information
        name = metadata.get("name", "unknown")
        machine_count = status.get("machineCount", 0)
        ready_machine_count = status.get("readyMachineCount", 0)
        updated_machine_count = status.get("updatedMachineCount", 0)
        degraded_machine_count = status.get("degradedMachineCount", 0)

        # Determine overall status
        if degraded_machine_count > 0:
            overall_status = "degraded"
        elif machine_count != ready_machine_count:
            overall_status = "updating"
        elif machine_count == ready_machine_count and ready_machine_count == updated_machine_count:
            overall_status = "ready"
        else:
            overall_status = "unknown"

        # Extract configuration information
        configuration = {
            "machine_config_selector": spec.get("machineConfigSelector", {}),
            "node_selector": spec.get("nodeSelector", {}),
            "paused": spec.get("paused", False),
            "max_unavailable": spec.get("maxUnavailable", "1")
        }

        # Extract conditions
        conditions = status.get("conditions", [])

        # Calculate update progress
        if machine_count > 0:
            update_progress = {
                "total_machines": machine_count,
                "ready_machines": ready_machine_count,
                "updated_machines": updated_machine_count,
                "degraded_machines": degraded_machine_count,
                "progress_percentage": round((updated_machine_count / machine_count) * 100, 2) if machine_count > 0 else 0,
                "is_updating": machine_count != updated_machine_count
            }
        else:
            update_progress = {
                "total_machines": 0,
                "ready_machines": 0,
                "updated_machines": 0,
                "degraded_machines": 0,
                "progress_percentage": 0,
                "is_updating": False
            }

        return {
            "name": name,
            "machine_count": machine_count,
            "ready_machine_count": ready_machine_count,
            "status": overall_status,
            "configuration": configuration,
            "conditions": conditions,
            "update_progress": update_progress,
            "node_status": []  # Will be populated later if requested
        }

    except Exception as e:
        return {
            "name": pool.get("metadata", {}).get("name", "unknown"),
            "machine_count": 0,
            "ready_machine_count": 0,
            "status": "error",
            "configuration": {},
            "conditions": [],
            "update_progress": {},
            "node_status": [],
            "error": str(e)
        }


def detect_pool_issues(pool_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Detect issues in machine config pool analysis."""
    issues = []
    name = pool_analysis.get("name", "unknown")
    status = pool_analysis.get("status", "unknown")
    update_progress = pool_analysis.get("update_progress", {})
    conditions = pool_analysis.get("conditions", [])

    # Check for degraded status
    if status == "degraded":
        degraded_count = update_progress.get("degraded_machines", 0)
        issues.append({
            "pool": name,
            "issue_type": "degraded_machines",
            "description": f"Pool has {degraded_count} degraded machine(s)",
            "affected_nodes": [],  # Would need node details to populate
            "severity": "high" if degraded_count > 1 else "medium",
            "remediation": "Check individual node status and machine config application logs"
        })

    # Check for stuck updates
    if update_progress.get("is_updating", False):
        progress_pct = update_progress.get("progress_percentage", 0)
        if progress_pct < 100:
            issues.append({
                "pool": name,
                "issue_type": "update_in_progress",
                "description": f"Update in progress: {progress_pct}% complete",
                "affected_nodes": [],
                "severity": "low",
                "remediation": "Monitor update progress and check for any stuck nodes"
            })

    # Check conditions for specific issues
    for condition in conditions:
        condition_type = condition.get("type", "")
        condition_status = condition.get("status", "")
        condition_reason = condition.get("reason", "")
        condition_message = condition.get("message", "")

        if condition_type == "NodeDegraded" and condition_status == "True":
            issues.append({
                "pool": name,
                "issue_type": "node_degraded",
                "description": f"Node degraded: {condition_message}",
                "affected_nodes": [],
                "severity": "high",
                "remediation": f"Investigate degraded condition: {condition_reason}"
            })
        elif condition_type == "RenderDegraded" and condition_status == "True":
            issues.append({
                "pool": name,
                "issue_type": "render_degraded",
                "description": f"Configuration rendering failed: {condition_message}",
                "affected_nodes": [],
                "severity": "high",
                "remediation": "Check machine config rendering and template validation"
            })

    return issues


def generate_update_recommendations(pools_analysis: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Generate update recommendations based on pool analysis."""
    recommendations = []

    for pool in pools_analysis:
        name = pool.get("name", "unknown")
        status = pool.get("status", "unknown")
        update_progress = pool.get("update_progress", {})
        configuration = pool.get("configuration", {})

        # Recommendation for degraded pools
        if status == "degraded":
            recommendations.append({
                "pool": name,
                "recommendation": "Investigate and resolve degraded machines immediately",
                "reasoning": "Degraded machines can impact cluster stability and workload scheduling",
                "urgency": "high"
            })

        # Recommendation for paused pools
        if configuration.get("paused", False):
            recommendations.append({
                "pool": name,
                "recommendation": "Review paused pool status and resume updates if appropriate",
                "reasoning": "Paused pools may miss critical security and stability updates",
                "urgency": "medium"
            })

        # Recommendation for stuck updates
        if update_progress.get("is_updating", False):
            progress_pct = update_progress.get("progress_percentage", 0)
            if progress_pct > 0 and progress_pct < 100:
                recommendations.append({
                    "pool": name,
                    "recommendation": "Monitor update progress and check for stuck nodes",
                    "reasoning": f"Update is {progress_pct}% complete but may require intervention",
                    "urgency": "low"
                })

    return recommendations


# ============================================================================
# OPERATOR ANALYSIS
# ============================================================================

def analyze_operator_dependencies(operators: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Analyze operator dependencies and relationships."""
    dependencies = []

    # Common OpenShift operator dependency mappings
    operator_deps = {
        "authentication": ["oauth-openshift", "openshift-apiserver"],
        "console": ["authentication", "oauth-openshift"],
        "monitoring": ["prometheus-operator"],
        "ingress": ["dns"],
        "image-registry": ["storage"],
        "openshift-apiserver": ["etcd", "kube-apiserver-operator"],
        "openshift-controller-manager": ["openshift-apiserver"],
        "machine-api": ["cluster-autoscaler-operator"],
        "cluster-autoscaler-operator": ["machine-api"]
    }

    operator_names = {op.get("name", "") for op in operators}

    for operator in operators:
        op_name = operator.get("name", "")
        deps_list = operator_deps.get(op_name, [])

        # Filter dependencies to only include those present in cluster
        existing_deps = [dep for dep in deps_list if dep in operator_names]

        if existing_deps:
            # Check dependency status
            dep_status = "healthy"
            for dep in existing_deps:
                dep_operator = next((op for op in operators if op.get("name") == dep), None)
                if dep_operator:
                    conditions = dep_operator.get("conditions", [])
                    for condition in conditions:
                        if condition.get("type") in ["Degraded", "Available"] and condition.get("status") != "True":
                            dep_status = "unhealthy"
                            break

            dependencies.append({
                "operator": op_name,
                "depends_on": existing_deps,
                "dependency_status": dep_status
            })

    return dependencies


def identify_critical_issues(operators: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Identify critical issues requiring immediate attention."""
    critical_issues = []

    for operator in operators:
        op_name = operator.get("name", "")
        # Simple health assessment based on conditions
        conditions_analysis = operator.get("conditions_analysis", {})
        critical_conditions = conditions_analysis.get("critical_conditions", [])
        warning_conditions = conditions_analysis.get("warning_conditions", [])

        # Determine health status
        if critical_conditions:
            health = "critical"
        elif warning_conditions:
            health = "warning"
        else:
            health = "healthy"

        if health == "critical":
            for cond in critical_conditions:
                critical_issues.append({
                    "operator": op_name,
                    "severity": "critical",
                    "issue": cond.get("message", "Operator is degraded"),
                    "impact": f"Operator {op_name} failure may affect cluster functionality",
                    "recommended_action": f"Investigate and resolve {op_name} operator issues immediately"
                })
        elif health == "warning":
            for cond in warning_conditions:
                critical_issues.append({
                    "operator": op_name,
                    "severity": "warning",
                    "issue": cond.get("message", "Operator is not available"),
                    "impact": f"Operator {op_name} availability issues may affect functionality",
                    "recommended_action": f"Monitor and investigate {op_name} operator availability"
                })

    return critical_issues


def analyze_operator_conditions(conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze operator conditions to determine health status."""
    condition_summary = {
        "available": False,
        "progressing": False,
        "degraded": False,
        "critical_conditions": [],
        "warning_conditions": [],
        "healthy_conditions": []
    }

    for condition in conditions:
        condition_type = condition.get("type", "")
        status = condition.get("status", "Unknown")
        message = condition.get("message", "")
        reason = condition.get("reason", "")

        if condition_type == "Available":
            condition_summary["available"] = status == "True"
        elif condition_type == "Progressing":
            condition_summary["progressing"] = status == "True"
        elif condition_type == "Degraded":
            condition_summary["degraded"] = status == "True"

        # Categorize conditions by severity
        if status == "True" and condition_type in ["Degraded", "Failed"]:
            condition_summary["critical_conditions"].append({
                "type": condition_type,
                "message": message,
                "reason": reason
            })
        elif status == "Unknown" or (status == "False" and condition_type in ["Available"]):
            condition_summary["warning_conditions"].append({
                "type": condition_type,
                "message": message,
                "reason": reason
            })
        else:
            condition_summary["healthy_conditions"].append({
                "type": condition_type,
                "status": status
            })

    return condition_summary


# ============================================================================
# TOPOLOGY MAPPING UTILITIES
# ============================================================================

async def get_multi_cluster_topology_clients(k8s_core_api, k8s_custom_api, k8s_apps_api, k8s_storage_api, k8s_batch_api) -> Dict[str, Dict[str, Any]]:
    """Get authenticated clients for multiple clusters for topology mapping."""
    # For now, return the current cluster - extend this for actual multi-cluster setups
    return {
        "current": {
            "core_api": k8s_core_api,
            "custom_api": k8s_custom_api,
            "apps_api": k8s_apps_api,
            "storage_api": k8s_storage_api,
            "batch_api": k8s_batch_api
        }
    }


def generate_node_id(cluster: str, namespace: str, resource_type: str, name: str) -> str:
    """Generate a unique node ID for the topology graph."""
    return f"{cluster}:{namespace}:{resource_type}:{name}"


def calculate_dependency_weight(source_type: str, target_type: str, relationship: str) -> float:
    """Calculate dependency weight based on relationship criticality."""
    weight_matrix = {
        ("deployment", "service"): 0.9,
        ("deployment", "configmap"): 0.7,
        ("deployment", "secret"): 0.8,
        ("deployment", "persistentvolumeclaim"): 0.6,
        ("service", "pod"): 0.9,
        ("pipelinerun", "pipeline"): 0.9,
        ("taskrun", "task"): 0.8,
        ("pod", "node"): 0.5,
        ("pod", "persistentvolumeclaim"): 0.6,
    }

    key = (source_type.lower(), target_type.lower())
    return weight_matrix.get(key, 0.5)


async def get_resource_metrics(cluster_name: str, resource_type: str, namespace: str, name: str, logger) -> Dict[str, Any]:
    """Get real-time metrics for a resource."""
    try:
        # This would integrate with Prometheus in a real implementation
        # For now, return basic status metrics
        return {
            "cpu_usage": "0.1",
            "memory_usage": "64Mi",
            "status": "running",
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        logger.debug(f"Could not fetch metrics for {resource_type}/{name}: {e}")
        return {}


async def analyze_owner_references(resource: Dict[str, Any], cluster: str) -> List[Dict[str, str]]:
    """Analyze Kubernetes OwnerReferences to find parent-child relationships."""
    edges = []
    owner_refs = resource.get("metadata", {}).get("ownerReferences", [])

    for owner in owner_refs:
        source_id = generate_node_id(
            cluster,
            resource.get("metadata", {}).get("namespace", "default"),
            owner.get("kind", "").lower(),
            owner.get("name", "")
        )
        target_id = generate_node_id(
            cluster,
            resource.get("metadata", {}).get("namespace", "default"),
            resource.get("kind", "").lower(),
            resource.get("metadata", {}).get("name", "")
        )

        edges.append({
            "source": source_id,
            "target": target_id,
            "relationship": "owns",
            "weight": calculate_dependency_weight(owner.get("kind", "").lower(),
                                                 resource.get("kind", "").lower(), "owns")
        })

    return edges


async def analyze_service_dependencies(service: Dict[str, Any], cluster: str, core_api, logger) -> List[Dict[str, str]]:
    """Analyze service selector relationships to pods."""
    edges = []
    try:
        selector = service.get("spec", {}).get("selector", {})
        namespace = service.get("metadata", {}).get("namespace", "default")

        if not selector:
            return edges

        # Find pods matching the selector
        pods = core_api.list_namespaced_pod(namespace=namespace)

        for pod in pods.items:
            pod_labels = pod.metadata.labels or {}

            # Check if all selector labels match pod labels
            if all(pod_labels.get(key) == value for key, value in selector.items()):
                service_id = generate_node_id(cluster, namespace, "service",
                                             service.get("metadata", {}).get("name", ""))
                pod_id = generate_node_id(cluster, namespace, "pod", pod.metadata.name)

                edges.append({
                    "source": service_id,
                    "target": pod_id,
                    "relationship": "routes_to",
                    "weight": calculate_dependency_weight("service", "pod", "routes_to")
                })

    except Exception as e:
        logger.debug(f"Error analyzing service dependencies: {e}")

    return edges


async def analyze_volume_dependencies(resource: Dict[str, Any], cluster: str, logger) -> List[Dict[str, str]]:
    """Analyze volume mount dependencies."""
    edges = []
    try:
        spec = resource.get("spec", {})
        namespace = resource.get("metadata", {}).get("namespace", "default")
        resource_name = resource.get("metadata", {}).get("name", "")
        resource_type = resource.get("kind", "").lower()

        volumes = spec.get("volumes", [])

        for volume in volumes:
            source_id = generate_node_id(cluster, namespace, resource_type, resource_name)

            # Check for PVC references
            if "persistentVolumeClaim" in volume:
                pvc_name = volume["persistentVolumeClaim"]["claimName"]
                target_id = generate_node_id(cluster, namespace, "persistentvolumeclaim", pvc_name)
                edges.append({
                    "source": source_id,
                    "target": target_id,
                    "relationship": "mounts",
                    "weight": calculate_dependency_weight(resource_type, "persistentvolumeclaim", "mounts")
                })

            # Check for ConfigMap references
            if "configMap" in volume:
                cm_name = volume["configMap"]["name"]
                target_id = generate_node_id(cluster, namespace, "configmap", cm_name)
                edges.append({
                    "source": source_id,
                    "target": target_id,
                    "relationship": "mounts",
                    "weight": calculate_dependency_weight(resource_type, "configmap", "mounts")
                })

            # Check for Secret references
            if "secret" in volume:
                secret_name = volume["secret"]["secretName"]
                target_id = generate_node_id(cluster, namespace, "secret", secret_name)
                edges.append({
                    "source": source_id,
                    "target": target_id,
                    "relationship": "mounts",
                    "weight": calculate_dependency_weight(resource_type, "secret", "mounts")
                })

    except Exception as e:
        logger.debug(f"Error analyzing volume dependencies: {e}")

    return edges


# ============================================================================
# SIMULATION AFFECTED COMPONENTS ANALYSIS
# ============================================================================


async def identify_affected_components(
    changes: Dict[str, Any],
    scope: Dict[str, Any],
    scenario_type: str,
    k8s_core_api,
    k8s_apps_api,
    list_pods,
    list_namespaces
) -> List[Dict[str, Any]]:
    """Identify components that will be affected by the proposed changes."""
    from kubernetes.client.rest import ApiException

    try:
        affected_components = []

        # Get namespaces in scope
        if scope.get("namespaces") == ["all"]:
            namespaces = await list_namespaces()
        else:
            namespaces = scope.get("namespaces", [])

        # Analyze components in each namespace
        for namespace in namespaces[:5]:  # Limit to prevent timeout
            try:
                if scenario_type == "scaling":
                    # Identify deployments that could be affected by scaling changes
                    deployments = k8s_apps_api.list_namespaced_deployment(namespace)
                    for deployment in deployments.items:
                        component_info = {
                            "component": f"deployment/{deployment.metadata.name}",
                            "namespace": namespace,
                            "impact_type": "scaling",
                            "severity": "medium",
                            "details": f"Deployment with {deployment.status.replicas} replicas"
                        }

                        # Check if this deployment matches any change criteria
                        for change_key, change_value in changes.items():
                            if change_key.lower() in deployment.metadata.name.lower():
                                component_info["severity"] = "high"
                                component_info["details"] += f" - directly affected by {change_key} changes"
                                break

                        affected_components.append(component_info)

                elif scenario_type == "resource_limits":
                    # Identify pods/containers that could be affected by resource limit changes
                    pods = await list_pods(namespace)
                    for pod in pods[:10]:  # Limit pods to prevent timeout
                        if not pod.get("error"):
                            component_info = {
                                "component": f"pod/{pod['name']}",
                                "namespace": namespace,
                                "impact_type": "resource_limits",
                                "severity": "medium",
                                "details": f"Pod with {len(pod.get('containers', []))} containers"
                            }

                            # Check for resource-constrained containers
                            containers = pod.get('containers', [])
                            constrained_containers = 0
                            for container in containers:
                                if container.get('state') in ['Waiting', 'Terminated']:
                                    constrained_containers += 1

                            if constrained_containers > 0:
                                component_info["severity"] = "high"
                                component_info["details"] += f" - {constrained_containers} containers show resource constraints"

                            affected_components.append(component_info)

                elif scenario_type in ["configuration", "deployment"]:
                    # Identify services and deployments that could be affected
                    services = k8s_core_api.list_namespaced_service(namespace)
                    for service in services.items:
                        component_info = {
                            "component": f"service/{service.metadata.name}",
                            "namespace": namespace,
                            "impact_type": scenario_type,
                            "severity": "low",
                            "details": f"Service with {len(service.spec.ports or [])} ports"
                        }

                        # Higher severity for services with many endpoints
                        if service.spec.ports and len(service.spec.ports) > 3:
                            component_info["severity"] = "medium"

                        affected_components.append(component_info)

            except ApiException as e:
                logger.warning(f"Error analyzing components in namespace {namespace}: {e}")
                continue

        # Limit the number of components returned
        return affected_components[:20]

    except Exception as e:
        logger.error(f"Error identifying affected components: {e}")
        return [{"component": "unknown", "impact_type": "error", "severity": "unknown", "details": str(e)}]
