# ============================================================================
# RESOURCE TOPOLOGY HELPER MODULE
# ============================================================================
#
# This module contains all resource topology related classes, functions, and utilities
# used by the MCP server for dependency analysis, topology mapping, multi-cluster
# coordination, and artifact tracking.
# ============================================================================

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger("lumino-mcp")


# ============================================================================
# MULTI-CLUSTER CLIENT MANAGEMENT
# ============================================================================


async def get_multi_cluster_clients(
    k8s_core_api, k8s_custom_api, k8s_apps_api
) -> Dict[str, Dict[str, Any]]:
    """Get authenticated clients for multiple clusters."""
    # For now, return the current cluster - extend this for actual multi-cluster setups
    return {
        "current": {
            "core_api": k8s_core_api,
            "custom_api": k8s_custom_api,
            "apps_api": k8s_apps_api,
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
    namespaces: Optional[List[str]] = None,
    max_namespaces: int = 50,
    tekton_namespaces: Optional[List[str]] = None,
    logger=None,
) -> List[Dict[str, Any]]:
    """
    Correlate pipeline runs across clusters using labels, annotations, and artifact references.

    Args:
        trace_identifier: The identifier to trace (commit SHA, PR number, image tag, etc.)
        trace_type: Type of trace ("commit", "pr", "image", "custom")
        cluster_clients: Dict of cluster clients with core_api, custom_api, apps_api
        start_time: Optional start time filter (ISO 8601)
        end_time: Optional end time filter (ISO 8601)
        namespaces: Optional list of specific namespaces to search (skips auto-detection)
        max_namespaces: Maximum namespaces to search when auto-detecting (default: 50)
        tekton_namespaces: Optional list of known tekton-active namespaces for prioritization
        logger: Optional logger instance

    Returns:
        List of pipeline info dicts with cluster, namespace, status, and metadata
    """
    pipeline_flow = []

    async def query_namespace(
        cluster_name: str, namespace: str, custom_api
    ) -> List[Dict[str, Any]]:
        """Query PipelineRuns in a single namespace - designed for parallel execution."""
        results = []
        try:
            pipeline_runs = custom_api.list_namespaced_custom_object(
                group="tekton.dev",
                version="v1",
                namespace=namespace,
                plural="pipelineruns",
                limit=200,
            )

            for pr in pipeline_runs.get("items", []):
                if matches_trace_identifier(pr, trace_identifier, trace_type):
                    pipeline_info = {
                        "cluster": cluster_name,
                        "namespace": namespace,
                        "pipeline_name": pr.get("metadata", {}).get("name", "unknown"),
                        "pipeline_run_name": pr.get("metadata", {}).get(
                            "name", "unknown"
                        ),
                        "status": get_pipeline_status(pr),
                        "start_time": pr.get("status", {}).get("startTime"),
                        "completion_time": pr.get("status", {}).get("completionTime"),
                        "tasks": extract_task_info(pr),
                        "labels": pr.get("metadata", {}).get("labels", {}),
                        "annotations": pr.get("metadata", {}).get("annotations", {}),
                    }

                    # Filter by time range if specified
                    if in_time_range(pipeline_info, start_time, end_time):
                        results.append(pipeline_info)

        except Exception as e:
            if logger:
                logger.debug(
                    f"Failed to query PipelineRuns in {cluster_name}/{namespace}: {e}"
                )

        return results

    for cluster_name, clients in cluster_clients.items():
        try:
            custom_api = clients["custom_api"]

            # Determine which namespaces to search
            target_namespaces = []

            if namespaces:
                # User specified exact namespaces - use them directly
                target_namespaces = namespaces
            else:
                # Auto-detect namespaces with tekton prioritization
                try:
                    ns_list = clients["core_api"].list_namespace()
                    all_namespaces = [ns.metadata.name for ns in ns_list.items]

                    if tekton_namespaces:
                        # Prioritize tekton-active namespaces first
                        tekton_set = set(tekton_namespaces)
                        prioritized = [ns for ns in all_namespaces if ns in tekton_set]
                        others = [ns for ns in all_namespaces if ns not in tekton_set]
                        target_namespaces = (prioritized + others)[:max_namespaces]
                    else:
                        # No tekton hints - use heuristic prioritization
                        # Prioritize namespaces likely to have pipelines
                        pipeline_keywords = [
                            "tenant",
                            "pipeline",
                            "tekton",
                            "cicd",
                            "ci-cd",
                            "build",
                            "konflux",
                        ]
                        prioritized = []
                        others = []
                        for ns in all_namespaces:
                            if any(kw in ns.lower() for kw in pipeline_keywords):
                                prioritized.append(ns)
                            else:
                                others.append(ns)
                        target_namespaces = (prioritized + others)[:max_namespaces]

                except Exception as e:
                    if logger:
                        logger.warning(
                            f"Failed to list namespaces in cluster {cluster_name}: {e}"
                        )
                    continue

            if not target_namespaces:
                continue

            if logger:
                logger.info(
                    f"Searching {len(target_namespaces)} namespaces in cluster {cluster_name}"
                )

            # Parallelize namespace queries using asyncio.gather
            tasks = [
                asyncio.create_task(query_namespace(cluster_name, ns, custom_api))
                for ns in target_namespaces
            ]

            # Execute all namespace queries in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Collect results, handling any exceptions
            for result in results:
                if isinstance(result, Exception):
                    if logger:
                        logger.debug(f"Namespace query failed: {result}")
                elif isinstance(result, list):
                    pipeline_flow.extend(result)

        except Exception as e:
            if logger:
                logger.error(f"Failed to query cluster {cluster_name}: {e}")
            continue

    return pipeline_flow


def matches_trace_identifier(
    pipeline_run: Dict[str, Any], trace_identifier: str, trace_type: str
) -> bool:
    """Check if a pipeline run matches the trace identifier."""
    metadata = pipeline_run.get("metadata", {})
    labels = metadata.get("labels", {})
    annotations = metadata.get("annotations", {})

    if trace_type == "commit":
        # Look for commit SHA in labels and annotations
        # Support multiple PAC/Konflux label formats
        commit_keys = [
            "pipelinesascode.tekton.dev/sha",  # Standard PAC
            "pac.test.appstudio.openshift.io/sha",  # Konflux/AppStudio PAC
            "tekton.dev/git-commit",  # Tekton standard
            "git.commit",  # Generic
            "build.appstudio.redhat.com/commit_sha",  # Red Hat AppStudio
        ]
        for key in commit_keys:
            if labels.get(key, "").startswith(trace_identifier) or annotations.get(
                key, ""
            ).startswith(trace_identifier):
                return True
        # Also check in all values (catches non-standard label placements)
        return any(trace_identifier in str(v) for v in labels.values()) or any(
            trace_identifier in str(v) for v in annotations.values()
        )

    elif trace_type == "pr":
        # Look for PR number in labels and annotations
        # Support multiple PAC label formats used by different Konflux/Tekton installations
        pr_keys = [
            "pac.test.appstudio.openshift.io/pull-request",  # Konflux/AppStudio PAC
            "pipelinesascode.tekton.dev/pull-request",  # Standard PAC
            "pull-request",
            "pr",
        ]
        for key in pr_keys:
            if (
                labels.get(key) == trace_identifier
                or annotations.get(key) == trace_identifier
            ):
                return True
        # Also check annotation keys that might store PR info
        pr_annotation_keys = [
            "pipelinesascode.tekton.dev/pull-request",
            "build.appstudio.openshift.io/pull_request_number",
        ]
        for key in pr_annotation_keys:
            if annotations.get(key) == trace_identifier:
                return True
        return False

    elif trace_type == "image":
        # Look for image reference in labels, annotations, or results
        return any(trace_identifier in str(v) for v in labels.values()) or any(
            trace_identifier in str(v) for v in annotations.values()
        )

    elif trace_type == "custom":
        # Search across all labels and annotations
        name = metadata.get("name", "")
        return (
            trace_identifier in name
            or any(trace_identifier in str(v) for v in labels.values())
            or any(trace_identifier in str(v) for v in annotations.values())
        )

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
            "status": task_run_status.get("status", {})
            .get("conditions", [{}])[-1]
            .get("reason", "Unknown"),
            "start_time": task_run_status.get("status", {}).get("startTime"),
            "completion_time": task_run_status.get("status", {}).get("completionTime"),
        }
        tasks.append(task_info)

    return tasks


def in_time_range(
    pipeline_info: Dict[str, Any], start_time: Optional[str], end_time: Optional[str]
) -> bool:
    """Check if pipeline execution falls within the specified time range."""
    if not start_time and not end_time:
        return True

    pipeline_start = pipeline_info.get("start_time")
    if not pipeline_start:
        return True

    try:
        pipeline_dt = datetime.fromisoformat(pipeline_start.replace("Z", "+00:00"))

        if start_time:
            start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            if pipeline_dt < start_dt:
                return False

        if end_time:
            end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            if pipeline_dt > end_dt:
                return False

        return True
    except Exception:
        return True


# ============================================================================
# FULL LIFECYCLE CHAIN FOLLOWING
# ============================================================================


async def follow_lifecycle_chain(
    pipeline_flow: List[Dict[str, Any]],
    custom_api,
    core_api,
    trace_depth: str = "deep",
    logger=None,
) -> Dict[str, Any]:
    """
    Follow the Konflux lifecycle chain from build PLRs through snapshots,
    integration tests, releases, and release pipelines.

    Chain: Build PLR → Snapshot → Integration Tests → Release → Managed/Tenant/Final PLR → Nudge Cascade

    Args:
        pipeline_flow: Initial matched PipelineRuns from correlate_pipeline_events()
        custom_api: Kubernetes CustomObjectsApi for CRD queries
        core_api: Kubernetes CoreV1Api
        trace_depth: "shallow" (snapshots only) or "deep" (full chain)
        logger: Optional logger

    Returns:
        Dict with snapshots, integration_tests, releases, release_pipelines, nudge_cascade
    """
    lifecycle = {
        "application": None,
        "component": None,
        "snapshots": [],
        "integration_tests": [],
        "releases": [],
        "release_pipelines": [],
        "nudge_cascade": [],
    }

    seen_snapshots = set()
    seen_releases = set()
    seen_plrs = set()

    for plr in pipeline_flow:
        annotations = plr.get("annotations", {})
        labels = plr.get("labels", {})
        namespace = plr.get("namespace", "")

        # ── Step 2: Build PLR → Snapshot ──
        # Check both annotations (build PLRs) and labels (test PLRs) for snapshot reference
        snapshot_name = annotations.get(
            "appstudio.openshift.io/snapshot"
        ) or labels.get("appstudio.openshift.io/snapshot")
        if not snapshot_name or snapshot_name in seen_snapshots:
            continue
        seen_snapshots.add(snapshot_name)

        snapshot_data = await _resolve_snapshot(
            custom_api, namespace, snapshot_name, plr, logger
        )
        if not snapshot_data:
            continue
        lifecycle["snapshots"].append(snapshot_data)

        # ── Step 2b: Resolve Application and Component context ──
        if not lifecycle["application"]:
            app_name = snapshot_data.get("application")
            if app_name:
                lifecycle["application"] = await _resolve_application(
                    custom_api, namespace, app_name, logger
                )

        if not lifecycle["component"]:
            comp_name = labels.get("appstudio.openshift.io/component")
            if comp_name:
                lifecycle["component"] = await _resolve_component(
                    custom_api, namespace, comp_name, logger
                )

        # ── Step 3: Snapshot → Integration Tests ──
        test_entries = _extract_test_info_from_snapshot(snapshot_data)
        for test_entry in test_entries:
            lifecycle["integration_tests"].append(test_entry)
            # Optionally fetch the test PLR for task-level details
            if trace_depth == "deep" and test_entry.get("plr_name"):
                test_plr_detail = await _resolve_plr(
                    custom_api, namespace, test_entry["plr_name"], logger
                )
                if test_plr_detail:
                    test_entry["tasks"] = extract_task_info_from_status(test_plr_detail)
                    test_entry["pipeline"] = (
                        test_plr_detail.get("metadata", {})
                        .get("labels", {})
                        .get("tekton.dev/pipeline", "")
                    )

        # ── Step 4: Snapshot → Releases ──
        releases = await _resolve_releases_for_snapshot(
            custom_api, namespace, snapshot_name, logger
        )
        for release in releases:
            release_key = release.get("name", "")
            if release_key in seen_releases:
                continue
            seen_releases.add(release_key)
            lifecycle["releases"].append(release)

            # ── Step 5: Release → Managed/Tenant/Final PLRs ──
            if trace_depth == "deep":
                release_plrs = await _resolve_release_pipelines(
                    custom_api, release, logger
                )
                for rp in release_plrs:
                    plr_key = f"{rp.get('namespace')}/{rp.get('name')}"
                    if plr_key not in seen_plrs:
                        seen_plrs.add(plr_key)
                        lifecycle["release_pipelines"].append(rp)

        # ── Step 6: Nudge Cascade (deep only) ──
        if trace_depth == "deep" and snapshot_data.get("nudge_processed"):
            nudge_entries = await _resolve_nudge_cascade(
                custom_api, namespace, snapshot_data, lifecycle["releases"], logger
            )
            lifecycle["nudge_cascade"].extend(nudge_entries)

    return lifecycle


async def _resolve_plr(
    custom_api, namespace: str, plr_name: str, logger=None
) -> Optional[Dict]:
    """Fetch a single PipelineRun resource."""
    try:
        return custom_api.get_namespaced_custom_object(
            group="tekton.dev",
            version="v1",
            namespace=namespace,
            plural="pipelineruns",
            name=plr_name,
        )
    except Exception as e:
        if logger:
            logger.debug(f"Failed to resolve PLR {plr_name} in {namespace}: {e}")
        return None


async def _resolve_application(
    custom_api, namespace: str, app_name: str, logger=None
) -> Optional[Dict]:
    """Fetch an Application resource and extract key fields."""
    try:
        app = custom_api.get_namespaced_custom_object(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace=namespace,
            plural="applications",
            name=app_name,
        )
        spec = app.get("spec", {})

        # Count components belonging to this application
        component_count = 0
        try:
            comp_list = custom_api.list_namespaced_custom_object(
                group="appstudio.redhat.com",
                version="v1alpha1",
                namespace=namespace,
                plural="components",
                label_selector=f"appstudio.openshift.io/application={app_name}",
            )
            component_count = len(comp_list.get("items", []))
        except Exception:
            pass

        return {
            "name": app_name,
            "namespace": namespace,
            "display_name": spec.get("displayName", app_name),
            "component_count": component_count,
        }
    except Exception as e:
        if logger:
            logger.debug(
                f"Failed to resolve application {app_name} in {namespace}: {e}"
            )
        return {"name": app_name, "namespace": namespace, "status": "not_found"}


async def _resolve_component(
    custom_api, namespace: str, comp_name: str, logger=None
) -> Optional[Dict]:
    """Fetch a Component resource and extract key fields."""
    try:
        comp = custom_api.get_namespaced_custom_object(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace=namespace,
            plural="components",
            name=comp_name,
        )
        spec = comp.get("spec", {})
        status = comp.get("status", {})
        annotations = comp.get("metadata", {}).get("annotations", {})

        # Parse build pipeline annotation
        build_pipeline = ""
        try:
            import json

            pipeline_info = json.loads(
                annotations.get("build.appstudio.openshift.io/pipeline", "{}")
            )
            build_pipeline = pipeline_info.get("name", "")
        except Exception:
            pass

        # Parse PaC status
        pac_enabled = False
        try:
            import json

            pac_info = json.loads(
                annotations.get("build.appstudio.openshift.io/status", "{}")
            )
            pac_enabled = pac_info.get("pac", {}).get("state") == "enabled"
        except Exception:
            pass

        git_source = spec.get("source", {}).get("git", {})

        return {
            "name": comp_name,
            "namespace": namespace,
            "application": spec.get("application", ""),
            "source_url": git_source.get("url", ""),
            "revision": git_source.get("revision", ""),
            "dockerfile": git_source.get("dockerfileUrl", ""),
            "context": git_source.get("context", ""),
            "build_pipeline": build_pipeline,
            "pac_enabled": pac_enabled,
            "container_image": spec.get("containerImage", "")[:100],
            "last_built_commit": status.get("lastBuiltCommit", "")[:12],
        }
    except Exception as e:
        if logger:
            logger.debug(f"Failed to resolve component {comp_name} in {namespace}: {e}")
        return {"name": comp_name, "namespace": namespace, "status": "not_found"}


async def _resolve_snapshot(
    custom_api, namespace: str, snapshot_name: str, source_plr: Dict, logger=None
) -> Optional[Dict]:
    """Fetch a Snapshot resource and extract key fields."""
    try:
        snapshot = custom_api.get_namespaced_custom_object(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace=namespace,
            plural="snapshots",
            name=snapshot_name,
        )
        metadata = snapshot.get("metadata", {})
        annotations = metadata.get("annotations", {})
        spec = snapshot.get("spec", {})
        status = snapshot.get("status", {})
        conditions = status.get("conditions", [])

        # Parse conditions into a simple dict
        condition_map = {}
        for c in conditions:
            condition_map[c.get("type", "")] = {
                "status": c.get("status"),
                "reason": c.get("reason"),
                "message": c.get("message", "")[:200],
            }

        # Extract components
        components = []
        for comp in spec.get("components", []):
            components.append(
                {
                    "name": comp.get("name", ""),
                    "containerImage": comp.get("containerImage", "")[:100],
                    "git_url": comp.get("source", {}).get("git", {}).get("url", ""),
                    "revision": comp.get("source", {})
                    .get("git", {})
                    .get("revision", "")[:12],
                }
            )

        return {
            "name": snapshot_name,
            "namespace": namespace,
            "application": spec.get("application", ""),
            "components": components,
            "component_count": len(components),
            "conditions": condition_map,
            "tests_passed": condition_map.get("AppStudioTestSucceeded", {}).get(
                "status"
            )
            == "True",
            "auto_released": condition_map.get("AutoReleased", {}).get("status")
            == "True",
            "nudge_processed": annotations.get(
                "build.appstudio.openshift.io/component-nudge-processed"
            )
            == "true",
            "triggered_by_plr": source_plr.get("pipeline_run_name", ""),
            "created_at": metadata.get("creationTimestamp", ""),
            "_raw_annotations": annotations,  # Kept for test extraction
        }
    except Exception as e:
        if logger:
            logger.debug(
                f"Failed to resolve snapshot {snapshot_name} in {namespace}: {e}"
            )
        status_msg = (
            "not_found"
            if "404" in str(e)
            else "rbac_denied"
            if "403" in str(e)
            else str(e)[:80]
        )
        return {
            "name": snapshot_name,
            "namespace": namespace,
            "status": status_msg,
            "components": [],
            "component_count": 0,
            "conditions": {},
            "tests_passed": None,
            "auto_released": None,
            "nudge_processed": False,
            "triggered_by_plr": source_plr.get("pipeline_run_name", ""),
        }


def _extract_test_info_from_snapshot(snapshot_data: Dict) -> List[Dict]:
    """Extract integration test results from snapshot annotations."""
    import json

    tests = []
    raw_annotations = snapshot_data.get("_raw_annotations", {})
    test_status_str = raw_annotations.get("test.appstudio.openshift.io/status", "[]")

    try:
        test_statuses = json.loads(test_status_str)
    except (json.JSONDecodeError, TypeError):
        return tests

    for test in test_statuses:
        duration_seconds = None
        start_time = test.get("startTime")
        completion_time = test.get("completionTime")
        if start_time and completion_time:
            try:
                from datetime import datetime

                s = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                e = datetime.fromisoformat(completion_time.replace("Z", "+00:00"))
                duration_seconds = int((e - s).total_seconds())
            except Exception:
                pass

        tests.append(
            {
                "scenario": test.get("scenario", ""),
                "status": test.get("status", ""),
                "plr_name": test.get("testPipelineRunName", ""),
                "start_time": start_time,
                "completion_time": completion_time,
                "duration_seconds": duration_seconds,
                "snapshot": snapshot_data.get("name", ""),
            }
        )

    return tests


async def _resolve_releases_for_snapshot(
    custom_api, namespace: str, snapshot_name: str, logger=None
) -> List[Dict]:
    """Find Release resources linked to a snapshot via label."""
    releases = []
    try:
        release_list = custom_api.list_namespaced_custom_object(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace=namespace,
            plural="releases",
            label_selector=f"release.appstudio.openshift.io/snapshot={snapshot_name}",
        )

        for release in release_list.get("items", []):
            metadata = release.get("metadata", {})
            spec = release.get("spec", {})
            status = release.get("status", {})
            conditions = status.get("conditions", [])

            condition_map = {}
            for c in conditions:
                condition_map[c.get("type", "")] = {
                    "status": c.get("status"),
                    "reason": c.get("reason"),
                    "message": c.get("message", "")[:200],
                }

            releases.append(
                {
                    "name": metadata.get("name", ""),
                    "namespace": namespace,
                    "snapshot": snapshot_name,
                    "release_plan": spec.get("releasePlan", ""),
                    "grace_period_days": spec.get("gracePeriodDays"),
                    "status": "Succeeded"
                    if condition_map.get("Released", {}).get("status") == "True"
                    else "Failed"
                    if condition_map.get("Released", {}).get("status") == "False"
                    else "InProgress",
                    "conditions": condition_map,
                    "automated": status.get("automated", False),
                    "author": status.get("attribution", {}).get("author", ""),
                    "start_time": status.get("startTime"),
                    "completion_time": status.get("completionTime"),
                    "target": status.get("target", ""),
                    "artifacts": status.get("artifacts", {}),
                    # Pipeline references for step 5
                    "_managed_plr_ref": status.get("managedProcessing", {}).get(
                        "pipelineRun"
                    ),
                    "_tenant_plr_ref": status.get("tenantProcessing", {}).get(
                        "pipelineRun"
                    ),
                    "_final_plr_ref": status.get("finalProcessing", {}).get(
                        "pipelineRun"
                    ),
                }
            )

    except Exception as e:
        if logger:
            logger.debug(f"Failed to find releases for snapshot {snapshot_name}: {e}")

    return releases


async def _resolve_release_pipelines(
    custom_api, release: Dict, logger=None
) -> List[Dict]:
    """Resolve managed, tenant, and final PLRs from a Release resource."""
    plrs = []
    refs = [
        ("managed", release.get("_managed_plr_ref")),
        ("tenant", release.get("_tenant_plr_ref")),
        ("final", release.get("_final_plr_ref")),
    ]

    for stage, ref in refs:
        if not ref or "/" not in ref:
            continue
        ns, name = ref.split("/", 1)

        try:
            plr = custom_api.get_namespaced_custom_object(
                group="tekton.dev",
                version="v1",
                namespace=ns,
                plural="pipelineruns",
                name=name,
            )
            status = plr.get("status", {})
            conditions = status.get("conditions", [])
            plr_status = "Unknown"
            plr_message = ""
            for c in conditions:
                if c.get("type") == "Succeeded":
                    plr_status = c.get("reason", "Unknown")
                    plr_message = c.get("message", "")
                    break

            # Extract tasks
            tasks = extract_task_info_from_status(plr)

            # Calculate duration
            duration_seconds = None
            start = status.get("startTime")
            end = status.get("completionTime")
            if start and end:
                try:
                    from datetime import datetime

                    s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                    e = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    duration_seconds = int((e - s).total_seconds())
                except Exception:
                    pass

            plrs.append(
                {
                    "name": name,
                    "namespace": ns,
                    "stage": stage,
                    "pipeline": plr.get("metadata", {})
                    .get("labels", {})
                    .get("tekton.dev/pipeline", ""),
                    "status": plr_status,
                    "message": plr_message[:150],
                    "start_time": start,
                    "completion_time": end,
                    "duration_seconds": duration_seconds,
                    "tasks": tasks,
                    "task_count": len(tasks),
                    "release": release.get("name", ""),
                }
            )

        except Exception as e:
            if logger:
                logger.debug(f"Failed to resolve {stage} PLR {ns}/{name}: {e}")
            status_msg = (
                "not_found"
                if "404" in str(e)
                else "rbac_denied"
                if "403" in str(e)
                else str(e)[:60]
            )
            plrs.append(
                {
                    "name": name,
                    "namespace": ns,
                    "stage": stage,
                    "status": status_msg,
                    "release": release.get("name", ""),
                }
            )

    return plrs


def extract_task_info_from_status(plr: Dict) -> List[Dict]:
    """Extract task info from a PipelineRun, using childReferences (v1) or taskRuns (v1beta1)."""
    tasks = []

    # Try childReferences first (Tekton v1 format)
    child_refs = plr.get("status", {}).get("childReferences", [])
    if child_refs:
        for ref in child_refs:
            tasks.append(
                {
                    "name": ref.get("pipelineTaskName", ref.get("name", "")),
                    "taskrun_name": ref.get("name", ""),
                }
            )
        return tasks

    # Fall back to inline taskRuns (v1beta1 format)
    task_runs = plr.get("status", {}).get("taskRuns", {})
    for task_run_name, task_run_status in task_runs.items():
        task_status = task_run_status.get("status", {})
        conds = task_status.get("conditions", [])
        result = conds[-1].get("reason", "Unknown") if conds else "Unknown"

        start = task_status.get("startTime")
        end = task_status.get("completionTime")
        duration = None
        if start and end:
            try:
                from datetime import datetime

                s = datetime.fromisoformat(start.replace("Z", "+00:00"))
                e = datetime.fromisoformat(end.replace("Z", "+00:00"))
                duration = int((e - s).total_seconds())
            except Exception:
                pass

        tasks.append(
            {
                "name": task_run_name,
                "status": result,
                "start_time": start,
                "completion_time": end,
                "duration_seconds": duration,
            }
        )

    return tasks


async def _resolve_nudge_cascade(
    custom_api, namespace: str, snapshot_data: Dict, releases: List[Dict], logger=None
) -> List[Dict]:
    """Find downstream build PLRs triggered by component nudge after a release."""
    nudge_entries = []

    # Find the latest release completion time as the nudge start point
    latest_completion = None
    for release in releases:
        comp_time = release.get("completion_time")
        if comp_time:
            try:
                from datetime import datetime

                dt = datetime.fromisoformat(comp_time.replace("Z", "+00:00"))
                if latest_completion is None or dt > latest_completion:
                    latest_completion = dt
            except Exception:
                pass

    if not latest_completion:
        return nudge_entries

    try:
        # List recent PLRs in the same namespace
        plr_list = custom_api.list_namespaced_custom_object(
            group="tekton.dev",
            version="v1",
            namespace=namespace,
            plural="pipelineruns",
            limit=50,
        )

        for plr in plr_list.get("items", []):
            metadata = plr.get("metadata", {})
            annotations = metadata.get("annotations", {})
            labels = metadata.get("labels", {})
            created = metadata.get("creationTimestamp", "")

            # Check if this PLR was created after the release and is a nudge-triggered build
            sender = annotations.get("pipelinesascode.tekton.dev/sender", "")
            title = annotations.get("pipelinesascode.tekton.dev/sha-title", "")
            is_nudge = (
                "konflux" in sender.lower() or "red-hat-konflux" in sender.lower()
            ) and ("chore(deps)" in title.lower() or "update" in title.lower())

            if not is_nudge:
                continue

            # Check creation time is after the release
            try:
                from datetime import datetime

                created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                if created_dt <= latest_completion:
                    continue
            except Exception:
                continue

            component = labels.get("appstudio.openshift.io/component", "")
            status_conds = plr.get("status", {}).get("conditions", [])
            plr_status = (
                status_conds[0].get("reason", "Unknown") if status_conds else "Unknown"
            )

            nudge_entries.append(
                {
                    "plr_name": metadata.get("name", ""),
                    "component": component,
                    "status": plr_status,
                    "triggered_at": created,
                    "title": title[:100],
                    "sender": sender[:40],
                }
            )

    except Exception as e:
        if logger:
            logger.debug(f"Failed to resolve nudge cascade in {namespace}: {e}")

    return nudge_entries


# ============================================================================
# ARTIFACT TRACKING AND ANALYSIS
# ============================================================================


async def track_artifacts(
    pipeline_flow: List[Dict[str, Any]], include_artifacts: bool = True, logger=None
) -> List[Dict[str, Any]]:
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
                logger.debug(
                    f"Failed to track artifacts for pipeline {pipeline.get('pipeline_name', '')}: {e}"
                )
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
        if any(
            keyword in key.lower() for keyword in ["image", "container", "artifact"]
        ):
            potential_images.append(value)

    for key, value in annotations.items():
        if any(
            keyword in key.lower() for keyword in ["image", "container", "artifact"]
        ):
            potential_images.append(value)

    for image in potential_images:
        if image and ":" in image:  # Basic image validation
            artifacts.append(
                {
                    "artifact_id": image,
                    "type": "container_image",
                    "registry": image.split("/")[0] if "/" in image else "unknown",
                    "propagation_path": [
                        {
                            "cluster": pipeline["cluster"],
                            "namespace": pipeline["namespace"],
                            "pipeline": pipeline["pipeline_name"],
                            "timestamp": pipeline.get("start_time", ""),
                        }
                    ],
                }
            )

    return artifacts


# ============================================================================
# PERFORMANCE ANALYSIS AND BOTTLENECK DETECTION
# ============================================================================


def analyze_bottlenecks(
    pipeline_flow: List[Dict[str, Any]], logger=None
) -> List[Dict[str, Any]]:
    """Analyze pipeline flow for bottlenecks and performance issues."""
    bottlenecks = []

    try:
        for i, pipeline in enumerate(pipeline_flow):
            start_time = pipeline.get("start_time")
            completion_time = pipeline.get("completion_time")

            if start_time and completion_time:
                try:
                    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(
                        completion_time.replace("Z", "+00:00")
                    )
                    duration = (end_dt - start_dt).total_seconds()

                    # Flag pipelines that take unusually long (> 30 minutes)
                    if duration > 1800:
                        bottlenecks.append(
                            {
                                "location": f"{pipeline['cluster']}/{pipeline['namespace']}/{pipeline['pipeline_name']}",
                                "type": "long_duration",
                                "duration": duration,
                                "description": f"Pipeline execution took {duration / 60:.1f} minutes",
                            }
                        )

                    # Check task-level bottlenecks
                    for task in pipeline.get("tasks", []):
                        task_start = task.get("start_time")
                        task_end = task.get("completion_time")

                        if task_start and task_end:
                            try:
                                task_start_dt = datetime.fromisoformat(
                                    task_start.replace("Z", "+00:00")
                                )
                                task_end_dt = datetime.fromisoformat(
                                    task_end.replace("Z", "+00:00")
                                )
                                task_duration = (
                                    task_end_dt - task_start_dt
                                ).total_seconds()

                                # Flag tasks that take more than 15 minutes
                                if task_duration > 900:
                                    bottlenecks.append(
                                        {
                                            "location": f"{pipeline['cluster']}/{pipeline['namespace']}/{task['name']}",
                                            "type": "slow_task",
                                            "duration": task_duration,
                                            "description": f"Task '{task['name']}' took {task_duration / 60:.1f} minutes",
                                        }
                                    )
                            except Exception:
                                continue

                except Exception:
                    continue

        # Look for patterns across the flow
        if len(pipeline_flow) > 1:
            # Check for frequent failures
            failed_pipelines = [
                p
                for p in pipeline_flow
                if p.get("status", "").lower() in ["failed", "error"]
            ]
            if len(failed_pipelines) / len(pipeline_flow) > 0.3:
                bottlenecks.append(
                    {
                        "location": "cross_cluster",
                        "type": "high_failure_rate",
                        "description": f"High failure rate: {len(failed_pipelines)}/{len(pipeline_flow)} pipelines failed",
                    }
                )

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
        elif (
            machine_count == ready_machine_count
            and ready_machine_count == updated_machine_count
        ):
            overall_status = "ready"
        else:
            overall_status = "unknown"

        # Extract configuration information
        configuration = {
            "machine_config_selector": spec.get("machineConfigSelector", {}),
            "node_selector": spec.get("nodeSelector", {}),
            "paused": spec.get("paused", False),
            "max_unavailable": spec.get("maxUnavailable", "1"),
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
                "progress_percentage": round(
                    (updated_machine_count / machine_count) * 100, 2
                )
                if machine_count > 0
                else 0,
                "is_updating": machine_count != updated_machine_count,
            }
        else:
            update_progress = {
                "total_machines": 0,
                "ready_machines": 0,
                "updated_machines": 0,
                "degraded_machines": 0,
                "progress_percentage": 0,
                "is_updating": False,
            }

        return {
            "name": name,
            "machine_count": machine_count,
            "ready_machine_count": ready_machine_count,
            "status": overall_status,
            "configuration": configuration,
            "conditions": conditions,
            "update_progress": update_progress,
            "node_status": [],  # Will be populated later if requested
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
            "error": str(e),
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
        issues.append(
            {
                "pool": name,
                "issue_type": "degraded_machines",
                "description": f"Pool has {degraded_count} degraded machine(s)",
                "affected_nodes": [],  # Would need node details to populate
                "severity": "high" if degraded_count > 1 else "medium",
                "remediation": "Check individual node status and machine config application logs",
            }
        )

    # Check for stuck updates
    if update_progress.get("is_updating", False):
        progress_pct = update_progress.get("progress_percentage", 0)
        if progress_pct < 100:
            issues.append(
                {
                    "pool": name,
                    "issue_type": "update_in_progress",
                    "description": f"Update in progress: {progress_pct}% complete",
                    "affected_nodes": [],
                    "severity": "low",
                    "remediation": "Monitor update progress and check for any stuck nodes",
                }
            )

    # Check conditions for specific issues
    for condition in conditions:
        condition_type = condition.get("type", "")
        condition_status = condition.get("status", "")
        condition_reason = condition.get("reason", "")
        condition_message = condition.get("message", "")

        if condition_type == "NodeDegraded" and condition_status == "True":
            issues.append(
                {
                    "pool": name,
                    "issue_type": "node_degraded",
                    "description": f"Node degraded: {condition_message}",
                    "affected_nodes": [],
                    "severity": "high",
                    "remediation": f"Investigate degraded condition: {condition_reason}",
                }
            )
        elif condition_type == "RenderDegraded" and condition_status == "True":
            issues.append(
                {
                    "pool": name,
                    "issue_type": "render_degraded",
                    "description": f"Configuration rendering failed: {condition_message}",
                    "affected_nodes": [],
                    "severity": "high",
                    "remediation": "Check machine config rendering and template validation",
                }
            )

    return issues


def generate_update_recommendations(
    pools_analysis: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Generate update recommendations based on pool analysis."""
    recommendations = []

    for pool in pools_analysis:
        name = pool.get("name", "unknown")
        status = pool.get("status", "unknown")
        update_progress = pool.get("update_progress", {})
        configuration = pool.get("configuration", {})

        # Recommendation for degraded pools
        if status == "degraded":
            recommendations.append(
                {
                    "pool": name,
                    "recommendation": "Investigate and resolve degraded machines immediately",
                    "reasoning": "Degraded machines can impact cluster stability and workload scheduling",
                    "urgency": "high",
                }
            )

        # Recommendation for paused pools
        if configuration.get("paused", False):
            recommendations.append(
                {
                    "pool": name,
                    "recommendation": "Review paused pool status and resume updates if appropriate",
                    "reasoning": "Paused pools may miss critical security and stability updates",
                    "urgency": "medium",
                }
            )

        # Recommendation for stuck updates
        if update_progress.get("is_updating", False):
            progress_pct = update_progress.get("progress_percentage", 0)
            if progress_pct > 0 and progress_pct < 100:
                recommendations.append(
                    {
                        "pool": name,
                        "recommendation": "Monitor update progress and check for stuck nodes",
                        "reasoning": f"Update is {progress_pct}% complete but may require intervention",
                        "urgency": "low",
                    }
                )

    return recommendations


# ============================================================================
# OPERATOR ANALYSIS
# ============================================================================


def analyze_operator_dependencies(
    operators: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
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
        "cluster-autoscaler-operator": ["machine-api"],
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
                dep_operator = next(
                    (op for op in operators if op.get("name") == dep), None
                )
                if dep_operator:
                    conditions = dep_operator.get("conditions", [])
                    for condition in conditions:
                        if (
                            condition.get("type") in ["Degraded", "Available"]
                            and condition.get("status") != "True"
                        ):
                            dep_status = "unhealthy"
                            break

            dependencies.append(
                {
                    "operator": op_name,
                    "depends_on": existing_deps,
                    "dependency_status": dep_status,
                }
            )

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
                critical_issues.append(
                    {
                        "operator": op_name,
                        "severity": "critical",
                        "issue": cond.get("message", "Operator is degraded"),
                        "impact": f"Operator {op_name} failure may affect cluster functionality",
                        "recommended_action": f"Investigate and resolve {op_name} operator issues immediately",
                    }
                )
        elif health == "warning":
            for cond in warning_conditions:
                critical_issues.append(
                    {
                        "operator": op_name,
                        "severity": "warning",
                        "issue": cond.get("message", "Operator is not available"),
                        "impact": f"Operator {op_name} availability issues may affect functionality",
                        "recommended_action": f"Monitor and investigate {op_name} operator availability",
                    }
                )

    return critical_issues


def analyze_operator_conditions(conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze operator conditions to determine health status."""
    condition_summary = {
        "available": False,
        "progressing": False,
        "degraded": False,
        "critical_conditions": [],
        "warning_conditions": [],
        "healthy_conditions": [],
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
            condition_summary["critical_conditions"].append(
                {"type": condition_type, "message": message, "reason": reason}
            )
        elif status == "Unknown" or (
            status == "False" and condition_type in ["Available"]
        ):
            condition_summary["warning_conditions"].append(
                {"type": condition_type, "message": message, "reason": reason}
            )
        else:
            condition_summary["healthy_conditions"].append(
                {"type": condition_type, "status": status}
            )

    return condition_summary


# ============================================================================
# TOPOLOGY MAPPING UTILITIES
# ============================================================================


async def get_multi_cluster_topology_clients(
    k8s_core_api, k8s_custom_api, k8s_apps_api, k8s_storage_api, k8s_batch_api
) -> Dict[str, Dict[str, Any]]:
    """Get authenticated clients for multiple clusters for topology mapping."""
    # For now, return the current cluster - extend this for actual multi-cluster setups
    return {
        "current": {
            "core_api": k8s_core_api,
            "custom_api": k8s_custom_api,
            "apps_api": k8s_apps_api,
            "storage_api": k8s_storage_api,
            "batch_api": k8s_batch_api,
        }
    }


def generate_node_id(
    cluster: str, namespace: str, resource_type: str, name: str
) -> str:
    """Generate a unique node ID for the topology graph."""
    return f"{cluster}:{namespace}:{resource_type}:{name}"


def calculate_dependency_weight(
    source_type: str, target_type: str, relationship: str
) -> float:
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


async def get_resource_metrics(
    cluster_name: str, resource_type: str, namespace: str, name: str, logger
) -> Dict[str, Any]:
    """Get real-time metrics for a resource."""
    try:
        # This would integrate with Prometheus in a real implementation
        # For now, return basic status metrics
        return {
            "cpu_usage": "0.1",
            "memory_usage": "64Mi",
            "status": "running",
            "last_updated": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.debug(f"Could not fetch metrics for {resource_type}/{name}: {e}")
        return {}


async def analyze_owner_references(
    resource: Dict[str, Any], cluster: str, resource_type: str
) -> List[Dict[str, str]]:
    """Analyze Kubernetes OwnerReferences to find parent-child relationships.

    Args:
        resource: Resource dict from Kubernetes API
        cluster: Cluster name
        resource_type: The type of the resource (e.g., 'pod', 'deployment', 'replicaset')
                      Required because .to_dict() doesn't include 'kind' field.
    """
    edges = []
    owner_refs = resource.get("metadata", {}).get("owner_references", [])

    # Also check camelCase version (raw API response vs client model)
    if not owner_refs:
        owner_refs = resource.get("metadata", {}).get("ownerReferences", [])

    for owner in owner_refs:
        owner_kind = owner.get("kind", "").lower()
        owner_name = owner.get("name", "")

        # Skip if owner info is missing
        if not owner_kind or not owner_name:
            continue

        source_id = generate_node_id(
            cluster,
            resource.get("metadata", {}).get("namespace", "default"),
            owner_kind,
            owner_name,
        )
        target_id = generate_node_id(
            cluster,
            resource.get("metadata", {}).get("namespace", "default"),
            resource_type,
            resource.get("metadata", {}).get("name", ""),
        )

        edges.append(
            {
                "source": source_id,
                "target": target_id,
                "relationship": "owns",
                "weight": calculate_dependency_weight(
                    owner_kind, resource_type, "owns"
                ),
            }
        )

    return edges


async def analyze_service_dependencies(
    service: Dict[str, Any], cluster: str, core_api, logger, pods_list=None
) -> List[Dict[str, str]]:
    """Analyze service selector relationships to pods.

    Args:
        service: Service resource dict
        cluster: Cluster name
        core_api: Kubernetes CoreV1Api client
        logger: Logger instance
        pods_list: Optional pre-fetched list of pods to avoid N+1 queries.
                   If None, will fetch pods from API (legacy behavior).
    """
    edges = []
    try:
        selector = service.get("spec", {}).get("selector", {})
        namespace = service.get("metadata", {}).get("namespace", "default")

        if not selector:
            return edges

        # Use pre-fetched pods if available, otherwise fetch (legacy fallback)
        if pods_list is not None:
            pods_items = pods_list
        else:
            import asyncio

            pods = await asyncio.to_thread(
                core_api.list_namespaced_pod, namespace=namespace
            )
            pods_items = pods.items

        for pod in pods_items:
            pod_labels = pod.metadata.labels or {}

            # Check if all selector labels match pod labels
            if all(pod_labels.get(key) == value for key, value in selector.items()):
                service_id = generate_node_id(
                    cluster,
                    namespace,
                    "service",
                    service.get("metadata", {}).get("name", ""),
                )
                pod_id = generate_node_id(cluster, namespace, "pod", pod.metadata.name)

                edges.append(
                    {
                        "source": service_id,
                        "target": pod_id,
                        "relationship": "routes_to",
                        "weight": calculate_dependency_weight(
                            "service", "pod", "routes_to"
                        ),
                    }
                )

    except Exception as e:
        logger.debug(f"Error analyzing service dependencies: {e}")

    return edges


async def analyze_volume_dependencies(
    resource: Dict[str, Any], cluster: str, resource_type: str, logger
) -> List[Dict[str, str]]:
    """Analyze volume mount dependencies.

    Args:
        resource: Resource dict from Kubernetes API
        cluster: Cluster name
        resource_type: The type of the resource (e.g., 'pod', 'deployment')
                      Required because .to_dict() doesn't include 'kind' field.
        logger: Logger instance
    """
    edges = []
    try:
        spec = resource.get("spec", {})
        namespace = resource.get("metadata", {}).get("namespace", "default")
        resource_name = resource.get("metadata", {}).get("name", "")

        # Get volumes - handle different resource types
        # For Pods: spec.volumes
        # For Deployments/StatefulSets/etc: spec.template.spec.volumes
        volumes = spec.get("volumes", [])
        if not volumes and "template" in spec:
            template_spec = spec.get("template", {}).get("spec", {})
            volumes = template_spec.get("volumes", [])

        for volume in volumes:
            source_id = generate_node_id(
                cluster, namespace, resource_type, resource_name
            )

            # Check for PVC references (handle both camelCase and snake_case)
            pvc_ref = volume.get("persistentVolumeClaim") or volume.get(
                "persistent_volume_claim"
            )
            if pvc_ref:
                pvc_name = pvc_ref.get("claimName") or pvc_ref.get("claim_name", "")
                if pvc_name:
                    target_id = generate_node_id(
                        cluster, namespace, "persistentvolumeclaim", pvc_name
                    )
                    edges.append(
                        {
                            "source": source_id,
                            "target": target_id,
                            "relationship": "mounts",
                            "weight": calculate_dependency_weight(
                                resource_type, "persistentvolumeclaim", "mounts"
                            ),
                        }
                    )

            # Check for ConfigMap references (handle both camelCase and snake_case)
            cm_ref = volume.get("configMap") or volume.get("config_map")
            if cm_ref:
                cm_name = cm_ref.get("name", "")
                if cm_name:
                    target_id = generate_node_id(
                        cluster, namespace, "configmap", cm_name
                    )
                    edges.append(
                        {
                            "source": source_id,
                            "target": target_id,
                            "relationship": "mounts",
                            "weight": calculate_dependency_weight(
                                resource_type, "configmap", "mounts"
                            ),
                        }
                    )

            # Check for Secret references
            secret_ref = volume.get("secret")
            if secret_ref:
                secret_name = secret_ref.get("secretName") or secret_ref.get(
                    "secret_name", ""
                )
                if secret_name:
                    target_id = generate_node_id(
                        cluster, namespace, "secret", secret_name
                    )
                    edges.append(
                        {
                            "source": source_id,
                            "target": target_id,
                            "relationship": "mounts",
                            "weight": calculate_dependency_weight(
                                resource_type, "secret", "mounts"
                            ),
                        }
                    )

            # Check for projected volumes (can include ConfigMaps and Secrets)
            projected = volume.get("projected")
            if projected:
                for source in projected.get("sources", []):
                    if "configMap" in source or "config_map" in source:
                        cm_src = source.get("configMap") or source.get("config_map", {})
                        cm_name = cm_src.get("name", "")
                        if cm_name:
                            target_id = generate_node_id(
                                cluster, namespace, "configmap", cm_name
                            )
                            edges.append(
                                {
                                    "source": source_id,
                                    "target": target_id,
                                    "relationship": "mounts",
                                    "weight": calculate_dependency_weight(
                                        resource_type, "configmap", "mounts"
                                    ),
                                }
                            )
                    if "secret" in source:
                        secret_src = source.get("secret", {})
                        secret_name = secret_src.get("name", "")
                        if secret_name:
                            target_id = generate_node_id(
                                cluster, namespace, "secret", secret_name
                            )
                            edges.append(
                                {
                                    "source": source_id,
                                    "target": target_id,
                                    "relationship": "mounts",
                                    "weight": calculate_dependency_weight(
                                        resource_type, "secret", "mounts"
                                    ),
                                }
                            )

    except Exception as e:
        logger.debug(f"Error analyzing volume dependencies: {e}")

    return edges


# ============================================================================
# SIMULATION AFFECTED COMPONENTS ANALYSIS
# ============================================================================


# ============================================================================
# PERMISSION-AWARE RESOURCE FETCHING
# ============================================================================


def handle_resource_fetch_error(
    e: Exception,
    resource_type: str,
    namespace: str,
    skip_on_permission_denied: bool,
    logger,
) -> Dict[str, Any]:
    """
    Handle errors during resource fetching with permission-aware logic.

    Returns:
        Dict with 'success', 'permission_denied', 'error_message' keys
    """
    from kubernetes.client.rest import ApiException

    result = {"success": False, "permission_denied": False, "error_message": str(e)}

    if isinstance(e, ApiException):
        if e.status == 403:
            # Permission denied
            result["permission_denied"] = True
            logger.info(
                f"Permission denied for {resource_type} in namespace {namespace} (403 Forbidden)"
            )

            if skip_on_permission_denied:
                logger.debug(
                    f"Skipping {resource_type} due to permission denied (skip_on_permission_denied=True)"
                )
            else:
                logger.warning(
                    f"Permission denied for {resource_type} in namespace {namespace}"
                )
        elif e.status == 404:
            # Resource type not found (e.g., Tekton not installed)
            logger.debug(
                f"Resource type {resource_type} not found in namespace {namespace} (404)"
            )
        else:
            # Other API error
            logger.warning(
                f"API error fetching {resource_type} in namespace {namespace}: {e.status} - {e.reason}"
            )
    else:
        # Non-API exception
        logger.error(
            f"Unexpected error fetching {resource_type} in namespace {namespace}: {e}"
        )

    return result


async def identify_affected_components(
    changes: Dict[str, Any],
    scope: Dict[str, Any],
    scenario_type: str,
    k8s_core_api,
    k8s_apps_api,
    list_pods,
    list_namespaces,
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
                            "details": f"Deployment with {deployment.status.replicas} replicas",
                        }

                        # Check if this deployment matches any change criteria
                        for change_key, change_value in changes.items():
                            if change_key.lower() in deployment.metadata.name.lower():
                                component_info["severity"] = "high"
                                component_info["details"] += (
                                    f" - directly affected by {change_key} changes"
                                )
                                break

                        affected_components.append(component_info)

                elif scenario_type == "resource_limits":
                    # Identify pods/containers that could be affected by resource limit changes
                    # list_pods requires (namespace, k8s_core_api, logger)
                    pods = await list_pods(namespace, k8s_core_api, logger)
                    for pod in pods[:10]:  # Limit pods to prevent timeout
                        if not pod.get("error"):
                            component_info = {
                                "component": f"pod/{pod['name']}",
                                "namespace": namespace,
                                "impact_type": "resource_limits",
                                "severity": "medium",
                                "details": f"Pod with {len(pod.get('containers', []))} containers",
                            }

                            # Check for resource-constrained containers
                            containers = pod.get("containers", [])
                            constrained_containers = 0
                            for container in containers:
                                if container.get("state") in ["Waiting", "Terminated"]:
                                    constrained_containers += 1

                            if constrained_containers > 0:
                                component_info["severity"] = "high"
                                component_info["details"] += (
                                    f" - {constrained_containers} containers show resource constraints"
                                )

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
                            "details": f"Service with {len(service.spec.ports or [])} ports",
                        }

                        # Higher severity for services with many endpoints
                        if service.spec.ports and len(service.spec.ports) > 3:
                            component_info["severity"] = "medium"

                        affected_components.append(component_info)

            except ApiException as e:
                logger.warning(
                    f"Error analyzing components in namespace {namespace}: {e}"
                )
                continue

        # Limit the number of components returned
        return affected_components[:20]

    except Exception as e:
        logger.error(f"Error identifying affected components: {e}")
        return [
            {
                "component": "unknown",
                "impact_type": "error",
                "severity": "unknown",
                "details": str(e),
            }
        ]


# ============================================================================
# TOPOLOGY OUTPUT FORMAT CONVERTERS
# ============================================================================


def convert_to_graphviz(
    nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]
) -> str:
    """Convert topology to Graphviz DOT format."""
    dot = ["digraph topology {"]
    dot.append("  rankdir=LR;")
    dot.append("  node [shape=box];")

    # Add nodes with labels
    for node in nodes:
        node_type = node.get("type", "unknown")
        name = node.get("name", "unknown")
        namespace = node.get("namespace", "default")
        label = f"{namespace}\\n{name}\\n({node_type})"
        dot.append(f'  "{node["id"]}" [label="{label}"];')

    # Add edges
    for edge in edges:
        relationship = edge.get("relationship", "")
        dot.append(
            f'  "{edge["source"]}" -> "{edge["target"]}" [label="{relationship}"];'
        )

    dot.append("}")
    return "\n".join(dot)


def convert_to_mermaid(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> str:
    """Convert topology to Mermaid diagram format."""
    mermaid = ["graph LR"]

    # Add nodes
    for node in nodes:
        node_type = node.get("type", "unknown")
        name = node.get("name", "unknown")
        label = f"{name}<br/>({node_type})"
        # Sanitize node ID for mermaid
        node_id = node["id"].replace(":", "_").replace("/", "_")
        mermaid.append(f'  {node_id}["{label}"]')

    # Add edges
    for edge in edges:
        source_id = edge["source"].replace(":", "_").replace("/", "_")
        target_id = edge["target"].replace(":", "_").replace("/", "_")
        relationship = edge.get("relationship", "")
        mermaid.append(f"  {source_id} -->|{relationship}| {target_id}")

    return "\n".join(mermaid)
