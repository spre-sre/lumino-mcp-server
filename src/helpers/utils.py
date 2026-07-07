"""
Utility functions for LUMINO MCP Server.

This module contains common utility functions used across multiple tools.
"""

import re
import json
import yaml
import base64
import asyncio
import logging
import hashlib
import statistics
from datetime import datetime, timedelta
from typing import List, Any, Dict, Optional

# Optional cryptography imports for certificate parsing
try:
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend
    from cryptography.x509.oid import NameOID, ExtensionOID
except ImportError:
    x509 = None
    default_backend = None
    NameOID = None
    ExtensionOID = None

logger = logging.getLogger("lumino-mcp")


def calculate_duration(start_time, end_time, use_current_if_missing: bool = False) -> str:
    """
    Calculate duration between two timestamps.

    Args:
        start_time: Start timestamp (string or datetime)
        end_time: End timestamp (string or datetime), can be None for running tasks
        use_current_if_missing: If True and end_time is missing, use current time (for running pipelines)

    Returns:
        str: Human-readable duration string (e.g., "5.32 minutes", "1.25 hours")
             For running pipelines: "5.32 minutes (running)"
             Returns "unknown" if start_time is invalid or missing.
    """
    if not start_time or start_time == "unknown":
        return "unknown"

    is_running = False
    if not end_time or end_time == "unknown":
        if use_current_if_missing:
            end_time = datetime.now(tz=None)
            is_running = True
        else:
            return "unknown"

    try:
        if isinstance(start_time, str):
            start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        else:
            start = start_time

        if isinstance(end_time, str):
            end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        elif isinstance(end_time, datetime):
            # Make timezone-aware if start is timezone-aware
            if start.tzinfo is not None and end_time.tzinfo is None:
                from datetime import timezone

                end = end_time.replace(tzinfo=timezone.utc)
            else:
                end = end_time
        else:
            end = end_time

        duration = end - start
        seconds = duration.total_seconds()

        if seconds < 60:
            duration_str = f"{seconds:.2f} seconds"
        elif seconds < 3600:
            duration_str = f"{seconds / 60:.2f} minutes"
        elif seconds < 86400:
            duration_str = f"{seconds / 3600:.2f} hours"
        else:
            days = int(seconds // 86400)
            remaining_hours = (seconds % 86400) / 3600
            duration_str = f"{days}d {remaining_hours:.1f}h"

        if is_running:
            duration_str = f"{duration_str} (running)"

        return duration_str
    except Exception:
        return "unknown"


def calculate_duration_seconds(
    start_time, end_time, use_current_if_missing: bool = False
) -> Optional[int]:
    """
    Calculate duration between two timestamps in seconds.

    Args:
        start_time: Start timestamp (string or datetime)
        end_time: End timestamp (string or datetime), can be None for running tasks
        use_current_if_missing: If True and end_time is missing, use current time

    Returns:
        int: Duration in seconds, or None if timestamps are invalid.
    """
    if not start_time or start_time == "unknown":
        return None

    if not end_time or end_time == "unknown":
        if use_current_if_missing:
            end_time = datetime.now(tz=None)
        else:
            return None

    try:
        if isinstance(start_time, str):
            start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        else:
            start = start_time

        if isinstance(end_time, str):
            end = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        elif isinstance(end_time, datetime):
            if start.tzinfo is not None and end_time.tzinfo is None:
                from datetime import timezone

                end = end_time.replace(tzinfo=timezone.utc)
            else:
                end = end_time
        else:
            end = end_time

        duration = end - start
        return int(duration.total_seconds())
    except Exception:
        return None


def parse_time_period(time_period: str) -> timedelta:
    """Parse time period string like '1h', '30m', '2d' into timedelta object."""

    pattern = r"^(\d+)([smhd])$"
    match = re.match(pattern, time_period.lower())

    if not match:
        raise ValueError(
            f"Invalid time period format: {time_period}. "
            "Expected format: number followed by s/m/h/d (e.g., '1h', '30m', '2d')"
        )

    value, unit = int(match.group(1)), match.group(2)

    if unit == "s":
        return timedelta(seconds=value)
    elif unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    else:
        raise ValueError(f"Unsupported time unit: {unit}")


def parse_time_parameters(
    since_seconds: Optional[int] = None,
    time_period: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Parse various time parameter formats into log retrieval parameters.

    Args:
        since_seconds: Legacy parameter - seconds from now
        time_period: Human-readable period like "1h", "30m", "2d", "1w"
        start_time: ISO timestamp "2024-01-15T10:30:00Z"
        end_time: ISO timestamp "2024-01-15T14:30:00Z"

    Returns:
        Dict with 'log_params' and 'time_info' for log retrieval
    """

    log_params = {}
    time_info = {}

    # Priority order: since_seconds > start_time/end_time > time_period
    if since_seconds is not None:
        log_params["since_seconds"] = since_seconds
        time_info["method"] = "since_seconds"
        time_info["value"] = since_seconds

    elif start_time is not None or end_time is not None:
        # Handle specific timestamps
        if start_time:
            try:
                # Ensure start_time is a string
                if not isinstance(start_time, str):
                    raise ValueError(
                        f"start_time must be a string, got {type(start_time).__name__}: {start_time}"
                    )
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                # Convert to seconds since epoch for kubectl
                # Use timezone-aware datetime.now() to match start_dt
                from datetime import timezone

                now_dt = datetime.now(timezone.utc)

                # Calculate time difference - if start_time is in the past, get logs since then
                time_diff = (now_dt - start_dt).total_seconds()
                if time_diff > 0:
                    # start_time is in the past, get logs from then until now
                    log_params["since_seconds"] = int(time_diff)
                    time_info["method"] = "start_time"
                    time_info["start_time"] = start_time
                    time_info["calculated_since_seconds"] = int(time_diff)
                    if end_time:
                        time_info["end_time"] = end_time
                else:
                    # start_time is in the future, which doesn't make sense for log retrieval
                    # Fall back to default behavior
                    time_info["method"] = "start_time_future_fallback"
                    time_info["warning"] = (
                        f"start_time {start_time} is in the future, using default time range"
                    )
            except ValueError:
                raise ValueError(
                    f"Invalid start_time format: {start_time}. Use ISO format like '2024-01-15T10:30:00Z'"
                )

    elif time_period is not None:
        # Parse human-readable time periods
        try:
            time_delta = parse_time_period(time_period)
            since_seconds_calc = int(time_delta.total_seconds())
            log_params["since_seconds"] = since_seconds_calc
            time_info["method"] = "time_period"
            time_info["period"] = time_period
            time_info["calculated_seconds"] = since_seconds_calc
        except ValueError as e:
            raise ValueError(f"Invalid time_period: {e}")

    # Default fallback if no time parameters provided
    if not log_params:
        default_seconds = 3600  # 1 hour default
        log_params["since_seconds"] = default_seconds
        time_info["method"] = "default"
        time_info["value"] = default_seconds

    return {"log_params": log_params, "time_info": time_info}


def _strip_none_values(obj):
    """Recursively strip None values and empty dicts/lists from a nested dict for cleaner output."""
    if isinstance(obj, dict):
        return {
            k: _strip_none_values(v)
            for k, v in obj.items()
            if v is not None and _strip_none_values(v) not in (None, {}, [])
        }
    elif isinstance(obj, list):
        return [_strip_none_values(item) for item in obj if item is not None]
    return obj


def format_yaml_output(resource_obj: Any, resource_type: str, name: str, namespace: str) -> str:
    """Format resource as YAML output."""
    try:
        if hasattr(resource_obj, "to_dict"):
            resource_dict = resource_obj.to_dict()
        else:
            resource_dict = resource_obj

        yaml_output = yaml.dump(resource_dict, default_flow_style=False, indent=2)
        return f"# {resource_type.title()} '{name}' in namespace '{namespace}'\n\n{yaml_output}"
    except Exception as e:
        return f"Error formatting YAML: {str(e)}"


def format_detailed_output(resource_obj: Any, resource_type: str, name: str, namespace: str) -> str:
    """Format resource with detailed information."""
    try:
        if hasattr(resource_obj, "to_dict"):
            resource_dict = resource_obj.to_dict()
        else:
            resource_dict = resource_obj

        output = [f"=== {resource_type.upper()} DETAILS ==="]
        output.append(f"Name: {name}")
        output.append(f"Namespace: {namespace}")

        # Metadata
        metadata = resource_dict.get("metadata", {})
        if metadata:
            output.append("\n--- METADATA ---")
            output.append(f"UID: {metadata.get('uid', 'N/A')}")
            output.append(f"Resource Version: {metadata.get('resource_version', 'N/A')}")

            # Creation timestamp
            created = metadata.get("creation_timestamp")
            if created:
                if isinstance(created, str):
                    output.append(f"Created: {created}")
                else:
                    output.append(f"Created: {created.isoformat()}")

            # Labels
            labels = metadata.get("labels", {})
            if labels:
                output.append("\nLabels:")
                for key, value in labels.items():
                    output.append(f"  {key}: {value}")

            # Annotations
            annotations = metadata.get("annotations", {})
            if annotations:
                output.append("\nAnnotations:")
                for key, value in annotations.items():
                    output.append(f"  {key}: {value}")

        # Spec
        spec = resource_dict.get("spec", {})
        if spec:
            output.append("\n--- SPECIFICATION ---")
            output.append(json.dumps(_strip_none_values(spec), indent=2, default=str))

        # Status
        status = resource_dict.get("status", {})
        if status:
            output.append("\n--- STATUS ---")
            output.append(json.dumps(_strip_none_values(status), indent=2, default=str))

        return "\n".join(output)

    except Exception as e:
        return f"Error formatting detailed output: {str(e)}"


def format_summary_output(resource_obj: Any, resource_type: str, name: str, namespace: str) -> str:
    """Format resource with summary information."""
    try:
        if hasattr(resource_obj, "to_dict"):
            resource_dict = resource_obj.to_dict()
        else:
            resource_dict = resource_obj

        cluster_scoped_types = [
            "node",
            "namespace",
            "persistentvolume",
            "pv",
            "storageclass",
            "clustertask",
        ]
        output = [f"=== {resource_type.upper()} SUMMARY ==="]
        output.append(f"Name: {name}")
        if resource_type not in cluster_scoped_types:
            output.append(f"Namespace: {namespace}")

        metadata = resource_dict.get("metadata", {})

        # Creation time
        created = metadata.get("creation_timestamp")
        if created:
            if isinstance(created, str):
                output.append(f"Created: {created}")
            else:
                output.append(f"Created: {created.isoformat()}")

        # Labels (limited)
        labels = metadata.get("labels", {})
        if labels:
            label_summary = ", ".join([f"{k}={v}" for k, v in list(labels.items())[:3]])
            if len(labels) > 3:
                label_summary += f" (and {len(labels) - 3} more)"
            output.append(f"Labels: {label_summary}")

        # Resource-specific summary
        spec = resource_dict.get("spec", {})
        status = resource_dict.get("status", {})

        if resource_type == "deployment":
            replicas = spec.get("replicas", 0)
            ready_replicas = status.get("ready_replicas", 0)
            output.append(f"Replicas: {ready_replicas}/{replicas}")

        elif resource_type == "pod":
            phase = status.get("phase", "Unknown")
            output.append(f"Phase: {phase}")
            containers = spec.get("containers", [])
            output.append(f"Containers: {len(containers)}")

        elif resource_type == "service":
            service_type = spec.get("type", "ClusterIP")
            cluster_ip = spec.get("cluster_ip")
            output.append(f"Type: {service_type}")
            if cluster_ip:
                output.append(f"Cluster IP: {cluster_ip}")

        elif resource_type in ["pipelinerun", "taskrun"]:
            # Tekton-specific summary
            conditions = status.get("conditions", [])
            if conditions:
                latest_condition = conditions[-1]
                condition_type = latest_condition.get("type", "Unknown")
                condition_status = latest_condition.get("status", "Unknown")
                output.append(f"Status: {condition_type} - {condition_status}")

            start_time = status.get("start_time")
            completion_time = status.get("completion_time")
            if start_time:
                output.append(f"Started: {start_time}")
            if completion_time:
                output.append(f"Completed: {completion_time}")

        elif resource_type == "node":
            # Node-specific summary
            capacity = status.get("capacity", {})
            allocatable = status.get("allocatable", {})
            if capacity:
                output.append(
                    f"CPU: {allocatable.get('cpu', '?')}/{capacity.get('cpu', '?')} (allocatable/capacity)"
                )
                output.append(
                    f"Memory: {allocatable.get('memory', '?')}/{capacity.get('memory', '?')}"
                )
                output.append(f"Pods: {allocatable.get('pods', '?')}/{capacity.get('pods', '?')}")
            node_info = status.get("node_info", {})
            if node_info:
                output.append(f"Kubelet: {node_info.get('kubelet_version', 'unknown')}")
                output.append(f"OS: {node_info.get('os_image', 'unknown')}")
                output.append(
                    f"Container Runtime: {node_info.get('container_runtime_version', 'unknown')}"
                )
            # Show roles from labels
            roles = [
                k.replace("node-role.kubernetes.io/", "")
                for k in labels
                if k.startswith("node-role.kubernetes.io/")
            ]
            if roles:
                output.append(f"Roles: {', '.join(roles)}")
            # Taints
            taints = spec.get("taints", [])
            if taints:
                taint_strs = [
                    f"{t.get('key', '?')}={t.get('value', '')}:{t.get('effect', '?')}"
                    for t in taints[:3]
                ]
                output.append(f"Taints: {', '.join(taint_strs)}")

        elif resource_type in ["pipeline", "task"]:
            # Tekton pipeline/task summary
            params = spec.get("params", [])
            if params:
                output.append(f"Parameters: {len(params)}")

            if resource_type == "pipeline":
                tasks = spec.get("tasks", [])
                output.append(f"Tasks: {len(tasks)}")
            else:  # task
                steps = spec.get("steps", [])
                output.append(f"Steps: {len(steps)}")

        # Add any important status conditions
        if status.get("conditions"):
            conditions = status["conditions"]
            if isinstance(conditions, list) and conditions:
                latest = conditions[-1]
                cond_type = latest.get("type", "Unknown")
                cond_status = latest.get("status", "Unknown")
                if cond_type not in ["Ready"] or resource_type not in ["deployment"]:
                    output.append(f"Condition: {cond_type}={cond_status}")

        return "\n".join(output)

    except Exception as e:
        return f"Error formatting summary: {str(e)}"


def calculate_context_tokens(text: str) -> int:
    """
    Estimate token count for text (conservative approximation).

    Uses a simple heuristic: 1 token ≈ 3 characters.
    This is intentionally conservative to avoid exceeding limits.
    """
    return len(text) // 3


async def get_all_pod_logs(
    pod_name: str,
    namespace: str,
    k8s_core_api,
    tail_lines: Optional[int] = None,
    since_seconds: Optional[int] = None,
    since_time: Optional[str] = None,
    timestamps: bool = True,
    previous: bool = False,
) -> Dict[str, str]:
    """
    Reads logs from all containers in a specified pod with optional filtering.

    Args:
        pod_name: The name of the pod.
        namespace: The namespace of the pod.
        k8s_core_api: Kubernetes Core API client
        tail_lines: Number of lines to retrieve from the end of logs.
        since_seconds: Retrieve logs newer than this many seconds.
        since_time: Retrieve logs newer than this RFC3339 timestamp.
        timestamps: Include timestamps in log output.
        previous: Retrieve logs from previous container instance.

    Returns:
        Dictionary where keys are container names and values are their logs.
    """
    container_logs = {}

    try:
        # Get the pod object to find its containers
        pod = await asyncio.to_thread(
            k8s_core_api.read_namespaced_pod, name=pod_name, namespace=namespace
        )

        # Check if pod has containers
        if not pod.spec.containers:
            logger.warning(f"Pod {pod_name} has no containers defined")
            return {"no_containers": "Pod has no containers defined"}

        # Get the names of all containers in the pod
        container_names = [container.name for container in pod.spec.containers]
        logger.debug(
            f"Found {len(container_names)} containers in pod {pod_name}: {container_names}"
        )

        # Build log parameters
        log_params = {
            "name": pod_name,
            "namespace": namespace,
            "container": None,  # Will be set per container
            "timestamps": timestamps,
            "previous": previous,
        }

        # Add optional time/line filtering parameters
        # Note: Kubernetes Python client does NOT support since_time parameter
        # (see https://github.com/kubernetes-client/python/issues/1351)
        # We must convert since_time to since_seconds
        if since_time:
            try:
                # Parse RFC3339 timestamp and convert to seconds from now
                from datetime import timezone

                since_dt = datetime.fromisoformat(since_time.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                delta = now - since_dt
                computed_since_seconds = max(1, int(delta.total_seconds()))
                log_params["since_seconds"] = computed_since_seconds
                logger.debug(
                    f"Converted since_time '{since_time}' to since_seconds={computed_since_seconds}"
                )
            except Exception as e:
                logger.warning(f"Failed to parse since_time '{since_time}': {e}, ignoring filter")
        elif since_seconds:
            log_params["since_seconds"] = since_seconds
        elif tail_lines:
            log_params["tail_lines"] = tail_lines

        # Loop through each container and fetch its logs
        for container_name in container_names:
            try:
                # Set the container for this iteration
                log_params["container"] = container_name

                # Read the logs for the specific container
                logs = await asyncio.to_thread(k8s_core_api.read_namespaced_pod_log, **log_params)
                container_logs[container_name] = logs
            except Exception as e:
                if hasattr(e, "reason"):
                    logger.warning(
                        f"Error reading logs for container {container_name} in pod {pod_name}: {e}"
                    )
                    container_logs[container_name] = f"Error fetching logs: {e.reason}"
                else:
                    logger.warning(
                        f"Unexpected error fetching logs for container {container_name} in pod {pod_name}: {e}"
                    )
                    container_logs[container_name] = f"Unexpected error fetching logs: {str(e)}"

    except Exception as e:
        if hasattr(e, "reason"):
            logger.error(f"Error getting pod details for {pod_name}: {e}")
            return {"pod_error": f"Error getting pod details: {e.reason}"}
        else:
            logger.error(f"Unexpected error getting pod details for {pod_name}: {e}")
            return {"pod_error": f"Unexpected error getting pod details: {str(e)}"}

    # Ensure we always return something
    if not container_logs:
        return {"no_logs": "No logs found for any containers in this pod"}

    return container_logs


def clean_pipeline_logs(raw_logs: str) -> str:
    """
    Clean pipeline logs by removing escape characters, line continuation symbols,
    and properly formatting JSON log entries commonly found in CI/CD pipeline outputs.

    This function handles common issues with pipeline logs:
    1. Line continuation characters (│, ┌, └, etc.)
    2. Multiple levels of JSON escaping
    3. Escaped newlines and other characters
    4. Terminal formatting characters and ANSI codes

    Args:
        raw_logs: Raw log content from pipeline pods

    Returns:
        Cleaned and formatted log content
    """
    if not raw_logs or raw_logs.strip() == "":
        return raw_logs

    try:
        # Split logs into individual lines
        lines = raw_logs.strip().split("\n")
        cleaned_lines = []

        for line in lines:
            if not line:
                continue

            # Remove line continuation characters commonly found in pipeline logs
            cleaned_line = re.sub(r"[│┌└├┤┐┘┬┴┼─═║╒╓╔╕╖╗╘╙╚╛╜╝╞╟╠╡╢╣╤╥╦╧╨╩╪╫╬]", "", line)

            # Remove leading/trailing whitespace
            cleaned_line = cleaned_line.strip()

            if not cleaned_line:
                continue

            # Skip lines that are just separators or formatting
            if re.match(r"^[─═│┌└├┤┐┘┬┴┼\s]*$", cleaned_line):
                continue

            # Remove ANSI escape codes
            cleaned_line = re.sub(r"\x1b\[[0-9;]*m", "", cleaned_line)

            # Handle multiple levels of JSON escaping
            cleaned_line = cleaned_line.replace('\\\\"', '"')
            cleaned_line = cleaned_line.replace("\\n", "\n")
            cleaned_line = cleaned_line.replace("\\/", "/")
            cleaned_line = cleaned_line.replace("\\t", "\t")
            cleaned_line = cleaned_line.replace("\\r", "\r")

            # Try to identify and format JSON log entries
            try:
                json_match = re.search(r"\{.*\}", cleaned_line)
                if json_match:
                    json_part = json_match.group(0)
                    prefix = cleaned_line[: json_match.start()].strip()
                    suffix = cleaned_line[json_match.end() :].strip()

                    try:
                        json_obj = json.loads(json_part)

                        if isinstance(json_obj, dict):
                            # Check for Renovate/dependency bot logs
                            if "name" in json_obj and json_obj.get("name") == "renovate":
                                timestamp = json_obj.get("time", "")
                                level = json_obj.get("level", "info")
                                msg = json_obj.get("msg", "")
                                repository = json_obj.get("repository", "")

                                formatted_parts = []
                                if timestamp:
                                    formatted_parts.append(f"[{timestamp}]")
                                formatted_parts.append(f"[RENOVATE/{level.upper()}]")
                                if repository:
                                    formatted_parts.append(f"[{repository}]")
                                if msg:
                                    formatted_parts.append(msg)

                                for key, value in json_obj.items():
                                    if (
                                        key
                                        not in [
                                            "name",
                                            "time",
                                            "level",
                                            "msg",
                                            "repository",
                                            "hostname",
                                            "pid",
                                            "v",
                                        ]
                                        and value is not None
                                    ):
                                        if isinstance(value, (str, int, float, bool)):
                                            formatted_parts.append(f"{key}={value}")
                                        elif isinstance(value, dict) and len(str(value)) < 200:
                                            formatted_parts.append(
                                                f"{key}={json.dumps(value, separators=(',', ':'))}"
                                            )

                                formatted_line = " ".join(formatted_parts)
                            else:
                                formatted_json = json.dumps(
                                    json_obj, indent=2, separators=(",", ": ")
                                )
                                formatted_line = f"{prefix} {formatted_json} {suffix}".strip()
                        else:
                            formatted_json = json.dumps(json_obj, separators=(",", ":"))
                            formatted_line = f"{prefix} {formatted_json} {suffix}".strip()

                        cleaned_lines.append(formatted_line)

                    except json.JSONDecodeError:
                        if prefix or suffix:
                            cleaned_line = f"{prefix} {json_part} {suffix}".strip()
                        else:
                            cleaned_line = json_part
                        cleaned_lines.append(cleaned_line)
                else:
                    cleaned_lines.append(cleaned_line)

            except Exception as e:
                logger.debug(f"Failed to process pipeline log line: {e}")
                cleaned_lines.append(cleaned_line)

        # Join the cleaned lines
        result = "\n".join(cleaned_lines)

        # Final cleanup - remove excessive whitespace and empty lines
        result = re.sub(r"\n\s*\n", "\n", result)
        result = re.sub(r" +", " ", result)

        return result.strip()

    except Exception as e:
        logger.error(f"Error cleaning pipeline logs: {e}")
        return raw_logs


def calculate_utilization(used: str, limit: str) -> float:
    """
    Calculate the utilization percentage of a resource.

    Handles CPU values (e.g., "200m", "0.5"), memory values
    (e.g., "256Mi", "1Gi", "512M"), and count values (e.g., "2k", "100").

    Args:
        used: Current resource usage as string
        limit: Resource limit as string

    Returns:
        Utilization percentage (0-100+)
    """
    try:

        def parse_cpu(value: str) -> float:
            if value.endswith("m"):
                return float(value[:-1]) / 1000.0
            return float(value)

        def parse_quantity(value: str) -> float:
            """Parse Kubernetes quantity values including memory, count, and SI suffixes."""
            # Handle binary (Ki, Mi, Gi, Ti) and decimal (k, K, M, G, T) suffixes
            # Note: lowercase 'k' is commonly used for count quotas (e.g., "2k" = 2000)
            units = {
                # Binary suffixes (IEC)
                "Ki": 2**10,
                "Mi": 2**20,
                "Gi": 2**30,
                "Ti": 2**40,
                "Pi": 2**50,
                "Ei": 2**60,
                # Decimal suffixes (SI) - uppercase
                "K": 10**3,
                "M": 10**6,
                "G": 10**9,
                "T": 10**12,
                "P": 10**15,
                "E": 10**18,
                # Lowercase 'k' for count-based quotas
                "k": 10**3,
            }

            for suffix, multiplier in units.items():
                if value.endswith(suffix):
                    return float(value[: -len(suffix)]) * multiplier

            return float(value)

        is_cpu = (
            used.endswith("m")
            or limit.endswith("m")
            or (used.count(".") > 0 and used[-1].isdigit())
        )

        if is_cpu:
            used_value = parse_cpu(used)
            limit_value = parse_cpu(limit)
        else:
            used_value = parse_quantity(used)
            limit_value = parse_quantity(limit)

        if limit_value == 0:
            return 0

        return (used_value / limit_value) * 100
    except Exception:
        return 0


async def list_pods(namespace: str, k8s_core_api, log: logging.Logger) -> List[Dict[str, Any]]:
    """
    List pods in a specific namespace with relevant details.

    Args:
        namespace: Kubernetes namespace
        k8s_core_api: Kubernetes Core API client
        log: Logger instance

    Returns:
        List of pod information dictionaries
    """
    from kubernetes.client.rest import ApiException

    try:
        pods = k8s_core_api.list_namespaced_pod(namespace)
        result = []

        for pod in pods.items:
            container_statuses = []
            if pod.status.container_statuses:
                for container in pod.status.container_statuses:
                    container_status = {
                        "name": container.name,
                        "ready": container.ready,
                        "restart_count": container.restart_count,
                    }

                    if container.state.running:
                        container_status["state"] = "Running"
                        container_status["started_at"] = container.state.running.started_at
                    elif container.state.waiting:
                        container_status["state"] = "Waiting"
                        container_status["reason"] = container.state.waiting.reason
                    elif container.state.terminated:
                        container_status["state"] = "Terminated"
                        container_status["exit_code"] = container.state.terminated.exit_code
                        container_status["reason"] = container.state.terminated.reason

                    container_statuses.append(container_status)

            result.append(
                {
                    "name": pod.metadata.name,
                    "status": pod.status.phase,
                    "node": pod.spec.node_name if pod.spec.node_name else "Unknown",
                    "ip": pod.status.pod_ip if pod.status.pod_ip else "Unknown",
                    "start_time": pod.status.start_time if pod.status.start_time else "Unknown",
                    "containers": container_statuses,
                    "labels": pod.metadata.labels,
                }
            )

        return result
    except ApiException as e:
        log.error(f"Error listing pods in namespace {namespace}: {e}")
        return [{"error": str(e)}]


def detect_anomalies_in_data(data_points: List[float], original_data: List[Any]) -> Dict[str, Any]:
    """
    Detect anomalies in numeric data using statistical methods (z-score).

    Uses z-score analysis with a threshold of 2.5 standard deviations to identify
    outliers in the provided data points.

    Args:
        data_points: List of numeric values to analyze
        original_data: Original data objects corresponding to each data point

    Returns:
        Dictionary containing:
            - anomalies_detected: Boolean indicating if anomalies were found
            - anomaly_details: Details about anomalies if found
            - message: Descriptive message about the result
    """
    if len(data_points) < 5:  # Need sufficient data
        return {
            "anomalies_detected": False,
            "anomaly_details": None,
            "message": "Insufficient data for anomaly detection",
        }

    try:
        # Calculate statistical measures
        mean_val = statistics.mean(data_points)
        std_dev = statistics.stdev(data_points) if len(data_points) > 1 else 0

        if std_dev == 0:
            return {
                "anomalies_detected": False,
                "anomaly_details": None,
                "message": "No variance in data - all values are identical",
            }

        # Identify outliers using z-score method (threshold: 2.5 standard deviations)
        anomalies = []
        threshold = 2.5

        for i, value in enumerate(data_points):
            z_score = abs(value - mean_val) / std_dev
            if z_score > threshold:
                anomalies.append(
                    {
                        "index": i,
                        "value": value,
                        "z_score": z_score,
                        "original_data": original_data[i] if i < len(original_data) else None,
                    }
                )

        if anomalies:
            return {
                "anomalies_detected": True,
                "anomaly_details": {
                    "count": len(anomalies),
                    "anomalies": anomalies,
                    "statistics": {"mean": mean_val, "std_dev": std_dev, "threshold": threshold},
                },
                "message": f"Found {len(anomalies)} anomalies using z-score analysis",
            }
        else:
            return {
                "anomalies_detected": False,
                "anomaly_details": None,
                "message": "No significant anomalies detected",
            }

    except Exception as e:
        return {
            "anomalies_detected": False,
            "anomaly_details": None,
            "message": f"Error in anomaly detection: {str(e)}",
        }


# ============================================================================
# LOG ANALYSIS HELPERS
# ============================================================================


def _normalize_log_newlines(log_text: str) -> str:
    """Normalize log text by converting literal backslash-n sequences to actual newlines.

    When log text is passed through JSON/MCP boundaries, actual newlines may arrive
    as literal two-character '\\n' sequences. This normalizes them so line splitting works.
    """
    # Replace literal \n (two chars: backslash + n) with actual newline,
    # but only when they appear between log lines (not inside JSON strings).
    # Heuristic: literal \n followed by a timestamp pattern or log-level keyword.
    import re

    # Replace literal \n that precede a timestamp (YYYY-MM-DD) or log-level prefix
    normalized = re.sub(
        r'\\n(?=\d{4}-\d{2}-\d{2}|level=|"level"|time=|"ts"|msg=|http:)', "\n", log_text
    )
    # Also handle remaining literal \n as a fallback if no actual newlines are present
    if "\n" not in normalized and "\\n" in normalized:
        normalized = normalized.replace("\\n", "\n")
    return normalized


def extract_error_patterns(log_text: str) -> List[str]:
    """
    Extract common error patterns from log text including Kubernetes-specific errors.

    Args:
        log_text: Raw log content to analyze

    Returns:
        List of error lines found in the logs (max 15)
    """
    if not log_text or log_text == "No pod logs available":
        return []

    # Normalize escaped newlines from MCP/JSON transport
    log_text = _normalize_log_newlines(log_text)

    # Common error patterns to look for (case-insensitive)
    patterns = [
        # General error indicators
        "Error:",
        "Exception:",
        "Failed:",
        "fatal:",
        "panic:",
        "cannot",
        "unable to",
        "failed to",
        "error",
        "invalid",
        "No such file",
        "Permission denied",
        "Out of memory",
        "Connection refused",
        "timed out",
        # Kubernetes-specific errors
        "OOMKilled",
        "CrashLoopBackOff",
        "ImagePullBackOff",
        "ErrImagePull",
        "CreateContainerConfigError",
        "CreateContainerError",
        "FailedMount",
        "FailedAttachVolume",
        "FailedScheduling",
        "Unschedulable",
        "BackOff",
        "Evicted",
        # Container runtime errors
        "container killed",
        "container exited",
        "restart count",
        "liveness probe failed",
        "readiness probe failed",
        # Network errors
        "dial tcp",
        "no route to host",
        "connection reset",
        # Resource errors
        "quota exceeded",
        "limit exceeded",
        "insufficient",
    ]

    # Find lines containing these patterns
    error_lines = []
    for line in log_text.split("\n"):
        line = line.strip()
        if any(pattern.lower() in line.lower() for pattern in patterns) and len(line) > 10:
            # Limit to a reasonable length for readability
            if len(line) > 200:
                line = line[:197] + "..."
            error_lines.append(line)

    # Return a limited number of most relevant error lines
    return error_lines[:15]


def categorize_errors(log_text: str, error_patterns: List[str]) -> Dict[str, int]:
    """
    Categorize errors into common types including Kubernetes-specific errors.

    Args:
        log_text: Raw log content
        error_patterns: List of extracted error patterns

    Returns:
        Dictionary mapping error categories to occurrence counts
    """
    categories = {
        # Kubernetes-specific categories
        "oom": [
            "oomkilled",
            "oom killed",
            "out of memory",
            "memory limit exceeded",
            "exceeded memory",
        ],
        "crash": ["crashloopbackoff", "crash loop", "container crashed", "backoff restarting"],
        "image": ["imagepullbackoff", "errimagepull", "image pull", "pull image", "registry"],
        "scheduling": ["unschedulable", "failedscheduling", "insufficient", "node affinity"],
        "storage": ["failedmount", "volume mount", "pvc", "persistent volume", "mount failed"],
        "config": [
            "createcontainerconfigerror",
            "configmap",
            "secret not found",
            "missing key",
            "invalid config",
        ],
        # General categories
        "resource_limits": [
            "memory limit",
            "cpu limit",
            "resource limit",
            "resource quota",
            "evicted",
        ],
        "network": [
            "timeout",
            "connection refused",
            "connection reset",
            "unreachable",
            "dns lookup",
            "dial tcp",
        ],
        "permissions": ["access denied", "permission denied", "forbidden", "unauthorized", "rbac"],
        "configuration": ["invalid configuration", "missing parameter", "environment variable"],
        "dependency": ["not found", "missing dependency", "version mismatch", "incompatible"],
        "filesystem": [
            "no such file",
            "directory not found",
            "file not found",
            "read-only filesystem",
        ],
    }

    counts = {category: 0 for category in categories.keys()}

    # Combined text from logs and error patterns
    combined_text = log_text.lower() + " " + " ".join(error_patterns).lower()

    # Count occurrences
    for category, terms in categories.items():
        for term in terms:
            counts[category] += combined_text.count(term.lower())

    # Filter out categories with no matches
    return {category: count for category, count in counts.items() if count > 0}


def generate_log_summary(
    log_text: str, error_patterns: List[str], error_categories: Dict[str, int]
) -> str:
    """
    Generate a concise summary of log analysis.

    Args:
        log_text: Raw log content
        error_patterns: List of extracted error patterns
        error_categories: Dictionary of categorized errors

    Returns:
        Human-readable summary string
    """
    if not log_text or log_text == "No pod logs available":
        return "No logs available to analyze."

    # Normalize escaped newlines from MCP/JSON transport
    log_text = _normalize_log_newlines(log_text)

    # Count total lines in log
    total_lines = len(log_text.split("\n"))

    # Get first and last timestamp if available
    first_timestamp = None
    last_timestamp = None
    for line in log_text.split("\n"):
        if len(line.strip()) > 0:
            timestamps = re.findall(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", line)
            if timestamps:
                if not first_timestamp:
                    first_timestamp = timestamps[0]
                last_timestamp = timestamps[0]

    # Build summary
    summary = []
    summary.append(f"Log contains {total_lines} lines.")

    if first_timestamp and last_timestamp:
        summary.append(f"Time span: {first_timestamp} to {last_timestamp}")

    # Add error summary
    error_count = len(error_patterns)
    if error_count > 0:
        summary.append(f"Found {error_count} potential errors.")

        # Add category breakdown
        if error_categories:
            summary.append("Error categories:")
            for category, count in sorted(
                error_categories.items(), key=lambda x: x[1], reverse=True
            ):
                summary.append(f"  - {category.replace('_', ' ').title()}: {count}")
    else:
        summary.append("No significant errors detected.")

    # List a few example errors
    if error_patterns:
        summary.append("\nExample errors:")
        for i, error in enumerate(error_patterns[:3]):  # Show top 3 errors
            summary.append(f"  {i + 1}. {error}")
        if len(error_patterns) > 3:
            summary.append(f"  ... and {len(error_patterns) - 3} more")

    return "\n".join(summary)


# ============================================================================
# PIPELINE ANALYSIS HELPERS
# ============================================================================


def determine_root_cause(analysis_results: Dict[str, Any]) -> str:
    """
    Determine most likely root cause based on analysis results.

    Args:
        analysis_results: Dictionary containing failed task analysis

    Returns:
        Human-readable root cause description
    """
    if "failed_tasks" not in analysis_results or not analysis_results["failed_tasks"]:
        return "Unknown - No failed tasks identified"

    # Combine error categories across all failed tasks
    all_categories: Dict[str, int] = {}
    for task in analysis_results["failed_tasks"]:
        for category, count in task.get("error_categories", {}).items():
            all_categories[category] = all_categories.get(category, 0) + count

    # Find the most common category
    if all_categories:
        most_common = max(all_categories.items(), key=lambda x: x[1])
        category = most_common[0]

        # Map category to more specific root causes
        # Kubernetes-specific categories
        if category == "oom":
            return "Out of Memory (OOMKilled) - container exceeded memory limits and was killed"
        elif category == "crash":
            return "Container crash loop (CrashLoopBackOff) - container is repeatedly crashing on startup"
        elif category == "image":
            # Verify this is an actual image pull failure, not just a log mentioning "image"
            # Check if error_patterns contain definitive image pull indicators
            actual_errors = []
            for task in analysis_results.get("failed_tasks", []):
                actual_errors.extend(task.get("error_patterns", []))
            errors_text = " ".join(actual_errors).lower()
            image_pull_indicators = [
                "imagepullbackoff",
                "errimagepull",
                "failed to pull image",
                "pull access denied",
            ]
            if any(ind in errors_text for ind in image_pull_indicators):
                return "Image pull failure (ImagePullBackOff) - unable to pull container image from registry"
            # If "image" matched from log context but errors point to a step/build failure, use step details
            all_failed_steps = []
            for task in analysis_results.get("failed_tasks", []):
                for step in task.get("failed_steps", []):
                    all_failed_steps.append(step)
            if all_failed_steps:
                step_details = []
                for step in all_failed_steps[:3]:
                    step_name = step.get("step_name", "unknown")
                    exit_code = step.get("exit_code", "?")
                    step_details.append(f"'{step_name}' (exit code {exit_code})")
                steps_str = ", ".join(step_details)
                first_error = actual_errors[0][:120] if actual_errors else ""
                context = f" - {first_error}" if first_error else ""
                return f"Task step failure - step {steps_str} exited with non-zero code{context}"
            return "Image-related error - check container image references and registry access"
        elif category == "scheduling":
            return "Pod scheduling failure - insufficient resources or node constraints preventing scheduling"
        elif category == "storage":
            return "Storage/volume issues - failed to mount volume or PVC problems"
        elif category == "config":
            return (
                "Container configuration error - missing ConfigMap, Secret, or environment variable"
            )
        # General categories
        elif category == "resource_limits":
            return (
                "Resource constraint issues - the pipeline is likely hitting memory or CPU limits"
            )
        elif category == "network":
            return "Network connectivity issues - check network policies and external dependencies"
        elif category == "permissions":
            return "Permission or authorization issues - check RBAC settings and service account permissions"
        elif category == "configuration":
            return "Configuration errors - check pipeline parameters and ConfigMaps"
        elif category == "dependency":
            return "Dependency issues - check for missing dependencies or version mismatches"
        elif category == "step_failures":
            # Build description from failed step details
            failed_step_details = []
            for task in analysis_results["failed_tasks"]:
                for step in task.get("failed_steps", []):
                    step_name = step.get("step_name", "unknown")
                    exit_code = step.get("exit_code", "?")
                    failed_step_details.append(f"'{step_name}' (exit code {exit_code})")
            if failed_step_details:
                steps_str = ", ".join(failed_step_details[:3])
                return f"Task step failure - step {steps_str} exited with non-zero code"
            return "Task step failure - one or more steps exited with non-zero code"
        elif category == "filesystem":
            return "Filesystem issues - check for missing files or storage problems"

    # Fallback: check for failed_steps even without categorized errors
    all_failed_steps = []
    for task in analysis_results["failed_tasks"]:
        for step in task.get("failed_steps", []):
            all_failed_steps.append(step)
    if all_failed_steps:
        step_details = []
        for step in all_failed_steps[:3]:
            step_name = step.get("step_name", "unknown")
            exit_code = step.get("exit_code", "?")
            reason = step.get("reason", "")
            detail = f"'{step_name}' (exit code {exit_code})"
            if reason:
                detail += f" [{reason}]"
            step_details.append(detail)
        steps_str = ", ".join(step_details)
        return f"Task step failure - step {steps_str} exited with non-zero code"

    # Check task messages for additional context
    task_messages = []
    for task in analysis_results["failed_tasks"]:
        msg = task.get("message", "")
        if msg:
            task_messages.append(msg)
    if task_messages:
        return f"Pipeline task failure: {task_messages[0][:150]}"

    return "Indeterminate - multiple potential causes"


def recommend_actions(analysis_results: Dict[str, Any]) -> List[str]:
    """
    Recommend actions based on analysis results.

    Args:
        analysis_results: Dictionary containing failed task analysis

    Returns:
        List of recommended actions
    """
    if "error" in analysis_results:
        return ["Fix connection or permission issues with Kubernetes API"]

    recommendations = []

    # Based on root cause, suggest appropriate actions
    root_cause = analysis_results.get("probable_root_cause", "").lower()

    # Kubernetes-specific root causes
    if "oomkilled" in root_cause or "out of memory" in root_cause:
        recommendations.extend(
            [
                "Increase memory limits for the affected container/task",
                "Check if the build process has memory leaks",
                "Consider splitting large tasks into smaller steps",
                "Review memory requests vs limits ratio",
                "Monitor memory usage during pipeline execution",
            ]
        )
    elif "crashloopbackoff" in root_cause or "crash loop" in root_cause:
        recommendations.extend(
            [
                "Check container logs for crash reason before restart",
                "Verify container entrypoint and command are correct",
                "Check if required dependencies are available at startup",
                "Review liveness/readiness probe configurations",
                "Check for race conditions in container initialization",
            ]
        )
    elif "imagepullbackoff" in root_cause or "image pull" in root_cause:
        recommendations.extend(
            [
                "Verify the container image exists in the registry",
                "Check image pull secrets are configured correctly",
                "Verify registry credentials are valid and not expired",
                "Check network access to the container registry",
                "Verify image tag is correct and available",
            ]
        )
    elif "scheduling" in root_cause:
        recommendations.extend(
            [
                "Check node resources - ensure sufficient CPU/memory available",
                "Review node selectors and affinity rules",
                "Check for taints on nodes that may prevent scheduling",
                "Verify resource quotas in the namespace",
                "Check if required node labels exist",
            ]
        )
    elif "storage" in root_cause or "volume" in root_cause:
        recommendations.extend(
            [
                "Check PVC status and bound PV availability",
                "Verify storage class exists and is default or specified",
                "Check if the storage provisioner is healthy",
                "Review volume mount paths for conflicts",
                "Check storage quota limits in the namespace",
            ]
        )
    elif "container configuration" in root_cause:
        recommendations.extend(
            [
                "Verify referenced ConfigMaps exist and have required keys",
                "Check Secrets are available and properly referenced",
                "Review environment variable definitions",
                "Check volume mounts for ConfigMaps/Secrets",
                "Verify container security context settings",
            ]
        )
    # General categories
    elif "resource constraint" in root_cause:
        recommendations.extend(
            [
                "Check resource quotas and limits in the namespace",
                "Consider increasing CPU/memory limits for affected pods",
                "Review resource requests/limits in PipelineRun and TaskRun specs",
                "Monitor cluster resource utilization during pipeline runs",
            ]
        )
    elif "network" in root_cause:
        recommendations.extend(
            [
                "Verify network policies allow necessary connections",
                "Check external dependencies are accessible from the cluster",
                "Review DNS configuration in the cluster",
                "Check for timeouts in build configurations",
            ]
        )
    elif "permission" in root_cause or "authorization" in root_cause:
        recommendations.extend(
            [
                "Review RBAC permissions for service accounts used by Tekton pipelines",
                "Check if appropriate ClusterRoles and RoleBindings are in place",
                "Verify service account tokens are mounted correctly",
                "Check for recent changes to RBAC policies",
            ]
        )
    elif "configuration" in root_cause:
        recommendations.extend(
            [
                "Check ConfigMaps and Secrets referenced by pipelines",
                "Verify pipeline parameters are correctly specified",
                "Review task definitions for correctness",
                "Check CI/CD pipeline configuration for inconsistencies",
            ]
        )
    elif "dependency" in root_cause:
        recommendations.extend(
            [
                "Check image versions in TaskRuns and PipelineRuns",
                "Verify external dependencies are available",
                "Update task definitions if using deprecated features",
                "Check for version mismatches between components",
            ]
        )
    elif "filesystem" in root_cause:
        recommendations.extend(
            [
                "Check persistent volume claims and storage classes",
                "Verify file paths in task specifications",
                "Check if required files exist in workspace volumes",
                "Review storage provisioner logs",
            ]
        )
    elif "step failure" in root_cause or "task step" in root_cause:
        recommendations.extend(
            [
                "Check the failed step's output for specific error details",
                "Review the task definition for the failing step",
                "Verify input parameters and workspace contents are correct",
                "Compare with previous successful runs of the same pipeline",
            ]
        )
        # Add step-specific recommendations
        for task in analysis_results.get("failed_tasks", []):
            for step in task.get("failed_steps", []):
                step_name = step.get("step_name", "")
                if step_name:
                    recommendations.append(
                        f"Investigate step '{step_name}' - check its script/command logic"
                    )
    else:
        # Generic recommendations when root cause is unclear
        recommendations.extend(
            [
                "Review complete logs of failed tasks",
                "Check recent changes to pipeline definitions",
                "Compare with previous successful runs",
                "Review cluster events for relevant warnings or errors",
                "Check health of Tekton controller components",
            ]
        )

    # Add specific task-related recommendations
    failed_tasks = analysis_results.get("failed_tasks", [])
    if failed_tasks:
        task_names = [task.get("task_name") for task in failed_tasks]
        recommendations.append(f"Focus investigation on failed tasks: {', '.join(task_names)}")

    return recommendations


async def get_pipeline_details(
    namespace: str,
    pipeline_run: str,
    k8s_custom_api,
    list_taskruns_func,
    calculate_duration_func,
    log,
) -> Dict[str, Any]:
    """
    Get detailed information about a specific pipeline run.

    Args:
        namespace: Kubernetes namespace
        pipeline_run: Name of the PipelineRun
        k8s_custom_api: Kubernetes CustomObjects API client
        list_taskruns_func: Function to list TaskRuns
        calculate_duration_func: Function to calculate duration
        log: Logger instance

    Returns:
        Dictionary with pipeline details or error
    """
    from kubernetes.client.rest import ApiException

    try:
        # Get the pipeline run custom resource
        pipeline_run_obj = k8s_custom_api.get_namespaced_custom_object(
            group="tekton.dev",
            version="v1",
            namespace=namespace,
            plural="pipelineruns",
            name=pipeline_run,
        )

        # Extract basic information
        metadata = pipeline_run_obj.get("metadata", {})
        spec = pipeline_run_obj.get("spec", {})
        status = pipeline_run_obj.get("status", {})
        conditions = status.get("conditions", [])
        condition = conditions[0] if conditions else {}

        # Get pipeline name from multiple sources (same logic as list_pipelineruns)
        # Priority: pipelineRef.name > labels > pipelineSpec metadata > unknown
        pipeline_name = "unknown"

        # 1. Check spec.pipelineRef.name (direct reference to named Pipeline)
        pipeline_ref = spec.get("pipelineRef", {})
        if pipeline_ref and pipeline_ref.get("name"):
            pipeline_name = pipeline_ref.get("name")

        # 2. Check common Tekton labels (used by Konflux and other platforms)
        if pipeline_name == "unknown":
            labels = metadata.get("labels", {})
            pipeline_name = (
                labels.get("tekton.dev/pipeline")
                or labels.get("pipelines.tekton.dev/pipeline")
                or labels.get("pipelines.openshift.io/pipeline")
                or "unknown"
            )

        # 3. Check inline pipelineSpec for name/displayName
        if pipeline_name == "unknown":
            pipeline_spec = spec.get("pipelineSpec", {})
            if pipeline_spec:
                pipeline_name = (
                    pipeline_spec.get("displayName") or pipeline_spec.get("name") or "unknown"
                )

        # Get all task runs for this pipeline
        task_runs = await list_taskruns_func(namespace, pipeline_run)

        result = {
            "name": pipeline_run,
            "pipeline": pipeline_name,
            "status": condition.get("reason", "Unknown"),
            "message": condition.get("message", ""),
            "started_at": status.get("startTime", "unknown"),
            "completed_at": status.get("completionTime", "unknown"),
            "duration": calculate_duration_func(
                status.get("startTime"), status.get("completionTime")
            ),
            "task_runs": task_runs,
        }

        return result

    except ApiException as e:
        log.error(
            f"Error getting pipeline details for {pipeline_run} in namespace {namespace}: {e}"
        )
        return {"error": str(e)}


async def get_task_details(
    namespace: str, task_run: str, k8s_custom_api, calculate_duration_func, log
) -> Dict[str, Any]:
    """
    Get detailed information about a specific task run.

    Args:
        namespace: Kubernetes namespace
        task_run: Name of the TaskRun
        k8s_custom_api: Kubernetes CustomObjects API client
        calculate_duration_func: Function to calculate duration
        log: Logger instance

    Returns:
        Dictionary with task details or error
    """
    from kubernetes.client.rest import ApiException

    try:
        # Get the task run custom resource
        task_run_obj = k8s_custom_api.get_namespaced_custom_object(
            group="tekton.dev", version="v1", namespace=namespace, plural="taskruns", name=task_run
        )

        # Extract basic information
        status = task_run_obj.get("status", {})
        conditions = status.get("conditions", [])
        condition = conditions[0] if conditions else {}

        # Get pod name from the task run
        pod_name = status.get("podName", "unknown")

        result = {
            "name": task_run,
            "task": task_run_obj.get("spec", {}).get("taskRef", {}).get("name", "unknown"),
            "status": condition.get("reason", "Unknown"),
            "message": condition.get("message", ""),
            "started_at": status.get("startTime", "unknown"),
            "completed_at": status.get("completionTime", "unknown"),
            "duration": calculate_duration_func(
                status.get("startTime"), status.get("completionTime")
            ),
            "pod": pod_name,
            "steps": [],
        }

        # Extract step information
        for step_state in status.get("steps", []):
            terminated = step_state.get("terminated", {})
            running = step_state.get("running", {})
            waiting = step_state.get("waiting", {})

            step_status = "Unknown"
            exit_code = None
            reason = None

            if terminated:
                step_status = "Terminated"
                exit_code = terminated.get("exitCode")
                reason = terminated.get("reason")
            elif running:
                step_status = "Running"
            elif waiting:
                step_status = "Waiting"
                reason = waiting.get("reason")

            result["steps"].append(
                {
                    "name": step_state.get("name", "unknown"),
                    "status": step_status,
                    "exit_code": exit_code,
                    "reason": reason,
                }
            )

        return result

    except ApiException as e:
        log.error(f"Error getting task details for {task_run} in namespace {namespace}: {e}")
        return {"error": str(e)}


# ============================================================================
# RESOURCE SEARCH HELPERS
# ============================================================================


def build_advanced_label_selector(label_selectors: List[Dict[str, Any]]) -> str:
    """
    Build Kubernetes label selector string from label selector criteria with operators.

    Args:
        label_selectors: List of selector criteria, each with:
            - key: Label key to match
            - value: Label value (optional depending on operator)
            - operator: One of "equals", "exists", "not_equals", "in", "not_in"

    Returns:
        Comma-separated label selector string for Kubernetes API
    """
    selectors = []

    for selector in label_selectors:
        key = selector.get("key", "")
        value = selector.get("value", "")
        operator = selector.get("operator", "equals")

        if not key:
            continue

        if operator == "equals":
            if value:
                selectors.append(f"{key}={value}")
            else:
                selectors.append(f"{key}")
        elif operator == "exists":
            selectors.append(f"{key}")
        elif operator == "not_equals":
            if value:
                selectors.append(f"{key}!={value}")
        elif operator == "in":
            if value and isinstance(value, str):
                values = [v.strip() for v in value.split(",")]
                selectors.append(f"{key} in ({','.join(values)})")
        elif operator == "not_in":
            if value and isinstance(value, str):
                values = [v.strip() for v in value.split(",")]
                selectors.append(f"{key} notin ({','.join(values)})")

    return ",".join(selectors)


def get_resource_api_info(resource_type: str) -> Optional[Dict[str, Any]]:
    """
    Get API information for different Kubernetes/OpenShift resource types.

    Args:
        resource_type: Resource type name (e.g., "pods", "deployments", "pipelineruns")

    Returns:
        Dictionary with API info (api, method, namespaced, group, version, plural) or None
    """
    resource_map = {
        # Core resources
        "pods": {"api": "core_v1", "method": "list_namespaced_pod", "namespaced": True},
        "services": {"api": "core_v1", "method": "list_namespaced_service", "namespaced": True},
        "configmaps": {
            "api": "core_v1",
            "method": "list_namespaced_config_map",
            "namespaced": True,
        },
        "secrets": {"api": "core_v1", "method": "list_namespaced_secret", "namespaced": True},
        "persistentvolumeclaims": {
            "api": "core_v1",
            "method": "list_namespaced_persistent_volume_claim",
            "namespaced": True,
        },
        "persistentvolumes": {
            "api": "core_v1",
            "method": "list_persistent_volume",
            "namespaced": False,
        },
        "nodes": {"api": "core_v1", "method": "list_node", "namespaced": False},
        "namespaces": {"api": "core_v1", "method": "list_namespace", "namespaced": False},
        # Apps resources
        "deployments": {
            "api": "apps_v1",
            "method": "list_namespaced_deployment",
            "namespaced": True,
        },
        "replicasets": {
            "api": "apps_v1",
            "method": "list_namespaced_replica_set",
            "namespaced": True,
        },
        "daemonsets": {
            "api": "apps_v1",
            "method": "list_namespaced_daemon_set",
            "namespaced": True,
        },
        "statefulsets": {
            "api": "apps_v1",
            "method": "list_namespaced_stateful_set",
            "namespaced": True,
        },
        # Batch resources
        "jobs": {"api": "batch_v1", "method": "list_namespaced_job", "namespaced": True},
        "cronjobs": {"api": "batch_v1", "method": "list_namespaced_cron_job", "namespaced": True},
        # OpenShift specific resources (using custom API)
        "routes": {
            "api": "custom",
            "group": "route.openshift.io",
            "version": "v1",
            "plural": "routes",
            "namespaced": True,
        },
        "buildconfigs": {
            "api": "custom",
            "group": "build.openshift.io",
            "version": "v1",
            "plural": "buildconfigs",
            "namespaced": True,
        },
        "builds": {
            "api": "custom",
            "group": "build.openshift.io",
            "version": "v1",
            "plural": "builds",
            "namespaced": True,
        },
        "imagestreams": {
            "api": "custom",
            "group": "image.openshift.io",
            "version": "v1",
            "plural": "imagestreams",
            "namespaced": True,
        },
        "deploymentconfigs": {
            "api": "custom",
            "group": "apps.openshift.io",
            "version": "v1",
            "plural": "deploymentconfigs",
            "namespaced": True,
        },
        # Tekton resources (using custom API)
        "pipelineruns": {
            "api": "custom",
            "group": "tekton.dev",
            "version": "v1",
            "plural": "pipelineruns",
            "namespaced": True,
        },
        "taskruns": {
            "api": "custom",
            "group": "tekton.dev",
            "version": "v1",
            "plural": "taskruns",
            "namespaced": True,
        },
        "pipelines": {
            "api": "custom",
            "group": "tekton.dev",
            "version": "v1",
            "plural": "pipelines",
            "namespaced": True,
        },
        "tasks": {
            "api": "custom",
            "group": "tekton.dev",
            "version": "v1",
            "plural": "tasks",
            "namespaced": True,
        },
        "clustertasks": {
            "api": "custom",
            "group": "tekton.dev",
            "version": "v1",
            "plural": "clustertasks",
            "namespaced": False,
        },
        # Tekton Triggers resources
        "triggers": {
            "api": "custom",
            "group": "triggers.tekton.dev",
            "version": "v1beta1",
            "plural": "triggers",
            "namespaced": True,
        },
        "triggerbindings": {
            "api": "custom",
            "group": "triggers.tekton.dev",
            "version": "v1beta1",
            "plural": "triggerbindings",
            "namespaced": True,
        },
        "triggertemplates": {
            "api": "custom",
            "group": "triggers.tekton.dev",
            "version": "v1beta1",
            "plural": "triggertemplates",
            "namespaced": True,
        },
        "eventlisteners": {
            "api": "custom",
            "group": "triggers.tekton.dev",
            "version": "v1beta1",
            "plural": "eventlisteners",
            "namespaced": True,
        },
    }

    return resource_map.get(resource_type.lower(), None)


def extract_resource_info(
    resource: Dict[str, Any], include_spec: bool, include_status: bool, resource_type_hint: str = ""
) -> Dict[str, Any]:
    """
    Extract relevant information from a Kubernetes resource.

    Args:
        resource: Raw Kubernetes resource dictionary
        include_spec: Whether to include the spec field
        include_status: Whether to include the status field
        resource_type_hint: Optional resource type (e.g. 'pods') used as fallback for kind

    Returns:
        Processed resource dictionary with standardized structure
    """
    metadata = resource.get("metadata") or {}

    # Handle both camelCase (raw API) and snake_case (Python client to_dict()) keys.
    # Use `or` to handle None values from to_dict() where the key exists but is None.
    # Kubernetes list API omits kind/apiVersion from individual items; use hint as fallback.
    _type_to_kind = {
        "pods": "Pod",
        "services": "Service",
        "deployments": "Deployment",
        "replicasets": "ReplicaSet",
        "daemonsets": "DaemonSet",
        "statefulsets": "StatefulSet",
        "jobs": "Job",
        "cronjobs": "CronJob",
        "configmaps": "ConfigMap",
        "secrets": "Secret",
        "ingresses": "Ingress",
        "pvc": "PersistentVolumeClaim",
        "serviceaccounts": "ServiceAccount",
        "nodes": "Node",
        "namespaces": "Namespace",
        "pipelineruns": "PipelineRun",
        "taskruns": "TaskRun",
    }
    _type_to_api_version = {
        "pods": "v1",
        "services": "v1",
        "configmaps": "v1",
        "secrets": "v1",
        "serviceaccounts": "v1",
        "namespaces": "v1",
        "nodes": "v1",
        "pvc": "v1",
        "persistentvolumes": "v1",
        "endpoints": "v1",
        "events": "v1",
        "deployments": "apps/v1",
        "replicasets": "apps/v1",
        "daemonsets": "apps/v1",
        "statefulsets": "apps/v1",
        "jobs": "batch/v1",
        "cronjobs": "batch/v1",
        "ingresses": "networking.k8s.io/v1",
        "pipelineruns": "tekton.dev/v1",
        "taskruns": "tekton.dev/v1",
        "pipelines": "tekton.dev/v1",
        "tasks": "tekton.dev/v1",
    }
    kind = (
        resource.get("kind")
        or resource.get("Kind")
        or _type_to_kind.get(resource_type_hint, "")
        or "Unknown"
    )
    api_version = (
        resource.get("apiVersion")
        or resource.get("api_version")
        or _type_to_api_version.get(resource_type_hint, "Unknown")
    )
    creation_ts = metadata.get("creationTimestamp") or metadata.get("creation_timestamp") or ""
    resource_version = metadata.get("resourceVersion") or metadata.get("resource_version") or ""

    kind = resource.get("kind") or _type_to_kind.get(resource_type_hint, "") or "Unknown"
    api_version = (
        resource.get("apiVersion")
        or resource.get("api_version")
        or _type_to_api_version.get(resource_type_hint, "Unknown")
    )
    creation_ts = metadata.get("creationTimestamp") or metadata.get("creation_timestamp") or ""
    resource_version = metadata.get("resourceVersion") or metadata.get("resource_version") or ""

    resource_info = {
        "kind": kind,
        "api_version": api_version,
        "metadata": {
            "name": metadata.get("name") or "",
            "namespace": metadata.get("namespace") or "",
            "labels": metadata.get("labels") or {},
            "annotations": metadata.get("annotations") or {},
            "creation_timestamp": creation_ts,
            "resource_version": resource_version,
            "uid": metadata.get("uid") or "",
        },
    }

    # Add spec if requested
    if include_spec:
        resource_info["spec"] = resource.get("spec", {})

    # Add status if requested
    if include_status:
        status = resource.get("status", {})
        processed_status = {
            "phase": status.get("phase", ""),
            "conditions": status.get("conditions", []),
            "ready_replicas": status.get("readyReplicas"),
            "available_replicas": status.get("availableReplicas"),
        }
        # Remove None values
        resource_info["status"] = {k: v for k, v in processed_status.items() if v is not None}

    # Add owner references (handle both camelCase and snake_case)
    owner_refs = metadata.get("ownerReferences") or metadata.get("owner_references") or []
    resource_info["owner_references"] = [
        {
            "kind": ref.get("kind", ""),
            "name": ref.get("name", ""),
            "uid": ref.get("uid", ""),
            "controller": ref.get("controller", False),
        }
        for ref in owner_refs
    ]

    # Placeholder for related resources
    resource_info["related_resources"] = []

    return resource_info


def analyze_labels(resources: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analyze label patterns across resources.

    Args:
        resources: List of processed resource dictionaries

    Returns:
        Analysis with common_labels, unique_labels, and label_patterns
    """
    from collections import defaultdict

    label_stats = defaultdict(lambda: {"values": set(), "count": 0})

    for resource in resources:
        labels = resource.get("metadata", {}).get("labels")
        if labels and isinstance(labels, dict):
            for key, value in labels.items():
                label_stats[key]["values"].add(str(value))
                label_stats[key]["count"] += 1

    # Convert to the expected format
    common_labels = []
    unique_labels = []

    for key, stats in label_stats.items():
        values_list = list(stats["values"])
        common_labels.append({"key": key, "values": values_list, "frequency": stats["count"]})

        # Add unique labels (labels with only one unique value)
        if len(values_list) == 1:
            unique_labels.append(
                {"key": key, "value": values_list[0], "resource_count": stats["count"]}
            )

    # Sort by frequency
    common_labels.sort(key=lambda x: x["frequency"], reverse=True)

    # Generate label patterns (simple pattern detection)
    label_patterns = []
    pattern_stats = defaultdict(int)

    for resource in resources:
        labels = resource.get("metadata", {}).get("labels", {})
        if labels:
            for key in labels.keys():
                if "/" in key:
                    domain = key.split("/")[0]
                    pattern_stats[f"{domain}/*"] += 1
                elif key.startswith("app"):
                    pattern_stats["app*"] += 1
                elif key.startswith("version"):
                    pattern_stats["version*"] += 1

    for pattern, count in pattern_stats.items():
        if count > 1:  # Only include patterns that appear multiple times
            label_patterns.append(
                {
                    "pattern": pattern,
                    "matching_resources": count,
                    "examples": [pattern.replace("*", "example")],
                }
            )

    return {
        "common_labels": common_labels[:10],  # Top 10 most common
        "unique_labels": unique_labels[:20],  # Top 20 unique labels
        "label_patterns": label_patterns,
    }


def calculate_namespace_distribution(resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Calculate resource distribution across namespaces.

    Args:
        resources: List of processed resource dictionaries

    Returns:
        List of namespace distribution entries sorted by resource count
    """
    from collections import defaultdict

    namespace_stats = defaultdict(lambda: {"count": 0, "types": set()})

    for resource in resources:
        namespace = resource.get("metadata", {}).get("namespace", "cluster-scoped")
        kind = resource.get("kind", "Unknown")

        namespace_stats[namespace]["count"] += 1
        namespace_stats[namespace]["types"].add(kind)

    distribution = []
    for namespace, stats in namespace_stats.items():
        distribution.append(
            {
                "namespace": namespace,
                "resource_count": stats["count"],
                "resource_types": list(stats["types"]),
            }
        )

    # Sort by resource count descending
    distribution.sort(key=lambda x: x["resource_count"], reverse=True)
    return distribution


def sort_resources(
    resources: List[Dict[str, Any]], sort_by: str, sort_order: str
) -> List[Dict[str, Any]]:
    """
    Sort resources based on specified criteria.

    Args:
        resources: List of processed resource dictionaries
        sort_by: Field to sort by - "name", "namespace", "creation_time", "labels"
        sort_order: Sort order - "asc" or "desc"

    Returns:
        Sorted list of resources
    """
    reverse = sort_order.lower() == "desc"

    if sort_by == "name":
        return sorted(
            resources, key=lambda x: x.get("metadata", {}).get("name", ""), reverse=reverse
        )
    elif sort_by == "namespace":
        return sorted(
            resources, key=lambda x: x.get("metadata", {}).get("namespace", ""), reverse=reverse
        )
    elif sort_by == "creation_time":
        return sorted(
            resources,
            key=lambda x: x.get("metadata", {}).get("creation_timestamp", ""),
            reverse=reverse,
        )
    elif sort_by == "labels":
        return sorted(
            resources, key=lambda x: len(x.get("metadata", {}).get("labels", {})), reverse=reverse
        )
    else:
        return resources


# ============================================================================
# CERTIFICATE PARSING HELPERS
# ============================================================================


def parse_certificate(cert_data: str) -> Optional[Dict[str, Any]]:
    """Parse X.509 certificate and extract relevant information."""
    try:
        if x509 is None:
            logger.warning("cryptography library not available for certificate parsing")
            return None

        # Handle different certificate formats
        if cert_data.startswith("-----BEGIN"):
            # PEM format
            cert_bytes = cert_data.encode("utf-8")
        else:
            # Assume base64 encoded
            cert_bytes = base64.b64decode(cert_data)

        cert = x509.load_pem_x509_certificate(cert_bytes, default_backend())

        # Extract certificate information
        subject = cert.subject
        issuer = cert.issuer

        # Get common name
        subject_cn = None
        issuer_cn = None
        try:
            subject_cn = subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        except (IndexError, AttributeError):
            pass

        try:
            issuer_cn = issuer.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
        except (IndexError, AttributeError):
            pass

        # Get SAN extension
        san_list = []
        try:
            san_ext = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
            san_list = [name.value for name in san_ext.value]
        except x509.ExtensionNotFound:
            pass

        # Calculate days until expiration
        now = datetime.utcnow()
        expiry_date = cert.not_valid_after
        days_remaining = (expiry_date - now).days

        # Extract key size from public key
        key_size = None
        try:
            public_key = cert.public_key()
            if hasattr(public_key, "key_size"):
                key_size = public_key.key_size
        except Exception:
            pass

        # Extract is_ca from BasicConstraints extension
        is_ca = False
        try:
            bc_ext = cert.extensions.get_extension_for_oid(ExtensionOID.BASIC_CONSTRAINTS)
            is_ca = bc_ext.value.ca
        except (x509.ExtensionNotFound, AttributeError):
            pass

        return {
            "subject_cn": subject_cn,
            "issuer_cn": issuer_cn,
            "subject": str(subject),
            "issuer": str(issuer),
            "not_before": cert.not_valid_before.isoformat(),
            "not_after": cert.not_valid_after.isoformat(),
            "days_remaining": days_remaining,
            "serial_number": str(cert.serial_number),
            "signature_algorithm": cert.signature_algorithm_oid._name,
            "san": san_list,
            "is_ca": is_ca,
            "key_size": key_size,
        }
    except Exception as e:
        logger.debug(f"Failed to parse certificate: {e}")
        return None


def categorize_certificate_status(
    days_remaining: int, warning_threshold: int, critical_threshold: int
) -> str:
    """Categorize certificate status based on days remaining."""
    if days_remaining < 0:
        return "expired"
    elif days_remaining <= critical_threshold:
        return "critical"
    elif days_remaining <= warning_threshold:
        return "warning"
    else:
        return "healthy"


# ============================================================================
# PERFORMANCE ANALYSIS HELPERS
# ============================================================================


def detect_performance_trend(durations: List[float]) -> str:
    """Detect performance trend in a series of duration values."""
    if not durations or len(durations) < 3:
        return "Insufficient data for trend analysis"

    # Simple linear regression to detect trend
    x = list(range(len(durations)))
    n = len(durations)

    # Calculate slope using least squares method
    sum_x = sum(x)
    sum_y = sum(durations)
    sum_xy = sum(x[i] * durations[i] for i in range(n))
    sum_xx = sum(x[i] ** 2 for i in range(n))

    try:
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x**2)
    except ZeroDivisionError:
        return "Unable to determine trend (calculation error)"

    # Calculate average for context
    avg_duration = sum_y / n

    # Calculate relative trend magnitude
    relative_slope = (slope / avg_duration) * 100 if avg_duration > 0 else 0

    # Interpret trend
    if abs(relative_slope) < 5:  # Less than 5% change per step
        return "Stable performance (no significant trend)"
    elif relative_slope > 10:  # More than 10% increase per step
        return "Significant performance degradation trend"
    elif relative_slope > 5:  # 5-10% increase per step
        return "Moderate performance degradation trend"
    elif relative_slope < -10:  # More than 10% decrease per step
        return "Significant performance improvement trend"
    elif relative_slope < -5:  # 5-10% decrease per step
        return "Moderate performance improvement trend"
    else:
        return "Slight performance variation (no clear trend)"


# ============================================================================
# TOPOLOGY GRAPH CONVERSION HELPERS
# ============================================================================


def convert_to_graphviz(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> str:
    """Convert topology to Graphviz DOT format."""
    lines = ["digraph topology {"]
    lines.append("    rankdir=TB;")
    lines.append("    node [shape=box];")

    for node in nodes:
        node_id = hashlib.md5(node["id"].encode()).hexdigest()[:8]
        label = f"{node['type']}\\n{node['name']}"
        lines.append(f'    {node_id} [label="{label}"];')

    for edge in edges:
        source_id = hashlib.md5(edge["source"].encode()).hexdigest()[:8]
        target_id = hashlib.md5(edge["target"].encode()).hexdigest()[:8]
        lines.append(f'    {source_id} -> {target_id} [label="{edge["relationship"]}"];')

    lines.append("}")
    return "\n".join(lines)


def convert_to_mermaid(nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]]) -> str:
    """Convert topology to Mermaid diagram format."""
    lines = ["graph TD"]

    # Track defined node IDs
    defined_nodes = set()

    # Add nodes
    for node in nodes:
        node_id = hashlib.md5(node["id"].encode()).hexdigest()[:8]
        label = f"{node['name']}<br/>{node['type']}"
        lines.append(f'    {node_id}["{label}"]')
        defined_nodes.add(node["id"])

    # Add missing target nodes from edges (e.g., pods referenced by services)
    for edge in edges:
        target = edge["target"]
        if target not in defined_nodes:
            # Parse target ID format: cluster:namespace:type:name
            parts = target.split(":")
            if len(parts) >= 4:
                resource_type = parts[2]
                resource_name = ":".join(parts[3:])  # Handle names with colons
            else:
                resource_type = "resource"
                resource_name = target.split(":")[-1] if ":" in target else target

            target_id = hashlib.md5(target.encode()).hexdigest()[:8]
            label = f"{resource_name}<br/>{resource_type}"
            lines.append(f'    {target_id}["{label}"]')
            defined_nodes.add(target)

    # Add edges
    for edge in edges:
        source_id = hashlib.md5(edge["source"].encode()).hexdigest()[:8]
        target_id = hashlib.md5(edge["target"].encode()).hexdigest()[:8]
        lines.append(f"    {source_id} -->|{edge['relationship']}| {target_id}")

    return "\n".join(lines)


# ============================================================================
# RESOURCE FORECASTING HELPERS
# ============================================================================


def calculate_forecast_intervals(forecast_horizon: str) -> int:
    """Calculate number of forecast intervals based on horizon."""
    period = parse_time_period(forecast_horizon)

    # Assuming 5-minute intervals
    intervals_per_hour = 12
    total_hours = int(period.total_seconds() / 3600)

    return total_hours * intervals_per_hour


def simple_linear_forecast(values: List[float], forecast_points: int) -> Dict[str, Any]:
    """Simple linear regression forecasting."""
    import numpy as np
    from scipy.stats import linregress

    if len(values) < 3:
        return {"predictions": [], "confidence": 0.0, "growth_rate": 0.0}

    try:
        x = np.arange(len(values))
        y = np.array(values)

        # Remove NaN values
        mask = ~np.isnan(y)
        x_clean = x[mask]
        y_clean = y[mask]

        if len(x_clean) < 3:
            return {"predictions": [], "confidence": 0.0, "growth_rate": 0.0}

        slope, intercept, r_value, p_value, std_err = linregress(x_clean, y_clean)

        # Generate predictions
        future_x = np.arange(len(values), len(values) + forecast_points)
        predictions = slope * future_x + intercept

        # Calculate confidence (R-squared)
        confidence = r_value**2 if not np.isnan(r_value) else 0.0

        return {
            "predictions": predictions.tolist(),
            "confidence": confidence,
            "growth_rate": slope,
            "r_squared": confidence,
        }
    except Exception as e:
        logger.warning(f"Linear forecasting failed: {str(e)}")
        return {"predictions": [], "confidence": 0.0, "growth_rate": 0.0}


# ============================================================================
# SIMULATION HELPER FUNCTIONS
# ============================================================================


def convert_duration_to_seconds(duration: str) -> int:
    """Convert duration string to seconds."""
    duration_map = {"1h": 3600, "24h": 86400, "7d": 604800}
    return duration_map.get(duration, 86400)


def convert_duration_to_hours(duration: str) -> int:
    """Convert duration string to hours."""
    duration_map = {"1h": 1, "24h": 24, "7d": 168}
    return duration_map.get(duration, 24)


def calculate_std_dev(values: List[float]) -> float:
    """Calculate standard deviation of a list of values."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
    return variance**0.5


def calibrate_simulation_models(
    behavior_models: Dict[str, Any], historical_data: Dict[str, Any], load_profile: str
) -> Dict[str, Any]:
    """Calibrate simulation models using historical data."""
    try:
        calibrated = behavior_models.copy()

        # Adjust models based on historical performance patterns
        if "cpu_utilization_stats" in historical_data:
            cpu_stats = historical_data["cpu_utilization_stats"]

            # Adjust resource consumption models
            if "resource_consumption" in calibrated:
                # Use historical variance to adjust uncertainty
                uncertainty_factor = cpu_stats.get("std_dev", 10) / cpu_stats.get("mean", 50)
                calibrated["resource_consumption"]["uncertainty_factor"] = uncertainty_factor
                calibrated["resource_consumption"]["historical_peak"] = cpu_stats.get("max", 80)
                calibrated["resource_consumption"]["historical_baseline"] = cpu_stats.get(
                    "mean", 45
                )

        # Adjust for load profile
        load_multipliers = {
            "current": 1.0,
            "peak": 1.8,  # 80% increase for peak load
            "custom": 1.5,  # 50% increase for custom load
        }

        load_multiplier = load_multipliers.get(load_profile, 1.0)

        # Apply load multiplier to relevant models
        if "resource_consumption" in calibrated:
            for key in ["avg_cpu_per_pod", "avg_memory_per_pod"]:
                if key in calibrated["resource_consumption"]:
                    calibrated["resource_consumption"][key] *= load_multiplier

        if "scaling_patterns" in calibrated:
            calibrated["scaling_patterns"]["load_multiplier"] = load_multiplier

        # Add calibration metadata
        calibrated["calibration_info"] = {
            "historical_data_points": len(historical_data.get("cpu_utilization", [])),
            "load_profile": load_profile,
            "load_multiplier": load_multiplier,
            "calibration_timestamp": datetime.now().isoformat(),
        }

        return calibrated

    except Exception as e:
        logger.error(f"Error calibrating simulation models: {e}")
        return behavior_models  # Return original models if calibration fails


def _parse_k8s_quantity(value: str) -> float:
    """Parse a Kubernetes resource quantity string to a numeric value.

    Handles suffixes: m (milli), Ki, Mi, Gi, Ti, K, M, G, T.
    Examples: '500m' -> 0.5, '1Gi' -> 1073741824, '2' -> 2.0
    """
    value = value.strip()
    try:
        return float(value)
    except ValueError:
        pass
    suffixes = {
        "m": 0.001,
        "Ki": 1024,
        "Mi": 1024**2,
        "Gi": 1024**3,
        "Ti": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }
    # Try longest suffixes first to match 'Gi' before 'G'
    for suffix in sorted(suffixes, key=len, reverse=True):
        if value.endswith(suffix):
            numeric_part = value[: -len(suffix)]
            return float(numeric_part) * suffixes[suffix]
    raise ValueError(f"Cannot parse Kubernetes quantity: {value}")


async def run_monte_carlo_simulation(
    models: Dict[str, Any],
    changes: Dict[str, Any],
    scenario_type: str,
    duration: str,
    risk_tolerance: str,
) -> Dict[str, Any]:
    """Run Monte Carlo simulation for uncertainty quantification."""
    import random

    try:
        # Number of simulation runs based on risk tolerance
        simulation_runs = {"conservative": 1000, "moderate": 500, "aggressive": 200}.get(
            risk_tolerance, 500
        )

        results = {
            "performance_impact": [],
            "resource_impact": [],
            "reliability_impact": [],
            "cost_impact": [],
        }

        logger.info(f"Running {simulation_runs} Monte Carlo simulations")

        # Calculate change magnitude from actual changes
        change_magnitude = 1.0
        for key, val in changes.items():
            if isinstance(val, dict) and "before" in val and "after" in val:
                try:
                    before = _parse_k8s_quantity(str(val["before"]))
                    after = _parse_k8s_quantity(str(val["after"]))
                    if before > 0:
                        ratio = abs(after - before) / before
                        change_magnitude = max(change_magnitude, ratio)
                except (ValueError, TypeError):
                    pass

        # Scale base impacts by scenario type
        scenario_impacts = {
            "resource_limits": {"perf": 0.15, "reliability": 0.1, "cost": 0.05},
            "scaling": {"perf": 0.05, "reliability": 0.03, "cost": 0.3},
            "configuration": {"perf": 0.1, "reliability": 0.15, "cost": 0.02},
            "deployment": {"perf": 0.08, "reliability": 0.12, "cost": 0.1},
        }
        base = scenario_impacts.get(scenario_type, {"perf": 0.1, "reliability": 0.05, "cost": 0.2})

        # Determine uncertainty factor once (outside the loop for consistency)
        raw_uncertainty = models.get("resource_consumption", {}).get("uncertainty_factor", 0.1)
        # Ensure minimum uncertainty so Monte Carlo produces meaningful variance
        uncertainty_factor = max(raw_uncertainty, 0.05)

        for run in range(simulation_runs):
            # Add randomness to each simulation run
            random_factor = random.gauss(1.0, uncertainty_factor)

            performance_impact = random_factor * base["perf"] * change_magnitude
            results["performance_impact"].append(performance_impact)

            resource_impact = random_factor * 0.15 * change_magnitude
            results["resource_impact"].append(resource_impact)

            reliability_impact = random_factor * base["reliability"] * change_magnitude
            results["reliability_impact"].append(reliability_impact)

            cost_impact = random_factor * base["cost"] * change_magnitude
            results["cost_impact"].append(cost_impact)

        # Calculate statistics for each impact type
        simulation_stats = {}
        for impact_type, values in results.items():
            if values:
                simulation_stats[impact_type] = {
                    "mean": statistics.mean(values),
                    "median": statistics.median(values),
                    "std_dev": statistics.stdev(values) if len(values) > 1 else 0,
                    "min": min(values),
                    "max": max(values),
                    "p95": statistics.quantiles(values, n=20)[18]
                    if len(values) >= 20
                    else max(values),
                    "p5": statistics.quantiles(values, n=20)[0]
                    if len(values) >= 20
                    else min(values),
                }

        simulation_stats["simulation_metadata"] = {
            "runs": simulation_runs,
            "scenario_type": scenario_type,
            "duration": duration,
            "risk_tolerance": risk_tolerance,
        }

        return simulation_stats

    except Exception as e:
        logger.error(f"Error in Monte Carlo simulation: {e}")
        return {"error": str(e)}


async def collect_baseline_system_data(
    scope: Dict[str, Any], k8s_core_api, list_namespaces, list_pods_fn
) -> Dict[str, Any]:
    """Collect current system state as baseline for simulation."""
    from kubernetes.client.rest import ApiException

    try:
        baseline = {
            "resource_usage": {},
            "performance_metrics": {},
            "component_health": {},
            "capacity_utilization": {},
        }

        # Get namespaces to analyze
        if scope.get("namespaces") == ["all"]:
            namespaces = await list_namespaces()
        else:
            namespaces = scope.get("namespaces", [])

        logger.info(
            f"Collecting baseline data for {len(namespaces)} namespaces (analyzing first 10)"
        )

        # Collect resource usage data
        for namespace in namespaces[:10]:  # Limit to prevent timeout
            try:
                # Get pods and their resource usage
                # list_pods requires (namespace, k8s_core_api, logger)
                pods = await list_pods_fn(namespace, k8s_core_api, logger)

                namespace_resources = {
                    "cpu_requests": 0,
                    "memory_requests": 0,
                    "cpu_limits": 0,
                    "memory_limits": 0,
                    "pod_count": len([p for p in pods if not p.get("error")]),
                }

                # Get resource quotas
                try:
                    quotas = k8s_core_api.list_namespaced_resource_quota(namespace)
                    quota_data = []
                    for quota in quotas.items:
                        if quota.status.hard and quota.status.used:
                            quota_info = {
                                "name": quota.metadata.name,
                                "hard": dict(quota.status.hard),
                                "used": dict(quota.status.used),
                            }
                            quota_data.append(quota_info)
                    namespace_resources["quotas"] = quota_data
                except ApiException:
                    namespace_resources["quotas"] = []

                baseline["resource_usage"][namespace] = namespace_resources

            except Exception as e:
                logger.warning(f"Error collecting baseline data for namespace {namespace}: {e}")

        # Get cluster-level metrics
        try:
            nodes = k8s_core_api.list_node()
            node_data = []
            for node in nodes.items:
                node_info = {
                    "name": node.metadata.name,
                    "capacity": dict(node.status.capacity) if node.status.capacity else {},
                    "allocatable": dict(node.status.allocatable) if node.status.allocatable else {},
                    "conditions": [],
                }

                if node.status.conditions:
                    for condition in node.status.conditions:
                        if condition.status == "True":
                            node_info["conditions"].append(condition.type)

                node_data.append(node_info)

            baseline["cluster_nodes"] = node_data
            logger.info(f"Collected data for {len(node_data)} nodes")

        except ApiException as e:
            logger.warning(f"Error collecting node data: {e}")
            baseline["cluster_nodes"] = []

        # Log collection summary
        namespaces_collected = len(baseline.get("resource_usage", {}))
        nodes_collected = len(baseline.get("cluster_nodes", []))
        logger.info(
            f"Baseline data collection complete: {namespaces_collected} namespaces, {nodes_collected} nodes"
        )

        return baseline

    except Exception as e:
        logger.error(f"Error collecting baseline system data: {e}", exc_info=True)
        return {"error": str(e)}


async def build_system_behavior_models(
    baseline_data: Dict[str, Any], scenario_type: str
) -> Dict[str, Any]:
    """Build mathematical models of system behavior based on current state."""
    try:
        models = {
            "resource_consumption": {},
            "performance_characteristics": {},
            "scaling_patterns": {},
            "dependency_relationships": {},
        }

        # Build resource consumption models
        total_pods = 0
        total_cpu_requests = 0
        total_memory_requests = 0

        for namespace, resources in baseline_data.get("resource_usage", {}).items():
            total_pods += resources.get("pod_count", 0)
            total_cpu_requests += resources.get("cpu_requests", 0)
            total_memory_requests += resources.get("memory_requests", 0)

        if total_pods > 0:
            models["resource_consumption"] = {
                "avg_cpu_per_pod": total_cpu_requests / total_pods,
                "avg_memory_per_pod": total_memory_requests / total_pods,
                "pod_density": total_pods,
                "baseline_utilization": {
                    "cpu": total_cpu_requests,
                    "memory": total_memory_requests,
                },
            }

        # Build performance characteristics based on scenario type
        if scenario_type == "scaling":
            models["scaling_patterns"] = {
                "linear_scaling_factor": 1.0,
                "overhead_factor": 0.1,
                "saturation_point": total_pods * 2,
                "resource_efficiency": 0.85,
            }
        elif scenario_type == "resource_limits":
            models["performance_characteristics"] = {
                "cpu_sensitivity": 0.8,
                "memory_sensitivity": 0.9,
                "io_sensitivity": 0.6,
                "network_sensitivity": 0.7,
            }
        elif scenario_type == "configuration":
            models["dependency_relationships"] = {
                "config_propagation_time": 30,
                "restart_probability": 0.3,
                "validation_time": 60,
                "rollback_time": 120,
            }
        elif scenario_type == "deployment":
            models["deployment_patterns"] = {
                "rolling_update_time": 300,
                "downtime_probability": 0.1,
                "resource_spike_factor": 1.5,
                "stabilization_time": 180,
            }

        return models

    except Exception as e:
        logger.error(f"Error building system behavior models: {e}")
        return {"error": str(e)}


async def load_historical_performance_data(
    scope: Dict[str, Any], duration: str, prometheus_query_fn=None
) -> Dict[str, Any]:
    """
    Load historical performance data for model calibration from Prometheus.

    Args:
        scope: Simulation scope with namespaces/clusters to analyze
        duration: Time duration for historical data (e.g., "24h", "7d")
        prometheus_query_fn: Async function to execute Prometheus queries.
                            Expected signature: async fn(query: str) -> Dict with 'success', 'data', 'error'

    Returns:
        Dict with historical metrics arrays and statistics
    """
    try:
        # Convert duration to hours for queries
        duration_hours = convert_duration_to_hours(duration)
        duration_str = f"{duration_hours}h"

        # Initialize historical data structure
        historical = {
            "cpu_utilization": [],
            "memory_utilization": [],
            "response_times": [],
            "error_rates": [],
            "throughput": [],
            "pipeline_durations": [],
            "data_source": "prometheus" if prometheus_query_fn else "synthetic",
        }

        # If no Prometheus function provided, fall back to synthetic data
        if prometheus_query_fn is None:
            logger.warning("No Prometheus query function provided, using synthetic data")
            return await _generate_synthetic_historical_data(duration_hours)

        logger.info(f"Loading historical performance data from Prometheus for {duration_str}")

        # Build namespace filter for queries
        namespaces = scope.get("namespaces", ["all"])
        if namespaces != ["all"] and namespaces:
            "|".join(namespaces[:10])  # Limit to prevent huge queries

        # Query 1: CPU utilization over time (hourly averages)
        # Using node-level CPU as percentage
        # Fallback simpler query if range vector fails
        cpu_query_simple = """
            avg(100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[1h])) * 100))
        """

        cpu_result = await prometheus_query_fn(cpu_query_simple)
        if cpu_result.get("success") and cpu_result.get("data"):
            for result in cpu_result["data"]:
                value = result.get("value", [None, None])
                if len(value) > 1 and value[1]:
                    try:
                        cpu_val = float(value[1])
                        historical["cpu_utilization"].append(cpu_val)
                    except (ValueError, TypeError):
                        pass

        # Query 2: Memory utilization
        memory_query = """
            avg(
                (1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
            )
        """

        memory_result = await prometheus_query_fn(memory_query)
        if memory_result.get("success") and memory_result.get("data"):
            for result in memory_result["data"]:
                value = result.get("value", [None, None])
                if len(value) > 1 and value[1]:
                    try:
                        mem_val = float(value[1])
                        historical["memory_utilization"].append(mem_val)
                    except (ValueError, TypeError):
                        pass

        # Query 3: Pipeline throughput (pipelines per hour)
        throughput_query = """
            sum(increase(tekton_pipelines_controller_pipelinerun_count[1h]))
        """

        throughput_result = await prometheus_query_fn(throughput_query)
        if throughput_result.get("success") and throughput_result.get("data"):
            for result in throughput_result["data"]:
                value = result.get("value", [None, None])
                if len(value) > 1 and value[1]:
                    try:
                        tput_val = float(value[1])
                        historical["throughput"].append(tput_val)
                    except (ValueError, TypeError):
                        pass

        # Query 4: Pipeline error rates
        error_rate_query = """
            sum(rate(tekton_pipelines_controller_pipelinerun_count{status="failed"}[1h])) /
            sum(rate(tekton_pipelines_controller_pipelinerun_count[1h])) * 100
        """

        error_result = await prometheus_query_fn(error_rate_query)
        if error_result.get("success") and error_result.get("data"):
            for result in error_result["data"]:
                value = result.get("value", [None, None])
                if len(value) > 1 and value[1]:
                    try:
                        err_val = float(value[1])
                        if not (err_val != err_val):  # Check for NaN
                            historical["error_rates"].append(err_val)
                    except (ValueError, TypeError):
                        pass

        # Query 5: Pipeline duration P50 (response times)
        duration_query = """
            histogram_quantile(0.50,
                sum(rate(tekton_pipelines_controller_pipelinerun_duration_seconds_bucket[1h])) by (le)
            )
        """

        duration_result = await prometheus_query_fn(duration_query)
        if duration_result.get("success") and duration_result.get("data"):
            for result in duration_result["data"]:
                value = result.get("value", [None, None])
                if len(value) > 1 and value[1]:
                    try:
                        dur_val = float(value[1])
                        if dur_val > 0 and not (dur_val != dur_val):  # Valid and not NaN
                            historical["response_times"].append(dur_val)
                            historical["pipeline_durations"].append(dur_val)
                    except (ValueError, TypeError):
                        pass

        # If we got no data from Prometheus, try alternate queries
        if not any(
            [
                historical["cpu_utilization"],
                historical["memory_utilization"],
                historical["throughput"],
            ]
        ):
            logger.warning("Primary Prometheus queries returned no data, trying alternate queries")

            # Try container-level CPU usage
            alt_cpu_query = """
                avg(rate(container_cpu_usage_seconds_total{container!=""}[5m])) * 100
            """
            alt_cpu_result = await prometheus_query_fn(alt_cpu_query)
            if alt_cpu_result.get("success") and alt_cpu_result.get("data"):
                for result in alt_cpu_result["data"]:
                    value = result.get("value", [None, None])
                    if len(value) > 1 and value[1]:
                        try:
                            historical["cpu_utilization"].append(float(value[1]))
                        except (ValueError, TypeError):
                            pass

            # Try container memory usage
            alt_memory_query = """
                sum(container_memory_working_set_bytes{container!=""}) /
                sum(machine_memory_bytes) * 100
            """
            alt_memory_result = await prometheus_query_fn(alt_memory_query)
            if alt_memory_result.get("success") and alt_memory_result.get("data"):
                for result in alt_memory_result["data"]:
                    value = result.get("value", [None, None])
                    if len(value) > 1 and value[1]:
                        try:
                            historical["memory_utilization"].append(float(value[1]))
                        except (ValueError, TypeError):
                            pass

            # Try tekton taskrun count as throughput proxy
            alt_throughput_query = """
                sum(increase(tekton_pipelines_controller_taskrun_count[1h]))
            """
            alt_throughput_result = await prometheus_query_fn(alt_throughput_query)
            if alt_throughput_result.get("success") and alt_throughput_result.get("data"):
                for result in alt_throughput_result["data"]:
                    value = result.get("value", [None, None])
                    if len(value) > 1 and value[1]:
                        try:
                            # Convert taskruns to estimated pipelines (divide by avg tasks per pipeline)
                            taskruns = float(value[1])
                            estimated_pipelines = (
                                taskruns / 5.0
                            )  # Assume 5 tasks per pipeline average
                            historical["throughput"].append(estimated_pipelines)
                        except (ValueError, TypeError):
                            pass

        # Log data collection results
        data_summary = {k: len(v) for k, v in historical.items() if isinstance(v, list)}
        logger.info(f"Historical data collected: {data_summary}")

        # If still no data, generate synthetic as fallback but mark it
        total_data_points = sum(len(v) for v in historical.values() if isinstance(v, list))
        if total_data_points == 0:
            logger.warning("No Prometheus data available, falling back to synthetic data")
            synthetic = await _generate_synthetic_historical_data(duration_hours)
            synthetic["data_source"] = "synthetic_fallback"
            synthetic["prometheus_queries_attempted"] = True
            return synthetic

        # Calculate statistics for each metric
        for metric in [
            "cpu_utilization",
            "memory_utilization",
            "response_times",
            "error_rates",
            "throughput",
            "pipeline_durations",
        ]:
            values = historical.get(metric, [])
            if values and len(values) > 0:
                historical[f"{metric}_stats"] = {
                    "mean": sum(values) / len(values),
                    "min": min(values),
                    "max": max(values),
                    "std_dev": calculate_std_dev(values) if len(values) > 1 else 0,
                    "count": len(values),
                }

        historical["collection_timestamp"] = datetime.now().isoformat()
        historical["duration_queried"] = duration_str

        return historical

    except Exception as e:
        logger.error(f"Error loading historical performance data: {e}", exc_info=True)
        # Return synthetic data as fallback with error info
        fallback = await _generate_synthetic_historical_data(
            convert_duration_to_hours(duration) if duration else 24
        )
        fallback["data_source"] = "synthetic_error_fallback"
        fallback["error"] = str(e)
        return fallback


async def _generate_synthetic_historical_data(duration_hours: int) -> Dict[str, Any]:
    """Generate synthetic historical data as fallback when Prometheus is unavailable."""
    import random
    import math

    historical = {
        "cpu_utilization": [],
        "memory_utilization": [],
        "response_times": [],
        "error_rates": [],
        "throughput": [],
        "data_source": "synthetic",
    }

    # Generate hourly data points
    for hour in range(min(168, duration_hours)):  # Max 1 week of hourly data
        # Simulate daily patterns (higher during business hours)
        hour_of_day = hour % 24
        business_hours_factor = 1.0 + 0.5 * math.sin(math.pi * (hour_of_day - 6) / 12)
        business_hours_factor = max(0.3, business_hours_factor)

        # Add some randomness
        noise = random.gauss(1.0, 0.1)

        # Generate metrics with realistic correlations
        base_cpu = 45 * business_hours_factor * noise
        base_memory = 60 * business_hours_factor * noise
        base_response = 150 * (1 + 0.5 * (base_cpu / 100))
        base_errors = max(0.1, 2.0 * (base_cpu / 100) ** 2)
        base_throughput = 1000 * business_hours_factor * (1 - base_errors / 100)

        historical["cpu_utilization"].append(min(95, max(10, base_cpu)))
        historical["memory_utilization"].append(min(90, max(20, base_memory)))
        historical["response_times"].append(max(50, base_response))
        historical["error_rates"].append(min(10, base_errors))
        historical["throughput"].append(max(100, base_throughput))

    # Calculate statistics
    for metric in [
        "cpu_utilization",
        "memory_utilization",
        "response_times",
        "error_rates",
        "throughput",
    ]:
        values = historical.get(metric, [])
        if values:
            historical[f"{metric}_stats"] = {
                "mean": sum(values) / len(values),
                "min": min(values),
                "max": max(values),
                "std_dev": calculate_std_dev(values) if len(values) > 1 else 0,
                "count": len(values),
            }

    return historical
