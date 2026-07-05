# ============================================================================
# SEMANTIC SEARCH HELPER MODULE
# ============================================================================
#
# This module contains all semantic search related classes, functions, and utilities
# used by the MCP server for natural language query processing, intent recognition,
# and intelligent search across logs, events, and resources.
# ============================================================================

import re
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict


# ============================================================================
# QUERY INTERPRETATION AND PROCESSING
# ============================================================================


def interpret_semantic_query(query: str, time_range: str) -> Dict[str, Any]:
    """Interpret natural language query using domain knowledge and NLP concepts."""
    query_lower = query.lower()

    # Domain-specific intent patterns
    intent_patterns = {
        "error_investigation": [
            "error",
            "fail",
            "crash",
            "exception",
            "problem",
            "issue",
            "broken",
        ],
        "performance_analysis": [
            "slow",
            "performance",
            "latency",
            "timeout",
            "bottleneck",
            "cpu",
            "memory",
        ],
        "deployment_tracking": ["deploy", "rollout", "update", "release", "version", "upgrade"],
        "pipeline_monitoring": ["pipeline", "build", "test", "ci/cd", "tekton", "task"],
        "resource_monitoring": ["resource", "quota", "limit", "capacity", "usage", "allocation"],
        "security_audit": ["security", "permission", "access", "auth", "rbac", "token"],
        "network_debugging": ["network", "connection", "dns", "service", "ingress", "route"],
    }

    # Determine primary intent
    intent_scores = {}
    for intent, keywords in intent_patterns.items():
        score = sum(1 for keyword in keywords if keyword in query_lower)
        if score > 0:
            intent_scores[intent] = score

    primary_intent = (
        max(intent_scores, key=intent_scores.get) if intent_scores else "general_search"
    )

    # Extract temporal indicators
    temporal_indicators = {
        "recent": ["recent", "latest", "new", "just", "now"],
        "ongoing": ["current", "active", "running", "ongoing"],
        "historical": ["yesterday", "last week", "previous", "old", "past"],
    }

    temporal_context = "recent"  # default
    for context, keywords in temporal_indicators.items():
        if any(keyword in query_lower for keyword in keywords):
            temporal_context = context
            break

    # Build semantic interpretation
    interpreted_intent = _build_interpreted_intent(primary_intent, query_lower, temporal_context)

    return {
        "original_query": query,
        "primary_intent": primary_intent,
        "interpreted_intent": interpreted_intent,
        "temporal_context": temporal_context,
        "intent_confidence": max(intent_scores.values()) if intent_scores else 0,
        "semantic_keywords": _extract_semantic_keywords(query_lower, primary_intent),
    }


def _build_interpreted_intent(primary_intent: str, query_lower: str, temporal_context: str) -> str:
    """Build human-readable interpretation of the query intent."""
    intent_templates = {
        "error_investigation": f"Investigating errors and failures{_get_temporal_phrase(temporal_context)}",
        "performance_analysis": f"Analyzing performance issues{_get_temporal_phrase(temporal_context)}",
        "deployment_tracking": f"Tracking deployment activities{_get_temporal_phrase(temporal_context)}",
        "pipeline_monitoring": f"Monitoring CI/CD pipeline execution{_get_temporal_phrase(temporal_context)}",
        "resource_monitoring": f"Monitoring resource usage and limits{_get_temporal_phrase(temporal_context)}",
        "security_audit": f"Auditing security and access patterns{_get_temporal_phrase(temporal_context)}",
        "network_debugging": f"Debugging network connectivity issues{_get_temporal_phrase(temporal_context)}",
        "general_search": f"General log search{_get_temporal_phrase(temporal_context)}",
    }

    base_intent = intent_templates.get(primary_intent, "General log analysis")

    # Add specific query elements
    specific_terms = [
        word
        for word in query_lower.split()
        if len(word) > 3 and word not in ["error", "logs", "find", "show", "get"]
    ]
    if specific_terms:
        base_intent += f" focusing on: {', '.join(specific_terms[:3])}"

    return base_intent


def _get_temporal_phrase(temporal_context: str) -> str:
    """Get temporal phrase for intent description."""
    phrases = {
        "recent": " in recent activity",
        "ongoing": " in current operations",
        "historical": " from historical data",
    }
    return phrases.get(temporal_context, "")


def _extract_semantic_keywords(query_lower: str, primary_intent: str) -> List[str]:
    """Extract semantically relevant keywords based on intent."""
    # Base keywords from query
    base_keywords = [word for word in query_lower.split() if len(word) > 2]

    # Intent-specific keyword expansion
    keyword_expansions = {
        "error_investigation": ["exception", "fault", "crash", "failure", "bug"],
        "performance_analysis": ["slow", "latency", "response", "throughput", "bottleneck"],
        "deployment_tracking": ["rollout", "release", "version", "update", "deploy"],
        "pipeline_monitoring": ["build", "test", "stage", "run", "execute"],
        "resource_monitoring": ["cpu", "memory", "disk", "quota", "limit"],
        "security_audit": ["auth", "permission", "access", "security", "token"],
        "network_debugging": ["connection", "dns", "timeout", "route", "service"],
    }

    expanded_keywords = base_keywords + keyword_expansions.get(primary_intent, [])
    return list(set(expanded_keywords))  # Remove duplicates


# ============================================================================
# SEARCH STRATEGY DETERMINATION
# ============================================================================


def determine_search_strategy(query_interpretation: Dict[str, Any]) -> Dict[str, Any]:
    """Determine optimal search strategy based on query interpretation."""
    primary_intent = query_interpretation["primary_intent"]
    confidence = query_interpretation["intent_confidence"]

    strategy_map = {
        "error_investigation": "error_focused_search",
        "performance_analysis": "metrics_and_logs_search",
        "deployment_tracking": "event_timeline_search",
        "pipeline_monitoring": "pipeline_lifecycle_search",
        "resource_monitoring": "resource_pattern_search",
        "security_audit": "security_event_search",
        "network_debugging": "network_trace_search",
        "general_search": "broad_semantic_search",
    }

    strategy = strategy_map.get(primary_intent, "broad_semantic_search")

    return {
        "strategy": strategy,
        "confidence": confidence,
        "search_scope": "targeted" if confidence > 2 else "broad",
        "priority_sources": _get_priority_sources(primary_intent),
    }


def _get_priority_sources(primary_intent: str) -> List[str]:
    """Get priority log sources based on intent."""
    source_priority = {
        "error_investigation": ["pods", "events", "controller-logs"],
        "performance_analysis": ["metrics", "pods", "system-logs"],
        "deployment_tracking": ["events", "controller-logs", "pods"],
        "pipeline_monitoring": ["pipelineruns", "taskruns", "pods"],
        "resource_monitoring": ["metrics", "events", "quota-logs"],
        "security_audit": ["audit-logs", "events", "rbac-logs"],
        "network_debugging": ["service-logs", "dns-logs", "ingress-logs"],
        "general_search": ["pods", "events", "controller-logs"],
    }
    return source_priority.get(primary_intent, ["pods", "events"])


# ============================================================================
# ENTITY EXTRACTION
# ============================================================================


def extract_k8s_entities(query: str) -> List[str]:
    """Extract Kubernetes/Tekton entities from query using domain knowledge."""
    query_lower = query.lower()

    # Kubernetes entities
    k8s_entities = {
        "pod": ["pod", "pods"],
        "service": ["service", "services", "svc"],
        "deployment": ["deployment", "deployments", "deploy"],
        "namespace": ["namespace", "namespaces", "ns"],
        "configmap": ["configmap", "configmaps", "cm"],
        "secret": ["secret", "secrets"],
        "ingress": ["ingress", "ingresses"],
        "node": ["node", "nodes"],
        "persistentvolume": ["pv", "persistentvolume", "volume"],
        "persistentvolumeclaim": ["pvc", "persistentvolumeclaim", "claim"],
    }

    # Tekton entities
    tekton_entities = {
        "pipelinerun": ["pipelinerun", "pipelineruns", "pr"],
        "taskrun": ["taskrun", "taskruns", "tr"],
        "pipeline": ["pipeline", "pipelines"],
        "task": ["task", "tasks"],
        "clustertask": ["clustertask", "clustertasks"],
    }

    # CI/CD and OpenShift specific
    cicd_entities = {
        "application": ["application", "applications", "app"],
        "component": ["component", "components"],
        "integration": ["integration", "integrations"],
        "release": ["release", "releases"],
        "snapshot": ["snapshot", "snapshots"],
    }

    all_entities = {**k8s_entities, **tekton_entities, **cicd_entities}

    identified = []
    for entity_type, aliases in all_entities.items():
        if any(alias in query_lower for alias in aliases):
            identified.append(entity_type)

    return identified


# ============================================================================
# SEMANTIC MATCHING AND RELEVANCE
# ============================================================================


def find_semantic_matches(
    lines: List[str],
    semantic_keywords: List[str],
    primary_intent: str,
    max_results: int = 50,
    original_query_terms: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Find semantically relevant matches in log lines."""
    matches = []

    for i, line in enumerate(lines):
        relevance_score = calculate_semantic_relevance(
            line, semantic_keywords, primary_intent, original_query_terms
        )

        if relevance_score > 0.3:  # Threshold for relevance
            timestamp, level = extract_log_metadata(line)
            matches.append(
                {
                    "line_number": i + 1,
                    "content": line.strip(),
                    "relevance_score": relevance_score,
                    "timestamp": timestamp,
                    "log_level": level,
                    "match_reasons": identify_match_reasons(
                        line, semantic_keywords, primary_intent
                    ),
                    "related_count": _count_related_entries(lines, i, semantic_keywords),
                }
            )

    # Sort by relevance score descending so best matches come first
    matches.sort(key=lambda x: x["relevance_score"], reverse=True)
    return matches[:max_results]


def calculate_semantic_relevance(
    line: str,
    semantic_keywords: List[str],
    primary_intent: str,
    original_query_terms: Optional[List[str]] = None,
) -> float:
    """Calculate semantic relevance score for a log line.

    Prioritizes matches on original query terms over expanded/generic keywords.
    """
    line_lower = line.lower()
    score = 0.0

    # Original query term matching (high weight - these are what the user actually asked for)
    original_term_matched = False
    if original_query_terms:
        for term in original_query_terms:
            if term.lower() in line_lower:
                score += 0.5
                original_term_matched = True

    # Expanded keyword matching (lower weight)
    keyword_matches = sum(1 for keyword in semantic_keywords if keyword in line_lower)
    score += keyword_matches * 0.15

    # Intent-specific patterns (lower weight to avoid generic "error" dominating)
    intent_patterns = {
        "error_investigation": ["error", "exception", "fail", "crash", "panic", "fatal"],
        "performance_analysis": ["slow", "timeout", "latency", "cpu", "memory", "performance"],
        "deployment_tracking": ["deploy", "rollout", "update", "version", "release"],
        "pipeline_monitoring": ["pipeline", "task", "build", "test", "stage"],
        "resource_monitoring": ["resource", "quota", "limit", "usage"],
        "security_audit": ["permission", "access", "auth", "security", "forbidden"],
        "network_debugging": ["connection", "dns", "network", "timeout", "refused"],
    }

    patterns = intent_patterns.get(primary_intent, [])
    pattern_matches = sum(1 for pattern in patterns if pattern in line_lower)
    score += pattern_matches * 0.2

    # Severity-based scoring: prefer error/warn over info for error-focused searches
    severity_indicators = {
        "error": 0.8,
        "fatal": 0.9,
        "panic": 0.9,
        "exception": 0.7,
        "warn": 0.6,
        "warning": 0.6,
        "info": 0.1,
        "debug": 0.05,
    }

    for indicator, weight in severity_indicators.items():
        if indicator in line_lower:
            score += weight * 0.2
            break

    # Timestamp presence
    if re.search(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", line):
        score += 0.05

    # Structured log indicators
    if any(char in line for char in ["{", "}", "=", ":"]):
        score += 0.05

    # Penalty: if user provided specific query terms but none matched,
    # reduce score significantly to filter out generic noise
    if original_query_terms and not original_term_matched:
        score *= 0.4

    return min(score, 1.0)  # Cap at 1.0


def identify_match_reasons(
    line: str, semantic_keywords: List[str], primary_intent: str
) -> List[str]:
    """Identify why a log line matched the semantic search."""
    reasons = []
    line_lower = line.lower()

    # Keyword matches
    matched_keywords = [kw for kw in semantic_keywords if kw in line_lower]
    if matched_keywords:
        reasons.append(f"Keywords: {', '.join(matched_keywords)}")

    # Intent patterns
    if "error" in line_lower or "exception" in line_lower:
        reasons.append("Error pattern detected")

    if "warn" in line_lower:
        reasons.append("Warning pattern detected")

    if primary_intent == "performance_analysis" and any(
        word in line_lower for word in ["slow", "timeout", "latency"]
    ):
        reasons.append("Performance issue indicators")

    if primary_intent == "deployment_tracking" and any(
        word in line_lower for word in ["deploy", "rollout", "update"]
    ):
        reasons.append("Deployment activity indicators")

    # Structured data
    if re.search(r"\{.*\}", line):
        reasons.append("Structured JSON log")

    # Timestamp presence
    if re.search(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}", line):
        reasons.append("Timestamped entry")

    return reasons if reasons else ["General relevance"]


def extract_log_metadata(line: str) -> Tuple[str, str]:
    """Extract timestamp and log level from a log line.

    Uses positional matching to avoid misclassifying logs that contain
    log level keywords in their message content (e.g., 'error handling').
    """
    # Extract timestamp
    timestamp_match = re.search(
        r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)", line
    )
    timestamp = timestamp_match.group(1) if timestamp_match else "unknown"

    # Extract log level using positional patterns
    # Priority order: fatal/panic first, then error, warn, info, debug, trace
    level = "info"  # default
    line_lower = line.lower()

    # Pattern 1: Log level at start of line (after optional timestamp)
    # Matches: "ERROR: ...", "2024-01-24 10:30:00 ERROR ...", "10:30:00.123 error ..."
    start_pattern = re.match(
        r"^(?:\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?\s+)?"  # optional timestamp
        r"(?:\d{2}:\d{2}:\d{2}(?:\.\d+)?\s+)?"  # optional time-only
        r"(fatal|panic|critical|error|err|warn|warning|info|debug|trace)\b",
        line_lower,
    )
    if start_pattern:
        matched = start_pattern.group(1)
        if matched in ("fatal", "panic", "critical"):
            return timestamp, "fatal"
        elif matched in ("error", "err"):
            return timestamp, "error"
        elif matched in ("warn", "warning"):
            return timestamp, "warn"
        elif matched == "info":
            return timestamp, "info"
        elif matched == "debug":
            return timestamp, "debug"
        elif matched == "trace":
            return timestamp, "trace"

    # Pattern 2: Bracketed log levels: [ERROR], [INFO], [WARN], etc.
    bracket_match = re.search(
        r"\[(fatal|panic|critical|error|err|warn|warning|info|debug|trace)\]", line_lower
    )
    if bracket_match:
        matched = bracket_match.group(1)
        if matched in ("fatal", "panic", "critical"):
            return timestamp, "fatal"
        elif matched in ("error", "err"):
            return timestamp, "error"
        elif matched in ("warn", "warning"):
            return timestamp, "warn"
        elif matched == "info":
            return timestamp, "info"
        elif matched == "debug":
            return timestamp, "debug"
        elif matched == "trace":
            return timestamp, "trace"

    # Pattern 3: Structured log formats: level=error, "level":"error", level: error
    structured_match = re.search(
        r'(?:level|lvl|severity)[=:"\s]+["\']?(fatal|panic|critical|error|err|warn|warning|info|debug|trace)["\']?',
        line_lower,
    )
    if structured_match:
        matched = structured_match.group(1)
        if matched in ("fatal", "panic", "critical"):
            return timestamp, "fatal"
        elif matched in ("error", "err"):
            return timestamp, "error"
        elif matched in ("warn", "warning"):
            return timestamp, "warn"
        elif matched == "info":
            return timestamp, "info"
        elif matched == "debug":
            return timestamp, "debug"
        elif matched == "trace":
            return timestamp, "trace"

    # Pattern 4: Kubernetes-style single letter prefixes: I0124 (info), E0124 (error), W0124 (warn)
    k8s_match = re.match(r"^([IWEF])\d{4}\s", line)
    if k8s_match:
        letter = k8s_match.group(1)
        if letter == "I":
            return timestamp, "info"
        elif letter == "W":
            return timestamp, "warn"
        elif letter == "E":
            return timestamp, "error"
        elif letter == "F":
            return timestamp, "fatal"

    return timestamp, level


def _count_related_entries(
    lines: List[str], current_index: int, semantic_keywords: List[str]
) -> int:
    """Count related log entries near the current match."""
    count = 0
    search_range = 5  # Look 5 lines before and after

    start = max(0, current_index - search_range)
    end = min(len(lines), current_index + search_range + 1)

    for i in range(start, end):
        if i != current_index:  # Don't count the current line
            line_lower = lines[i].lower()
            if any(keyword in line_lower for keyword in semantic_keywords):
                count += 1

    return count


# ============================================================================
# RESULT RANKING AND PROCESSING
# ============================================================================


def _truncate_result_content(result: Dict[str, Any], max_message_len: int = 300) -> Dict[str, Any]:
    """Truncate log message content in a search result to limit output size."""
    result = result.copy()
    log_entry = result.get("log_entry", {})
    if isinstance(log_entry, dict):
        msg = log_entry.get("message", "")
        if isinstance(msg, str) and len(msg) > max_message_len:
            log_entry = log_entry.copy()
            log_entry["message"] = msg[:max_message_len] + "..."
            result["log_entry"] = log_entry
    return result


def rank_results_by_semantic_relevance(
    all_results: List[Dict[str, Any]],
    query_interpretation: Dict[str, Any],
    group_similar: bool = True,
) -> List[Dict[str, Any]]:
    """Rank and filter results by semantic relevance."""
    if not all_results:
        return []

    if group_similar:
        # Group similar results
        grouped_results = _group_similar_results(all_results)

        # Rank groups by best relevance score
        ranked_groups = sorted(
            grouped_results,
            key=lambda group: max(item.get("relevance_score", 0) for item in group),
            reverse=True,
        )

        # Deduplicate: keep only the best representative from each group
        ranked_results = []
        for group in ranked_groups:
            best = max(group, key=lambda x: x.get("relevance_score", 0))
            best = _truncate_result_content(best)
            if len(group) > 1:
                best["similar_count"] = len(group) - 1
            ranked_results.append(best)

        return ranked_results
    else:
        # Simple sort by relevance, with truncation
        sorted_results = sorted(
            all_results, key=lambda x: x.get("relevance_score", 0), reverse=True
        )
        return [_truncate_result_content(r) for r in sorted_results]


def _group_similar_results(results: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Group similar results together."""
    groups = []
    used_indices = set()

    for i, result in enumerate(results):
        if i in used_indices:
            continue

        current_group = [result]
        used_indices.add(i)

        # Find similar results
        for j, other_result in enumerate(results):
            if j in used_indices or j <= i:
                continue

            if _are_results_similar(result, other_result):
                current_group.append(other_result)
                used_indices.add(j)

        groups.append(current_group)

    return groups


def _get_result_content(result: Dict[str, Any]) -> str:
    """Extract the text content from a search result for comparison."""
    # Check nested log_entry.message first (standard structure from _search_pod_logs_semantically)
    log_entry = result.get("log_entry", {})
    if isinstance(log_entry, dict) and log_entry.get("message"):
        return str(log_entry["message"])
    # Fall back to top-level content
    return result.get("content", "")


def _get_result_source_key(result: Dict[str, Any]) -> str:
    """Build a comparable string key from a result's source metadata."""
    source = result.get("source", {})
    if isinstance(source, dict):
        return (
            f"{source.get('type', '')}:{source.get('namespace', '')}:{source.get('pod_name', '')}"
        )
    return str(source)


def _are_results_similar(result1: Dict[str, Any], result2: Dict[str, Any]) -> bool:
    """Check if two results are similar enough to group together."""
    # Check if they're from the same source (namespace + pod)
    if _get_result_source_key(result1) != _get_result_source_key(result2):
        return False

    # Extract content from the correct field
    content1 = _get_result_content(result1).lower()
    content2 = _get_result_content(result2).lower()

    # Simple similarity check based on common words
    words1 = set(content1.split())
    words2 = set(content2.split())

    if not words1 or not words2:
        return False

    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))

    similarity = intersection / union if union > 0 else 0
    return similarity > 0.6  # 60% similarity threshold


# ============================================================================
# PATTERN IDENTIFICATION
# ============================================================================


def identify_common_patterns(ranked_results: List[Dict[str, Any]]) -> List[str]:
    """Identify common patterns across search results."""
    patterns = []

    if not ranked_results:
        return patterns

    # Analyze log levels
    level_counts = defaultdict(int)
    for result in ranked_results:
        # Check nested log_entry.level first, then top-level log_level
        log_entry = result.get("log_entry", {})
        level = log_entry.get("level") if isinstance(log_entry, dict) else None
        if not level:
            level = result.get("log_level", "unknown")
        # Normalize level names
        if level == "warning":
            level = "warn"
        level_counts[level] += 1

    total_results = len(ranked_results)
    for level, count in level_counts.items():
        if count > total_results * 0.3:  # If level appears in >30% of results
            patterns.append(f"Frequent {level} level messages ({count}/{total_results})")

    # Analyze content patterns
    content_words = []
    for result in ranked_results:
        # Check nested log_entry.message first, then top-level content
        log_entry = result.get("log_entry", {})
        content = log_entry.get("message", "") if isinstance(log_entry, dict) else ""
        if not content:
            content = result.get("content", "")
        content = content.lower()
        # Extract meaningful words (excluding common words)
        words = [
            word
            for word in content.split()
            if len(word) > 3 and word not in ["the", "and", "for", "with", "from"]
        ]
        content_words.extend(words)

    # Find frequent words
    word_counts = defaultdict(int)
    for word in content_words:
        word_counts[word] += 1

    frequent_words = [
        word for word, count in word_counts.items() if count > len(ranked_results) * 0.25
    ]

    if frequent_words:
        patterns.append(f"Common terms: {', '.join(frequent_words[:5])}")

    # Analyze temporal patterns
    timestamps = []
    for result in ranked_results:
        # Check nested log_entry.timestamp first, then top-level timestamp
        log_entry = result.get("log_entry", {})
        ts = log_entry.get("timestamp") if isinstance(log_entry, dict) else None
        if not ts or ts == "unknown":
            ts = result.get("timestamp", "")
        if ts and ts != "unknown":
            timestamps.append(ts)

    if len(timestamps) > 5:
        patterns.append(f"Temporal distribution: {len(timestamps)} timestamped entries")

    return patterns


def analyze_severity_distribution(ranked_results: List[Dict[str, Any]]) -> Dict[str, int]:
    """Analyze severity distribution of search results."""
    severity_counts = {"error": 0, "warn": 0, "info": 0, "debug": 0, "unknown": 0}

    for result in ranked_results:
        # Check nested log_entry.level first, then top-level log_level for backwards compatibility
        log_entry = result.get("log_entry", {})
        level = log_entry.get("level") if isinstance(log_entry, dict) else None
        if not level:
            level = result.get("log_level", "unknown")

        # Normalize level names (warn/warning -> warn)
        if level == "warning":
            level = "warn"

        if level in severity_counts:
            severity_counts[level] += 1
        else:
            severity_counts["unknown"] += 1

    return severity_counts


# ============================================================================
# SEARCH SUGGESTIONS
# ============================================================================


def generate_semantic_suggestions(
    query_interpretation: Dict[str, Any], search_results: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Generate search suggestions based on query interpretation and results."""
    suggestions = {"related_queries": [], "broader_search": "", "narrower_search": ""}

    primary_intent = query_interpretation.get("primary_intent", "general_search")
    confidence = query_interpretation.get("intent_confidence", 0)

    # Low confidence suggestions
    if confidence < 2:
        suggestions["related_queries"].append("Try more specific keywords related to your issue")
        suggestions["related_queries"].append("Include error messages or specific component names")

    # Intent-specific suggestions
    if primary_intent == "error_investigation":
        suggestions["related_queries"].extend(
            [
                "Search for 'exception' or 'failed' for more error details",
                "Try searching for specific error codes or messages",
            ]
        )
    elif primary_intent == "performance_analysis":
        suggestions["related_queries"].extend(
            [
                "Search for 'timeout', 'slow', or 'latency' keywords",
                "Look for memory or CPU related issues",
            ]
        )
    elif primary_intent == "deployment_tracking":
        suggestions["related_queries"].extend(
            [
                "Search for 'rollout', 'update', or 'version' keywords",
                "Look for deployment status messages",
            ]
        )

    # Results-based suggestions
    if not search_results:
        suggestions["broader_search"] = "Try broadening your search with fewer specific terms"
        suggestions["narrower_search"] = "Check if the time range includes when the issue occurred"
    elif len(search_results) > 100:
        suggestions["broader_search"] = "Results are comprehensive"
        suggestions["narrower_search"] = "Try narrowing your search with more specific keywords"

    # Limit suggestions
    suggestions["related_queries"] = suggestions["related_queries"][:5]

    return suggestions


# ============================================================================
# ASYNC SEARCH HELPER FUNCTIONS
# ============================================================================


def _build_log_params(search_params: Dict[str, Any]) -> Dict[str, Any]:
    """Build log retrieval parameters from search params."""
    time_range = search_params.get("time_range", "1h")

    # Convert time range to seconds
    time_mapping = {"1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}

    since_seconds = time_mapping.get(time_range, 3600)

    return {
        "since_seconds": since_seconds,
        "tail_lines": 500,  # Reasonable limit for semantic analysis
    }


async def _get_target_namespaces(
    namespaces: Optional[List[str]],
    identified_components: List[str],
    list_namespaces,
    detect_tekton_namespaces_func,
) -> List[str]:
    """Determine target namespaces based on provided namespaces and identified components."""
    if namespaces:
        # If specific namespaces provided, use them directly
        return namespaces

    # Auto-detect relevant namespaces based on components
    all_namespaces = await list_namespaces()

    if not identified_components:
        # If no specific components, focus on Tekton/CI-CD related namespaces
        tekton_namespaces = await detect_tekton_namespaces_func()
        target_namespaces = []
        for ns_list in tekton_namespaces.values():
            target_namespaces.extend(ns_list)

        # Add some common system namespaces
        system_namespaces = ["kube-system", "openshift-etcd", "openshift-apiserver"]
        for ns in system_namespaces:
            if ns in all_namespaces:
                target_namespaces.append(ns)

        return target_namespaces[:10]  # Limit to avoid overwhelming search

    # Component-specific namespace targeting
    component_namespace_hints = {
        "pipelinerun": ["tekton", "pipeline", "ci", "build"],
        "taskrun": ["tekton", "pipeline", "ci", "build"],
        "pod": all_namespaces,  # Pods can be anywhere
        "deployment": all_namespaces,
        "service": all_namespaces,
    }

    target_namespaces = set()
    for component in identified_components:
        hints = component_namespace_hints.get(component, [])
        if hints == all_namespaces:
            target_namespaces.update(all_namespaces[:5])  # Limit broad searches
        else:
            matching_ns = [ns for ns in all_namespaces if any(hint in ns.lower() for hint in hints)]
            target_namespaces.update(matching_ns[:3])  # Limit per component

    return list(target_namespaces)[:10]  # Final limit


async def _search_pod_logs_semantically(
    pod_info: Dict[str, Any],
    namespace: str,
    query_interpretation: Dict[str, Any],
    search_params: Dict[str, Any],
    get_pod_logs,
    build_log_params_func,
    find_semantic_matches_func,
) -> List[Dict[str, Any]]:
    """Search pod logs using semantic matching."""
    import logging

    logger = logging.getLogger(__name__)
    results = []
    pod_name = pod_info.get("name", "unknown")

    try:
        # Get pod logs with time filtering
        log_params = build_log_params_func(search_params)
        pod_logs = await get_pod_logs(namespace, pod_name, **log_params)

        if not pod_logs or "error" in pod_logs:
            return results

        # Logs are keyed by container name, not pod name
        logs_dict = pod_logs.get("logs", {})
        if not logs_dict:
            return results

        # Combine logs from all containers in this pod
        all_logs_lines = []
        for container_name, container_logs in logs_dict.items():
            if isinstance(container_logs, list):
                all_logs_lines.extend(container_logs)
            elif isinstance(container_logs, str):
                all_logs_lines.extend(container_logs.split("\n"))

        if not all_logs_lines:
            return results

        logs_lines = all_logs_lines

        # Perform semantic matching on logs
        # Extract original query terms (words > 2 chars, excluding stop words)
        original_query = query_interpretation.get("original_query", "")
        stop_words = {
            "the",
            "and",
            "for",
            "are",
            "but",
            "not",
            "you",
            "all",
            "can",
            "had",
            "her",
            "was",
            "one",
            "our",
            "out",
            "has",
            "have",
            "from",
            "with",
            "they",
            "been",
            "that",
            "this",
            "what",
            "which",
            "errors",
            "logs",
            "find",
            "show",
            "get",
            "any",
            "pods",
        }
        original_terms = [
            w for w in original_query.lower().split() if len(w) > 2 and w not in stop_words
        ]

        matches = find_semantic_matches_func(
            logs_lines,
            query_interpretation.get("semantic_keywords", []),
            query_interpretation.get("primary_intent", "general_search"),
            search_params.get("max_results", 50),
            original_query_terms=original_terms,
        )

        for match in matches:
            results.append(
                {
                    "source": {
                        "type": "pod_logs",
                        "namespace": namespace,
                        "pod_name": pod_name,
                        "pod_status": pod_info.get("status", "unknown"),
                        "node_name": pod_info.get("node_name", "unknown"),
                    },
                    "log_entry": {
                        "timestamp": match.get("timestamp", "unknown"),
                        "level": match.get("log_level", "info"),
                        "message": match.get("content", ""),
                        "line_number": match.get("line_number", 0),
                    },
                    "relevance_score": match.get("relevance_score", 0),
                    "match_reasons": match.get("match_reasons", []),
                    "related_entries": match.get("related_count", 0),
                }
            )

    except Exception as e:
        logger.warning(f"Error searching pod logs for {pod_name}: {str(e)}")

    return results


async def _search_events_semantically(
    namespace: str,
    query_interpretation: Dict[str, Any],
    search_params: Dict[str, Any],
    get_namespace_events_func,
    calc_semantic_relevance_func,
    identify_match_reasons_func,
    extract_log_metadata_func,
) -> List[Dict[str, Any]]:
    """Search Kubernetes events using semantic matching."""
    import logging

    logger = logging.getLogger(__name__)
    results = []

    try:
        events_response = await get_namespace_events_func(namespace)
        events = events_response.get("events", [])

        if not events:
            return results

        semantic_keywords = query_interpretation.get("semantic_keywords", [])
        primary_intent = query_interpretation.get("primary_intent", "general_search")

        for event_line in events:
            relevance_score = calc_semantic_relevance_func(
                event_line, semantic_keywords, primary_intent
            )

            if relevance_score > 0.2:  # Lower threshold for events
                match_reasons = identify_match_reasons_func(
                    event_line, semantic_keywords, primary_intent
                )

                # Parse event details
                timestamp, level = extract_log_metadata_func(event_line)

                results.append(
                    {
                        "source": {"type": "kubernetes_events", "namespace": namespace},
                        "log_entry": {
                            "timestamp": timestamp,
                            "level": level,
                            "message": event_line,
                            "context_lines": [],
                        },
                        "relevance_score": relevance_score,
                        "match_reasons": match_reasons,
                        "related_entries": 0,
                    }
                )

    except Exception as e:
        logger.warning(f"Error searching events in namespace {namespace}: {str(e)}")

    return results


async def _search_tekton_resources_semantically(
    namespace: str,
    query_interpretation: Dict[str, Any],
    search_params: Dict[str, Any],
    list_pipelineruns,
    calc_semantic_relevance_func,
) -> List[Dict[str, Any]]:
    """Search Tekton resources using semantic matching."""
    import logging

    logger = logging.getLogger(__name__)
    results = []

    try:
        # Search PipelineRuns
        pipeline_runs = await list_pipelineruns(namespace)

        if pipeline_runs and not isinstance(pipeline_runs, dict):
            semantic_keywords = query_interpretation.get("semantic_keywords", [])

            for pr in pipeline_runs:
                pr_text = f"{pr.get('name', '')} {pr.get('status', '')} {pr.get('pipeline', '')}"
                relevance_score = calc_semantic_relevance_func(
                    pr_text, semantic_keywords, "pipeline_monitoring"
                )

                if relevance_score > 0.3:
                    results.append(
                        {
                            "source": {
                                "type": "tekton_pipelinerun",
                                "namespace": namespace,
                                "resource_name": pr.get("name", "unknown"),
                            },
                            "log_entry": {
                                "timestamp": pr.get("started_at", "unknown"),
                                "level": "info" if pr.get("status") == "Succeeded" else "error",
                                "message": f"PipelineRun {pr.get('name', 'unknown')} - Status: {pr.get('status', 'unknown')}, Duration: {pr.get('duration', 'unknown')}",
                                "context_lines": [],
                            },
                            "relevance_score": relevance_score,
                            "match_reasons": ["Tekton resource match"],
                            "related_entries": 0,
                        }
                    )

    except Exception as e:
        logger.warning(f"Error searching Tekton resources in namespace {namespace}: {str(e)}")

    return results
