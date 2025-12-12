# ============================================================================
# LOG ANALYSIS HELPER MODULE
# ============================================================================
#
# This module contains all log analysis related classes, functions, and utilities
# used by the MCP server for log processing and pattern detection.
# ============================================================================

import time
import hashlib
import asyncio
import re
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from collections import Counter, defaultdict

from .constants import LOG_ANALYSIS_CONFIG

# ============================================================================
# LOG ANALYSIS STRATEGY CLASSES
# ============================================================================

class LogAnalysisStrategy(Enum):
    """Available log analysis strategies."""
    SMART_SUMMARY = "smart_summary"
    STREAMING = "streaming"
    HYBRID = "hybrid"
    AUTO = "auto"

@dataclass
class LogAnalysisContext:
    """Context information for strategy selection."""
    log_size_estimate: int
    pod_name: str
    namespace: str
    request_type: str  # "troubleshooting", "monitoring", "investigation"
    urgency: str  # "low", "medium", "high", "critical"
    time_sensitivity: bool
    follow_up_analysis: bool

# ============================================================================
# ANALYSIS CACHE CLASS
# ============================================================================

class AnalysisCache:
    """Simple in-memory cache for analysis results."""

    def __init__(self, max_size: int = 100):
        self.cache = {}
        self.max_size = max_size
        self.access_times = {}

    def _generate_key(self, namespace: str, pod_name: str, params: Dict[str, Any]) -> str:
        """Generate cache key from parameters."""
        key_data = f"{namespace}:{pod_name}:{str(sorted(params.items()))}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def get(self, namespace: str, pod_name: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Retrieve cached result if available and still valid."""
        key = self._generate_key(namespace, pod_name, params)

        if key in self.cache:
            result, timestamp = self.cache[key]
            # Cache valid for 10 minutes
            if time.time() - timestamp < 600:
                self.access_times[key] = time.time()
                return result
            else:
                # Expired, remove from cache
                del self.cache[key]
                if key in self.access_times:
                    del self.access_times[key]

        return None

    def set(self, namespace: str, pod_name: str, params: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Store result in cache."""
        key = self._generate_key(namespace, pod_name, params)

        # Evict oldest entries if cache is full
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
            del self.cache[oldest_key]
            del self.access_times[oldest_key]

        self.cache[key] = (result, time.time())
        self.access_times[key] = time.time()

# ============================================================================
# STRATEGY SELECTOR CLASS
# ============================================================================

class StrategySelector:
    """Intelligent strategy selector based on context and requirements."""

    @staticmethod
    def select_strategy(context: LogAnalysisContext, available_strategies: List[LogAnalysisStrategy]) -> LogAnalysisStrategy:
        """Select optimal strategy based on context."""

        # High urgency always uses streaming for real-time insights
        if context.urgency == "critical" and context.time_sensitivity:
            if LogAnalysisStrategy.STREAMING in available_strategies:
                return LogAnalysisStrategy.STREAMING

        # Large logs benefit from smart summarization
        if context.log_size_estimate > 50000:  # >50k lines
            if LogAnalysisStrategy.SMART_SUMMARY in available_strategies:
                return LogAnalysisStrategy.SMART_SUMMARY

        # Medium-sized logs for troubleshooting work well with streaming
        if context.request_type == "troubleshooting" and context.log_size_estimate > 10000:
            if LogAnalysisStrategy.STREAMING in available_strategies:
                return LogAnalysisStrategy.STREAMING

        # Investigation and monitoring typically use smart summary
        if context.request_type in ["investigation", "monitoring"]:
            if LogAnalysisStrategy.SMART_SUMMARY in available_strategies:
                return LogAnalysisStrategy.SMART_SUMMARY

        # Default to smart summary as it's most versatile
        return LogAnalysisStrategy.SMART_SUMMARY

    @staticmethod
    def estimate_log_size(namespace: str, pod_name: str) -> int:
        """Estimate log size for strategy selection."""
        try:
            # This would need to import the actual get_pod_logs function
            # For now, return a default estimate
            return 10000
        except Exception:
            return 10000  # Default safe estimate

# ============================================================================
# LOG STREAM PROCESSOR CLASS
# ============================================================================

class LogStreamProcessor:
    """Manages streaming log processing with pattern detection."""

    def __init__(self, chunk_size: int = 5000, analysis_mode: str = "errors_and_warnings",
                 max_patterns_per_chunk: int = 100, max_content_length: int = 200):
        self.chunk_size = chunk_size
        self.analysis_mode = analysis_mode
        self.max_patterns_per_chunk = max_patterns_per_chunk
        self.max_content_length = max_content_length
        self.processed_lines = 0
        self.detected_patterns = []
        self.current_chunk = []

    def add_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Add a line to current chunk and return analysis if chunk is complete."""
        self.current_chunk.append(line)
        self.processed_lines += 1

        if len(self.current_chunk) >= self.chunk_size:
            return self._analyze_chunk()
        return None

    def _analyze_chunk(self) -> Dict[str, Any]:
        """Analyze current chunk and return results."""
        chunk_patterns = self._extract_patterns_from_chunk(self.current_chunk)

        result = {
            "chunk_id": len(self.detected_patterns) + 1,
            "lines_processed": len(self.current_chunk),
            "total_lines_processed": self.processed_lines,
            "timestamp": datetime.utcnow().isoformat(),
            "patterns": chunk_patterns,
            "new_issues": self._identify_new_issues(chunk_patterns),
            "chunk_summary": self._summarize_chunk(chunk_patterns)
        }

        self.detected_patterns.append(result)
        self.current_chunk = []  # Reset chunk
        return result

    def _extract_patterns_from_chunk(self, chunk_lines: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Extract patterns from a chunk of log lines with token-aware limits."""
        focus_areas = self._get_focus_areas_for_mode(self.analysis_mode)
        # Calculate max patterns per area based on total limit
        max_per_area = max(10, self.max_patterns_per_chunk // len(focus_areas)) if focus_areas else 10
        return extract_log_patterns(
            chunk_lines,
            focus_areas,
            max_patterns_per_area=max_per_area,
            max_content_length=self.max_content_length
        )

    def _get_focus_areas_for_mode(self, mode: str) -> List[str]:
        """Get focus areas based on analysis mode."""
        mode_mappings = {
            "errors_only": ["errors"],
            "errors_and_warnings": ["errors", "warnings"],
            "full_analysis": ["errors", "warnings", "performance", "exceptions", "timeouts"],
            "custom_patterns": ["errors", "warnings", "performance", "exceptions", "timeouts", "memory_issues", "network_issues"]
        }
        return mode_mappings.get(mode, ["errors", "warnings"])

    def _identify_new_issues(self, chunk_patterns: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Identify new issues not seen in previous chunks (limited to prevent token overflow)."""
        new_issues = []
        max_new_issues = 20  # Limit new issues per chunk to prevent token overflow

        for category, patterns in chunk_patterns.items():
            for pattern in patterns:
                if len(new_issues) >= max_new_issues:
                    break  # Stop if we've found enough new issues

                # Simple new issue detection (could be enhanced with ML)
                pattern_signature = pattern["content"][:100]  # First 100 chars as signature

                # Check if this pattern signature was seen before
                seen_before = any(
                    pattern_signature in str(prev_chunk.get("patterns", {}).get(category, []))
                    for prev_chunk in self.detected_patterns[-5:]  # Check last 5 chunks
                )

                if not seen_before:
                    new_issues.append({
                        "category": category,
                        "pattern": pattern,
                        "severity": self._assess_severity(category, pattern)
                    })

            if len(new_issues) >= max_new_issues:
                break

        return new_issues

    def _assess_severity(self, category: str, pattern: Dict[str, Any]) -> str:
        """Assess severity of an issue."""
        content = pattern["content"].lower()

        if category in ["exceptions", "memory_issues"] or any(word in content for word in ["fatal", "panic", "crash"]):
            return "critical"
        elif category in ["errors", "timeouts"] or any(word in content for word in ["error", "failed", "timeout"]):
            return "high"
        elif category in ["warnings", "performance"]:
            return "medium"
        else:
            return "low"

    def _summarize_chunk(self, chunk_patterns: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Generate summary for the current chunk."""
        total_issues = sum(len(patterns) for patterns in chunk_patterns.values())

        return {
            "total_issues": total_issues,
            "error_count": len(chunk_patterns.get("errors", [])),
            "warning_count": len(chunk_patterns.get("warnings", [])),
            "critical_issues": len([p for patterns in chunk_patterns.values() for p in patterns
                                 if self._assess_severity("", p) == "critical"]),
            "dominant_category": max(chunk_patterns.keys(), key=lambda k: len(chunk_patterns[k])) if chunk_patterns else None
        }

    def finalize(self) -> Optional[Dict[str, Any]]:
        """Process any remaining lines in the current chunk."""
        if self.current_chunk:
            return self._analyze_chunk()
        return None

# ============================================================================
# LOG PATTERN EXTRACTION FUNCTIONS
# ============================================================================

def extract_log_patterns(log_lines: List[str], focus_areas: List[str], max_patterns_per_area: int = 50, max_content_length: int = 200) -> Dict[str, List[Dict[str, Any]]]:
    """Extract patterns from log lines based on focus areas.

    Args:
        log_lines: List of log lines to analyze
        focus_areas: List of focus areas to extract patterns for
        max_patterns_per_area: Maximum number of patterns per area (default: 50)
        max_content_length: Maximum content length per pattern (default: 200 chars)
    """

    patterns = {area: [] for area in focus_areas}

    # Define pattern regex for different categories
    pattern_regex = {
        "errors": [
            r'(?i)error[:|\s](.{0,100})',
            r'(?i)exception[:|\s](.{0,100})',
            r'(?i)failed[:|\s](.{0,100})',
            r'(?i)failure[:|\s](.{0,100})'
        ],
        "warnings": [
            r'(?i)warning[:|\s](.{0,100})',
            r'(?i)warn[:|\s](.{0,100})',
            r'(?i)deprecated[:|\s](.{0,100})'
        ],
        "performance": [
            r'(?i)slow[:|\s](.{0,100})',
            r'(?i)timeout[:|\s](.{0,100})',
            r'(?i)latency[:|\s](.{0,100})',
            r'(?i)bottleneck[:|\s](.{0,100})'
        ],
        "exceptions": [
            r'(?i)panic[:|\s](.{0,100})',
            r'(?i)stacktrace[:|\s](.{0,100})',
            r'(?i)traceback[:|\s](.{0,100})'
        ],
        "timeouts": [
            r'(?i)timeout[:|\s](.{0,100})',
            r'(?i)timed out[:|\s](.{0,100})',
            r'(?i)deadline exceeded[:|\s](.{0,100})'
        ],
        "memory_issues": [
            r'(?i)out of memory[:|\s](.{0,100})',
            r'(?i)oom[:|\s](.{0,100})',
            r'(?i)memory leak[:|\s](.{0,100})'
        ],
        "network_issues": [
            r'(?i)connection refused[:|\s](.{0,100})',
            r'(?i)dns[:|\s](.{0,100})',
            r'(?i)network unreachable[:|\s](.{0,100})'
        ],
        "security": [
            r'(?i)tls[:|\s](.{0,100})',
            r'(?i)certificate[:|\s](.{0,100})',
            r'(?i)ssl[:|\s](.{0,100})',
            r'(?i)x509[:|\s](.{0,100})',
            r'(?i)permission[:|\s](.{0,100})',
            r'(?i)forbidden[:|\s](.{0,100})'
        ]
    }

    for line_num, line in enumerate(log_lines, 1):
        timestamp = extract_timestamp(line)

        for area in focus_areas:
            # Skip if this area already has max patterns
            if len(patterns[area]) >= max_patterns_per_area:
                continue

            if area in pattern_regex:
                for regex in pattern_regex[area]:
                    # Skip if area is already full
                    if len(patterns[area]) >= max_patterns_per_area:
                        break

                    matches = re.findall(regex, line)
                    for match in matches:
                        if len(patterns[area]) >= max_patterns_per_area:
                            break
                        # Truncate content to max_content_length
                        truncated_content = line.strip()[:max_content_length]
                        if len(line.strip()) > max_content_length:
                            truncated_content += "..."
                        patterns[area].append({
                            "line_number": line_num,
                            "timestamp": timestamp,
                            "content": truncated_content,
                            "matched_text": match if isinstance(match, str) else str(match),
                            "severity": assess_log_severity(line)
                        })

    return patterns

def extract_timestamp(log_line: str) -> Optional[str]:
    """Extract timestamp from log line using common patterns."""

    # Common timestamp patterns
    timestamp_patterns = [
        r'(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)',  # ISO format
        r'(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2})',  # MM/DD/YYYY HH:MM:SS
        r'(\w{3}\s\d{1,2}\s\d{2}:\d{2}:\d{2})',     # Mon DD HH:MM:SS
        r'(\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})',        # MM-DD HH:MM:SS
        r'(\d{10,13})',                              # Unix timestamp
    ]

    for pattern in timestamp_patterns:
        match = re.search(pattern, log_line)
        if match:
            return match.group(1)

    return None

def assess_log_severity(log_line: str) -> str:
    """Assess the severity of a log line."""

    line_lower = log_line.lower()

    # Critical indicators
    if any(word in line_lower for word in ["fatal", "panic", "crash", "oom", "killed"]):
        return "critical"

    # High severity indicators
    if any(word in line_lower for word in ["error", "exception", "failed", "failure"]):
        return "high"

    # Medium severity indicators
    if any(word in line_lower for word in ["warning", "warn", "deprecated", "timeout"]):
        return "medium"

    # Low severity (info, debug, etc.)
    return "low"

def sample_logs_by_time(log_lines: List[str], time_segments: int, max_logs_per_segment: int = 100, max_line_length: int = 300) -> Dict[str, List[str]]:
    """Sample logs by dividing into time segments with token-aware limits.

    Args:
        log_lines: List of log lines to segment
        time_segments: Number of time segments to create
        max_logs_per_segment: Maximum number of log lines per segment (default: 100)
        max_line_length: Maximum characters per log line (default: 300)
    """

    if not log_lines or time_segments <= 0:
        return {}

    # Extract timestamps and create segments
    timestamped_logs = []
    for line in log_lines:
        timestamp = extract_timestamp(line)
        if timestamp:
            timestamped_logs.append((timestamp, line))
        else:
            # If no timestamp, use current time as fallback
            timestamped_logs.append((datetime.now().isoformat(), line))

    if not timestamped_logs:
        # Limit even the fallback case
        limited_logs = log_lines[:max_logs_per_segment]
        return {"segment_1": [line[:max_line_length] + ("..." if len(line) > max_line_length else "") for line in limited_logs]}

    # Sort by timestamp
    timestamped_logs.sort(key=lambda x: x[0])

    # Divide into segments
    segment_size = len(timestamped_logs) // time_segments
    segments = {}

    for i in range(time_segments):
        start_idx = i * segment_size
        end_idx = start_idx + segment_size if i < time_segments - 1 else len(timestamped_logs)

        # Get segment logs with limit
        segment_logs_raw = [log for _, log in timestamped_logs[start_idx:end_idx]]

        # Apply sampling if segment exceeds max_logs_per_segment
        if len(segment_logs_raw) > max_logs_per_segment:
            # Sample: first 30%, middle 40%, last 30%
            first_count = max_logs_per_segment * 30 // 100
            middle_count = max_logs_per_segment * 40 // 100
            last_count = max_logs_per_segment - first_count - middle_count

            first_logs = segment_logs_raw[:first_count]
            middle_start = len(segment_logs_raw) // 2 - middle_count // 2
            middle_logs = segment_logs_raw[middle_start:middle_start + middle_count]
            last_logs = segment_logs_raw[-last_count:]

            segment_logs_raw = first_logs + middle_logs + last_logs

        # Truncate long lines
        segment_logs = [
            line[:max_line_length] + ("..." if len(line) > max_line_length else "")
            for line in segment_logs_raw
        ]

        segments[f"segment_{i + 1}"] = segment_logs

    return segments

# ============================================================================
# STREAMING ANALYSIS FUNCTIONS
# ============================================================================

def generate_streaming_summary(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Generate summary from streaming chunk results."""

    if not chunk_results:
        return {"error": "No chunk results to summarize"}

    total_lines = sum(chunk.get("lines_processed", 0) for chunk in chunk_results)
    total_issues = sum(
        chunk.get("chunk_summary", {}).get("total_issues", 0)
        for chunk in chunk_results
    )

    # Aggregate patterns across chunks
    all_patterns = defaultdict(list)
    for chunk in chunk_results:
        patterns = chunk.get("patterns", {})
        for category, pattern_list in patterns.items():
            all_patterns[category].extend(pattern_list)

    # Find most common issues
    error_counter = Counter()
    for patterns in all_patterns.get("errors", []):
        error_counter[patterns.get("matched_text", "unknown")] += 1

    return {
        "total_chunks_processed": len(chunk_results),
        "total_lines_analyzed": total_lines,
        "total_issues_found": total_issues,
        "pattern_categories": list(all_patterns.keys()),
        "most_common_errors": dict(error_counter.most_common(5)),
        "analysis_timespan": {
            "first_chunk": chunk_results[0].get("timestamp"),
            "last_chunk": chunk_results[-1].get("timestamp")
        }
    }

def analyze_trending_patterns(chunk_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze patterns that are trending across chunks."""

    if len(chunk_results) < 2:
        return {"trending": "insufficient_data"}

    # Track pattern frequency across chunks
    pattern_trends = defaultdict(list)

    for chunk in chunk_results:
        chunk_patterns = chunk.get("patterns", {})
        timestamp = chunk.get("timestamp", datetime.now().isoformat())

        for category, patterns in chunk_patterns.items():
            pattern_trends[category].append({
                "timestamp": timestamp,
                "count": len(patterns)
            })

    # Identify increasing trends
    trending_up = {}
    for category, trend_data in pattern_trends.items():
        if len(trend_data) >= 2:
            recent_avg = sum(d["count"] for d in trend_data[-2:]) / 2
            earlier_avg = sum(d["count"] for d in trend_data[:-2]) / max(1, len(trend_data) - 2)

            if recent_avg > earlier_avg * 1.5:  # 50% increase threshold
                trending_up[category] = {
                    "recent_average": recent_avg,
                    "earlier_average": earlier_avg,
                    "trend_strength": recent_avg / max(earlier_avg, 0.1)
                }

    return {
        "trending_up": trending_up,
        "pattern_trends": dict(pattern_trends)
    }

def generate_streaming_recommendations(overall_summary: Dict[str, Any], trending_patterns: Dict[str, Any]) -> List[str]:
    """Generate recommendations based on streaming analysis."""

    recommendations = []

    # High issue count recommendations
    total_issues = overall_summary.get("total_issues_found", 0)
    if total_issues > 100:
        recommendations.append(f"High issue count detected ({total_issues}). Consider reviewing application stability.")

    # Trending pattern recommendations
    trending_up = trending_patterns.get("trending_up", {})
    if "errors" in trending_up:
        recommendations.append("Error rate is increasing. Immediate investigation recommended.")

    if "memory_issues" in trending_up:
        recommendations.append("Memory issues trending up. Check for memory leaks or increase resource limits.")

    if "timeouts" in trending_up:
        recommendations.append("Timeout patterns increasing. Review network connectivity and service dependencies.")

    # Pattern-specific recommendations
    common_errors = overall_summary.get("most_common_errors", {})
    for error, count in common_errors.items():
        if count > 10:
            recommendations.append(f"Frequent error pattern detected: '{error}' ({count} occurrences)")

    if not recommendations:
        recommendations.append("No critical patterns detected. System appears stable.")

    return recommendations

# ============================================================================
# ANALYSIS COMBINATION FUNCTIONS
# ============================================================================

def combine_analysis_results(summary_result: Dict[str, Any], streaming_result: Dict[str, Any]) -> Dict[str, Any]:
    """Combine results from summary and streaming analysis."""

    combined = {
        "analysis_type": "hybrid",
        "summary_analysis": summary_result,
        "streaming_analysis": streaming_result,
        "combined_insights": []
    }

    # Generate combined insights
    insights = []

    # Compare issue counts
    summary_issues = summary_result.get("summary", {}).get("total_issues", 0)
    streaming_issues = streaming_result.get("overall_summary", {}).get("total_issues_found", 0)

    if abs(summary_issues - streaming_issues) > 10:
        insights.append(f"Analysis divergence detected: Summary found {summary_issues} issues, streaming found {streaming_issues}")

    # Check for consistency in error patterns
    summary_errors = set(summary_result.get("patterns", {}).get("errors", {}).keys())
    streaming_errors = set(streaming_result.get("overall_summary", {}).get("most_common_errors", {}).keys())

    common_errors = summary_errors.intersection(streaming_errors)
    if common_errors:
        insights.append(f"Consistent error patterns identified: {', '.join(list(common_errors)[:3])}")

    combined["combined_insights"] = insights
    return combined

def generate_supplementary_insights(primary_results: Dict[str, Any], context: LogAnalysisContext) -> Dict[str, Any]:
    """Generate supplementary insights based on context."""

    insights = {
        "contextual_analysis": [],
        "recommendations": [],
        "follow_up_actions": []
    }

    # Context-specific insights
    if context.request_type == "troubleshooting":
        insights["contextual_analysis"].append("Analysis focused on troubleshooting - prioritizing error patterns")

        error_count = len(primary_results.get("patterns", {}).get("errors", []))
        if error_count > 5:
            insights["recommendations"].append("Multiple error patterns found - recommend systematic investigation")

    elif context.request_type == "monitoring":
        insights["contextual_analysis"].append("Monitoring mode - tracking trends and performance indicators")

        # Check for performance patterns
        perf_issues = len(primary_results.get("patterns", {}).get("performance", []))
        if perf_issues > 0:
            insights["recommendations"].append("Performance issues detected - consider resource optimization")

    # Urgency-based recommendations
    if context.urgency == "critical":
        insights["follow_up_actions"].append("CRITICAL: Immediate escalation and remediation required")
    elif context.urgency == "high":
        insights["follow_up_actions"].append("HIGH: Schedule investigation within 2 hours")

    return insights

def generate_hybrid_recommendations(primary_results: Dict[str, Any], context: LogAnalysisContext, strategy: LogAnalysisStrategy) -> List[str]:
    """Generate recommendations based on hybrid analysis."""

    recommendations = []

    # Strategy-specific recommendations
    if strategy == LogAnalysisStrategy.STREAMING:
        recommendations.append("Real-time analysis completed - monitor for pattern evolution")
    elif strategy == LogAnalysisStrategy.SMART_SUMMARY:
        recommendations.append("Comprehensive analysis completed - detailed patterns extracted")

    # Context-driven recommendations
    total_issues = primary_results.get("summary", {}).get("total_issues", 0)

    if context.urgency == "critical" and total_issues > 10:
        recommendations.append("IMMEDIATE ACTION: High issue count in critical context - activate incident response")

    if context.follow_up_analysis:
        recommendations.append("Follow-up analysis recommended - schedule detailed investigation")

    # Pattern-specific recommendations
    patterns = primary_results.get("patterns", {})

    if "memory_issues" in patterns and len(patterns["memory_issues"]) > 3:
        recommendations.append("Memory issues detected - review resource limits and check for leaks")

    if "network_issues" in patterns and len(patterns["network_issues"]) > 2:
        recommendations.append("Network connectivity issues - verify service mesh and DNS configuration")

    return recommendations

# Create global cache instance
analysis_cache = AnalysisCache(max_size=50)

def generate_focused_summary(patterns: Dict[str, List[Dict[str, Any]]],
                            focus_areas: List[str],
                            summary_level: str) -> Dict[str, Any]:
    """Generate a focused summary based on extracted patterns."""
    summary = {
        "overview": {},
        "key_findings": [],
        "recommendations": [],
        "pattern_counts": {},
        "timeline_analysis": {},
        "critical_issues": []
    }

    # Count patterns
    for category, items in patterns.items():
        summary["pattern_counts"][category] = len(items)

    # Generate overview
    total_issues = sum(len(items) for items in patterns.values())
    error_count = len(patterns.get("errors", []))
    warning_count = len(patterns.get("warnings", []))

    summary["overview"] = {
        "total_issues_found": total_issues,
        "error_count": error_count,
        "warning_count": warning_count,
        "performance_issues": len(patterns.get("performance", [])),
        "critical_categories": [cat for cat, items in patterns.items() if len(items) > 5]
    }

    # Key findings based on summary level
    if summary_level in ["detailed", "comprehensive"]:
        # Add specific error patterns
        for category in focus_areas:
            if category in patterns and patterns[category]:
                # Get most frequent error patterns
                error_messages = [item["content"] for item in patterns[category][:10]]
                summary["key_findings"].append({
                    "category": category,
                    "count": len(patterns[category]),
                    "sample_messages": error_messages[:5]
                })

    # Timeline analysis for comprehensive summaries
    if summary_level == "comprehensive":
        timestamps = []
        for category, items in patterns.items():
            for item in items:
                if item.get("timestamp"):
                    timestamps.append({
                        "timestamp": item["timestamp"],
                        "category": category,
                        "line": item["line_number"]
                    })

        if timestamps:
            # Sort by timestamp (simplified)
            timestamps.sort(key=lambda x: x["timestamp"])
            summary["timeline_analysis"] = {
                "first_issue": timestamps[0] if timestamps else None,
                "last_issue": timestamps[-1] if timestamps else None,
                "issue_distribution": {}
            }

    # Critical issues (high-priority items)
    critical_patterns = ["exceptions", "timeouts", "memory_issues"]
    for pattern in critical_patterns:
        if pattern in patterns and patterns[pattern]:
            summary["critical_issues"].extend(patterns[pattern][:3])  # Top 3 critical issues

    # Recommendations
    if error_count > 10:
        summary["recommendations"].append("High error count detected. Investigate application stability.")
    if len(patterns.get("memory_issues", [])) > 0:
        summary["recommendations"].append("Memory issues detected. Consider increasing pod memory limits or investigating memory leaks.")
    if len(patterns.get("timeouts", [])) > 5:
        summary["recommendations"].append("Multiple timeout issues found. Check network connectivity and service dependencies.")
    if len(patterns.get("performance", [])) > 5:
        summary["recommendations"].append("Performance issues detected. Consider resource optimization or scaling.")

    return summary

def get_strategy_selection_reason(context: LogAnalysisContext, strategy: LogAnalysisStrategy) -> str:
    """Get explanation for why a strategy was selected."""
    if strategy == LogAnalysisStrategy.STREAMING:
        if context.urgency == "critical":
            return "Streaming selected for critical urgency requiring immediate insights"
        elif context.request_type == "troubleshooting":
            return "Streaming selected for real-time troubleshooting support"
        else:
            return "Streaming selected for progressive analysis of medium-sized logs"

    elif strategy == LogAnalysisStrategy.SMART_SUMMARY:
        if context.log_size_estimate > 50000:
            return "Smart summary selected for large log size requiring efficient processing"
        elif context.request_type in ["investigation", "monitoring"]:
            return f"Smart summary selected for {context.request_type} requiring comprehensive analysis"
        else:
            return "Smart summary selected as versatile default strategy"

    elif strategy == LogAnalysisStrategy.HYBRID:
        return "Hybrid strategy selected for comprehensive analysis requiring multiple approaches"

    else:
        return "Strategy selected based on automatic optimization"

def preprocess_log_data(log_lines: List[str]) -> pd.DataFrame:
    """Preprocess log data for ML analysis."""
    processed_data = []

    for line in log_lines:
        # Extract timestamp if present
        timestamp_match = re.search(r'\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}', line)
        timestamp = timestamp_match.group() if timestamp_match else None

        # Extract log level
        level_match = re.search(r'\b(DEBUG|INFO|WARN|ERROR|FATAL|PANIC)\b', line, re.IGNORECASE)
        log_level = level_match.group().upper() if level_match else 'UNKNOWN'

        # Extract error patterns
        error_indicators = len(re.findall(r'\b(error|exception|failed|fatal|panic|timeout)\b', line, re.IGNORECASE))

        # Calculate message length and entropy
        message_length = len(line)
        message_entropy = calculate_entropy(line)

        processed_data.append({
            'timestamp': timestamp,
            'log_level': log_level,
            'error_indicators': error_indicators,
            'message_length': message_length,
            'message_entropy': message_entropy,
            'raw_message': line
        })

    return pd.DataFrame(processed_data)

def calculate_entropy(text: str) -> float:
    """Calculate Shannon entropy of text."""
    if not text:
        return 0.0

    # Count character frequencies
    char_counts = {}
    for char in text:
        char_counts[char] = char_counts.get(char, 0) + 1

    # Calculate entropy
    text_length = len(text)
    entropy = 0.0
    for count in char_counts.values():
        probability = count / text_length
        if probability > 0:
            entropy -= probability * np.log2(probability)

    return entropy

def extract_log_features(df: pd.DataFrame) -> np.ndarray:
    """Extract features from preprocessed log data."""
    features = []

    # Time-based features
    df['hour'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.hour
    df['minute'] = pd.to_datetime(df['timestamp'], errors='coerce').dt.minute

    # Rolling window features (last 10 messages)
    window_size = 10
    df['error_rate_window'] = df['error_indicators'].rolling(window=window_size, min_periods=1).mean()
    df['avg_length_window'] = df['message_length'].rolling(window=window_size, min_periods=1).mean()
    df['entropy_trend'] = df['message_entropy'].rolling(window=window_size, min_periods=1).std()

    # Log level encoding
    level_encoding = {'DEBUG': 0, 'INFO': 1, 'WARN': 2, 'ERROR': 3, 'FATAL': 4, 'PANIC': 5, 'UNKNOWN': 0}
    df['log_level_encoded'] = df['log_level'].map(level_encoding)

    # Select feature columns
    feature_columns = [
        'error_indicators', 'message_length', 'message_entropy',
        'hour', 'minute', 'error_rate_window', 'avg_length_window',
        'entropy_trend', 'log_level_encoded'
    ]

    return df[feature_columns].fillna(0).values

def train_anomaly_model(features: np.ndarray, contamination: float = 0.1):
    """Train isolation forest model for anomaly detection."""
    from sklearn.ensemble import IsolationForest
    model = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=100
    )
    model.fit(features)
    return model

def analyze_log_patterns_for_failure_prediction(log_data: pd.DataFrame, historical_failures: List[Dict]) -> Dict[str, Any]:
    """Analyze log patterns to predict potential failures."""
    failure_patterns = []

    # Pattern 1: High error rate
    error_rate = log_data['error_indicators'].mean()
    if error_rate > 0.1:  # More than 10% error indicators
        failure_patterns.append({
            'pattern': 'high_error_rate',
            'severity': 'high' if error_rate > 0.3 else 'medium',
            'value': error_rate
        })

    # Pattern 2: Entropy spikes (indicating unusual log patterns)
    entropy_threshold = log_data['message_entropy'].mean() + 2 * log_data['message_entropy'].std()
    entropy_spikes = (log_data['message_entropy'] > entropy_threshold).sum()
    if entropy_spikes > len(log_data) * 0.05:  # More than 5% of logs are entropy spikes
        failure_patterns.append({
            'pattern': 'entropy_spikes',
            'severity': 'medium',
            'value': entropy_spikes / len(log_data)
        })

    # Pattern 3: Message length anomalies
    length_threshold = log_data['message_length'].mean() + 3 * log_data['message_length'].std()
    length_anomalies = (log_data['message_length'] > length_threshold).sum()
    if length_anomalies > 0:
        failure_patterns.append({
            'pattern': 'message_length_anomalies',
            'severity': 'low',
            'value': length_anomalies
        })

    return {
        'failure_patterns': failure_patterns,
        'risk_score': sum(1 for p in failure_patterns if p['severity'] == 'high') * 0.5 +
                     sum(1 for p in failure_patterns if p['severity'] == 'medium') * 0.3 +
                     sum(1 for p in failure_patterns if p['severity'] == 'low') * 0.1
    }

def generate_failure_predictions(patterns: Dict[str, Any], confidence_threshold: float, prediction_window: str) -> List[Dict[str, Any]]:
    """Generate failure predictions based on detected patterns."""
    predictions = []

    risk_score = patterns['risk_score']
    confidence = min(risk_score * 0.8, 0.95)  # Cap confidence at 95%

    if confidence >= confidence_threshold:
        # Calculate predicted time based on window
        window_hours = {
            "1h": 1, "6h": 6, "24h": 24, "7d": 168
        }.get(prediction_window, 6)

        predicted_time = (datetime.now() + timedelta(hours=window_hours)).isoformat()

        # Determine failure type based on patterns
        failure_types = []
        affected_components = []
        warning_indicators = []
        recommended_actions = []

        for pattern in patterns['failure_patterns']:
            if pattern['pattern'] == 'high_error_rate':
                failure_types.append('service_degradation')
                affected_components.append('application_pods')
                warning_indicators.append(f"Error rate at {pattern['value']:.2%}")
                recommended_actions.append("Investigate error logs and increase monitoring")
            elif pattern['pattern'] == 'entropy_spikes':
                failure_types.append('unusual_behavior')
                affected_components.append('logging_system')
                warning_indicators.append(f"Entropy spikes in {pattern['value']:.2%} of logs")
                recommended_actions.append("Check for configuration changes or new deployments")

        predictions.append({
            'failure_type': failure_types[0] if failure_types else 'general_failure',
            'predicted_time': predicted_time,
            'confidence': confidence,
            'affected_components': affected_components,
            'warning_indicators': warning_indicators,
            'recommended_actions': recommended_actions
        })

    return predictions

def _build_log_params(search_params: Dict[str, Any]) -> Dict[str, Any]:
    """Build log retrieval parameters from search params."""
    time_range = search_params.get('time_range', '1h')

    # Convert time range to seconds
    time_mapping = {
        '1h': 3600,
        '6h': 21600,
        '24h': 86400,
        '7d': 604800
    }

    since_seconds = time_mapping.get(time_range, 3600)

    return {
        'since_seconds': since_seconds,
        'tail_lines': 500  # Reasonable limit for semantic analysis
    }


# ============================================================================
# TOKEN LIMIT TRUNCATION FUNCTIONS
# ============================================================================

def truncate_to_token_limit(data: Dict[str, Any], max_tokens: int, chars_per_token: int = 4) -> Dict[str, Any]:
    """Truncate response data to fit within token limit.

    Args:
        data: The response dictionary to truncate
        max_tokens: Maximum number of tokens allowed
        chars_per_token: Estimated characters per token (default: 4)

    Returns:
        Truncated data that fits within the token limit
    """
    import json

    # Estimate current size
    try:
        current_chars = len(json.dumps(data, default=str))
        current_tokens = current_chars // chars_per_token
    except (TypeError, ValueError):
        current_tokens = max_tokens + 1  # Force truncation if serialization fails

    if current_tokens <= max_tokens:
        return data

    # Create a copy to avoid modifying original
    result = data.copy()

    # Progressive truncation strategy
    # Stage 1: Truncate patterns to top N per category
    if 'patterns' in result and isinstance(result['patterns'], dict):
        max_per_category = max(5, max_tokens // 200)  # Scale with token limit
        for category in result['patterns']:
            if isinstance(result['patterns'][category], list):
                result['patterns'][category] = result['patterns'][category][:max_per_category]
                # Also truncate content within each pattern
                for pattern in result['patterns'][category]:
                    if isinstance(pattern, dict) and 'content' in pattern:
                        pattern['content'] = pattern['content'][:150] + "..." if len(pattern.get('content', '')) > 150 else pattern.get('content', '')

    # Check size after stage 1
    try:
        current_tokens = len(json.dumps(result, default=str)) // chars_per_token
    except (TypeError, ValueError):
        pass

    if current_tokens <= max_tokens:
        result['_truncated'] = True
        result['_truncation_stage'] = 1
        return result

    # Stage 2: Convert time_segments to counts only
    if 'time_segments' in result and isinstance(result['time_segments'], dict):
        result['time_segments'] = {
            k: len(v) if isinstance(v, list) else v
            for k, v in result['time_segments'].items()
        }
        result['time_segments']['_note'] = 'Counts only - full logs truncated for token limit'

    # Check size after stage 2
    try:
        current_tokens = len(json.dumps(result, default=str)) // chars_per_token
    except (TypeError, ValueError):
        pass

    if current_tokens <= max_tokens:
        result['_truncated'] = True
        result['_truncation_stage'] = 2
        return result

    # Stage 3: Truncate representative_samples
    if 'representative_samples' in result and isinstance(result['representative_samples'], list):
        max_samples = max(3, max_tokens // 500)
        result['representative_samples'] = result['representative_samples'][:max_samples]
        for sample in result['representative_samples']:
            if isinstance(sample, dict) and 'content' in sample:
                sample['content'] = sample['content'][:100] + "..." if len(sample.get('content', '')) > 100 else sample.get('content', '')

    # Check size after stage 3
    try:
        current_tokens = len(json.dumps(result, default=str)) // chars_per_token
    except (TypeError, ValueError):
        pass

    if current_tokens <= max_tokens:
        result['_truncated'] = True
        result['_truncation_stage'] = 3
        return result

    # Stage 4: Truncate chunk results (for streaming analysis)
    if 'chunks' in result and isinstance(result['chunks'], list):
        max_chunks = max(3, max_tokens // 1000)
        result['chunks'] = result['chunks'][:max_chunks]
        # Truncate patterns within each chunk
        for chunk in result['chunks']:
            if isinstance(chunk, dict) and 'patterns' in chunk:
                for category in chunk['patterns']:
                    if isinstance(chunk['patterns'][category], list):
                        chunk['patterns'][category] = chunk['patterns'][category][:5]

    # Stage 5: Remove large metadata fields if still too large
    try:
        current_tokens = len(json.dumps(result, default=str)) // chars_per_token
    except (TypeError, ValueError):
        pass

    if current_tokens > max_tokens:
        # Remove optional large fields
        fields_to_trim = ['raw_logs', 'full_timeline', 'detailed_analysis', 'chunk_details']
        for field in fields_to_trim:
            if field in result:
                del result[field]

    result['_truncated'] = True
    result['_truncation_stage'] = 'final'
    result['_original_token_estimate'] = current_tokens
    result['_max_tokens'] = max_tokens

    return result


def truncate_streaming_results(chunk_results: List[Dict[str, Any]], max_tokens: int) -> List[Dict[str, Any]]:
    """Truncate streaming chunk results to fit within token limit.

    Args:
        chunk_results: List of chunk analysis results
        max_tokens: Maximum number of tokens allowed

    Returns:
        Truncated list of chunk results
    """
    import json

    if not chunk_results:
        return chunk_results

    chars_per_token = 4

    # Estimate current size
    try:
        current_tokens = len(json.dumps(chunk_results, default=str)) // chars_per_token
    except (TypeError, ValueError):
        current_tokens = max_tokens + 1

    if current_tokens <= max_tokens:
        return chunk_results

    # Calculate how many chunks we can afford
    avg_tokens_per_chunk = current_tokens // len(chunk_results) if chunk_results else 1
    max_chunks = max(3, max_tokens // max(avg_tokens_per_chunk, 100))

    # Keep most recent chunks (they're likely more relevant)
    truncated = chunk_results[-max_chunks:]

    # Further truncate patterns within each chunk
    for chunk in truncated:
        if 'patterns' in chunk and isinstance(chunk['patterns'], dict):
            for category in chunk['patterns']:
                if isinstance(chunk['patterns'][category], list):
                    chunk['patterns'][category] = chunk['patterns'][category][:10]

        if 'new_issues' in chunk and isinstance(chunk['new_issues'], list):
            chunk['new_issues'] = chunk['new_issues'][:5]

    return truncated
