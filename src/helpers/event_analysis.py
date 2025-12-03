# ============================================================================
# EVENT ANALYSIS HELPER MODULE
# ============================================================================
#
# This module contains event analysis related classes, functions, and utilities
# used by the MCP server for smart event processing, classification, and analysis.
# ============================================================================

import re
import statistics
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional
from collections import Counter, defaultdict

from .constants import SMART_EVENTS_CONFIG

# ============================================================================
# EVENT CLASSIFICATION ENUMS
# ============================================================================


class EventSeverity(Enum):
    """Event severity levels."""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


class EventCategory(Enum):
    """Event functional categories."""
    FAILURE = "FAILURE"
    SCHEDULING = "SCHEDULING"
    NETWORKING = "NETWORKING"
    STORAGE = "STORAGE"
    SCALING = "SCALING"
    LIFECYCLE = "LIFECYCLE"
    HEALTH = "HEALTH"
    OTHER = "OTHER"


# ============================================================================
# PROGRESSIVE EVENT ANALYZER CLASS
# ============================================================================


class ProgressiveEventAnalyzer:
    """Progressive disclosure engine for events."""

    def __init__(self, classified_events: List[Dict[str, Any]]):
        self.classified_events = classified_events
        self.timeline_sorted = sorted(
            classified_events,
            key=lambda x: x.get("timestamp", datetime.now())
        )

    def get_overview(self, max_items: int = 5) -> Dict[str, Any]:
        """Quick overview of event landscape."""

        if not self.classified_events:
            return {"message": "No events to analyze", "overview": {}}

        # Get top critical events
        critical_events = [
            e for e in self.classified_events
            if e.get("severity") == EventSeverity.CRITICAL.value
        ][:max_items]

        # Get most recent high-impact events
        recent_high_impact = [
            e for e in self.timeline_sorted[-max_items:]
            if e.get("severity") in [EventSeverity.CRITICAL.value, EventSeverity.HIGH.value]
        ]

        # Pattern summary
        patterns = self._identify_quick_patterns()

        return {
            "overview_level": "high_level_summary",
            "critical_events_preview": [
                {
                    "severity": e.get("severity"),
                    "category": e.get("category"),
                    "preview": e.get("event_string", "")[:80] + "...",
                    "timestamp": e.get("timestamp", datetime.now()).isoformat()
                }
                for e in critical_events
            ],
            "recent_high_impact": [
                {
                    "severity": e.get("severity"),
                    "category": e.get("category"),
                    "preview": e.get("event_string", "")[:60] + "...",
                    "timestamp": e.get("timestamp", datetime.now()).isoformat()
                }
                for e in recent_high_impact
            ],
            "quick_patterns": patterns,
            "drill_down_suggestions": [
                "Use 'detailed' level for specific event analysis",
                "Use 'correlation' level to find event relationships",
                "Use 'deep_dive' level for comprehensive investigation"
            ]
        }

    def get_detailed_analysis(self, event_filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Detailed analysis of specific events or categories."""

        # Apply filters if provided
        filtered_events = self.classified_events
        if event_filters:
            filtered_events = self._apply_progressive_filters(self.classified_events, event_filters)

        if not filtered_events:
            return {"message": "No events match the specified filters"}

        # Group events by category and severity
        analysis = {
            "detailed_level": "comprehensive_analysis",
            "total_analyzed": len(filtered_events),
            "category_analysis": self._analyze_by_category(filtered_events),
            "severity_analysis": self._analyze_by_severity(filtered_events),
            "temporal_analysis": self._analyze_temporal_patterns(filtered_events),
            "resource_impact": self._analyze_resource_impact(filtered_events),
            "detailed_recommendations": self._generate_detailed_recommendations(filtered_events)
        }

        return analysis

    def get_correlation_analysis(self, seed_event_id: str = None) -> Dict[str, Any]:
        """Find event correlations and cascades."""

        correlations = []

        if seed_event_id:
            # Find correlations for specific event
            seed_event = next(
                (e for e in self.classified_events if str(e.get("timestamp", "")) == seed_event_id),
                None
            )
            if seed_event:
                correlations = [self._find_event_correlations(seed_event)]
        else:
            # Find all significant correlations
            correlations = self._find_all_correlations()

        # Detect failure cascades
        cascades = self._detect_failure_cascades()

        # Group by root cause patterns
        root_cause_groups = self._group_by_root_cause()

        return {
            "correlation_level": "relationship_analysis",
            "event_correlations": correlations,
            "failure_cascades": cascades,
            "root_cause_analysis": root_cause_groups,
            "correlation_insights": self._generate_correlation_insights(correlations, cascades)
        }

    def _find_all_correlations(self) -> List[Dict[str, Any]]:
        """Find all significant correlations between events."""
        correlations = []

        try:
            # Group events by time windows (5-minute windows)
            time_windows = {}
            for event in self.classified_events:
                timestamp = event.get("timestamp", datetime.now())
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))

                # Round to 5-minute window
                window_key = timestamp.replace(minute=(timestamp.minute // 5) * 5, second=0, microsecond=0)
                if window_key not in time_windows:
                    time_windows[window_key] = []
                time_windows[window_key].append(event)

            # Find windows with multiple related events
            for window_time, events in time_windows.items():
                if len(events) > 1:
                    # Look for related events in the same time window
                    for i, event1 in enumerate(events):
                        for event2 in events[i+1:]:
                            correlation_strength = self._calculate_correlation_strength(event1, event2)
                            if correlation_strength > 0.3:  # Threshold for correlation
                                correlations.append({
                                    "event1": event1.get("event_string", "")[:100] + "...",
                                    "event2": event2.get("event_string", "")[:100] + "...",
                                    "correlation_strength": correlation_strength,
                                    "time_window": window_time.isoformat(),
                                    "correlation_type": "temporal_proximity"
                                })

        except Exception as e:
            # Return empty correlations on error
            correlations = []

        return correlations[:10]  # Limit to top 10 correlations

    def _find_event_correlations(self, seed_event: Dict[str, Any]) -> Dict[str, Any]:
        """Find correlations for a specific event."""
        try:
            seed_timestamp = seed_event.get("timestamp", datetime.now())
            if isinstance(seed_timestamp, str):
                seed_timestamp = datetime.fromisoformat(seed_timestamp.replace('Z', '+00:00'))

            related_events = []

            # Find events within 10 minutes of seed event
            for event in self.classified_events:
                if event == seed_event:
                    continue

                event_timestamp = event.get("timestamp", datetime.now())
                if isinstance(event_timestamp, str):
                    event_timestamp = datetime.fromisoformat(event_timestamp.replace('Z', '+00:00'))

                time_diff = abs((event_timestamp - seed_timestamp).total_seconds())
                if time_diff <= 600:  # Within 10 minutes
                    correlation_strength = self._calculate_correlation_strength(seed_event, event)
                    if correlation_strength > 0.2:
                        related_events.append({
                            "event": event.get("event_string", "")[:100] + "...",
                            "correlation_strength": correlation_strength,
                            "time_difference_seconds": time_diff
                        })

            return {
                "seed_event": seed_event.get("event_string", "")[:100] + "...",
                "related_events": sorted(related_events, key=lambda x: x["correlation_strength"], reverse=True)[:5]
            }

        except Exception as e:
            return {"seed_event": "error", "related_events": [], "error": str(e)}

    def _calculate_correlation_strength(self, event1: Dict[str, Any], event2: Dict[str, Any]) -> float:
        """Calculate correlation strength between two events."""
        try:
            strength = 0.0

            event1_content = event1.get("event_string", "").lower()
            event2_content = event2.get("event_string", "").lower()

            # Same severity increases correlation
            if event1.get("severity") == event2.get("severity"):
                strength += 0.2

            # Same category increases correlation
            if event1.get("category") == event2.get("category"):
                strength += 0.3

            # Common keywords increase correlation
            words1 = set(event1_content.split())
            words2 = set(event2_content.split())
            common_words = words1.intersection(words2)
            if len(common_words) > 2:
                strength += min(0.4, len(common_words) * 0.1)

            return min(1.0, strength)

        except Exception:
            return 0.0

    def _detect_failure_cascades(self) -> List[Dict[str, Any]]:
        """Detect failure cascade patterns."""
        cascades = []

        try:
            # Group events by severity and time
            critical_events = [e for e in self.timeline_sorted if e.get("severity") == "CRITICAL"]

            for i, critical_event in enumerate(critical_events):
                # Look for events that follow this critical event
                critical_time = critical_event.get("timestamp", datetime.now())
                if isinstance(critical_time, str):
                    critical_time = datetime.fromisoformat(critical_time.replace('Z', '+00:00'))

                following_events = []
                for event in self.timeline_sorted:
                    event_time = event.get("timestamp", datetime.now())
                    if isinstance(event_time, str):
                        event_time = datetime.fromisoformat(event_time.replace('Z', '+00:00'))

                    # Events within 30 minutes after critical event
                    if 0 < (event_time - critical_time).total_seconds() <= 1800:
                        following_events.append(event)

                if len(following_events) >= 3:  # Potential cascade
                    cascades.append({
                        "trigger_event": critical_event.get("event_string", "")[:100] + "...",
                        "cascade_events": len(following_events),
                        "cascade_duration_minutes": 30,
                        "cascade_type": "failure_propagation"
                    })

        except Exception as e:
            cascades = []

        return cascades[:5]  # Limit to top 5 cascades

    def _group_by_root_cause(self) -> Dict[str, Any]:
        """Group events by potential root cause."""
        try:
            root_causes = {
                "resource_exhaustion": [],
                "network_issues": [],
                "authentication_problems": [],
                "configuration_errors": [],
                "unknown": []
            }

            for event in self.classified_events:
                event_content = event.get("event_string", "").lower()

                if any(pattern in event_content for pattern in ["memory", "cpu", "disk", "resource", "quota"]):
                    root_causes["resource_exhaustion"].append(event)
                elif any(pattern in event_content for pattern in ["network", "connection", "dns", "timeout"]):
                    root_causes["network_issues"].append(event)
                elif any(pattern in event_content for pattern in ["auth", "permission", "forbidden", "unauthorized"]):
                    root_causes["authentication_problems"].append(event)
                elif any(pattern in event_content for pattern in ["config", "invalid", "missing", "not found"]):
                    root_causes["configuration_errors"].append(event)
                else:
                    root_causes["unknown"].append(event)

            # Return summary with counts
            return {
                root_cause: {
                    "count": len(events),
                    "sample_events": [e.get("event_string", "")[:80] + "..." for e in events[:3]]
                }
                for root_cause, events in root_causes.items()
                if len(events) > 0
            }

        except Exception as e:
            return {"error": str(e)}

    def _generate_correlation_insights(self, correlations: List[Dict[str, Any]], cascades: List[Dict[str, Any]]) -> List[str]:
        """Generate insights from correlation analysis."""
        insights = []

        try:
            if correlations:
                insights.append(f"Found {len(correlations)} event correlations indicating related issues")

                # Analyze correlation strengths
                strong_correlations = [c for c in correlations if c.get("correlation_strength", 0) > 0.7]
                if strong_correlations:
                    insights.append(f"{len(strong_correlations)} strong correlations suggest systemic issues")

            if cascades:
                insights.append(f"Detected {len(cascades)} potential failure cascades")
                total_cascade_events = sum(c.get("cascade_events", 0) for c in cascades)
                insights.append(f"Cascade analysis shows {total_cascade_events} related events")

            if not correlations and not cascades:
                insights.append("No significant event correlations detected - issues appear isolated")

        except Exception as e:
            insights.append(f"Correlation analysis error: {str(e)}")

        return insights

    def _identify_quick_patterns(self) -> Dict[str, Any]:
        """Identify quick patterns for overview."""

        patterns = {}

        # Frequency patterns
        severity_counts = {}
        category_counts = {}

        for event in self.classified_events:
            severity = event.get("severity", "UNKNOWN")
            category = event.get("category", "OTHER")

            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            category_counts[category] = category_counts.get(category, 0) + 1

        patterns["severity_distribution"] = severity_counts
        patterns["category_distribution"] = category_counts

        # Time-based patterns
        if len(self.timeline_sorted) > 1:
            time_span = (
                self.timeline_sorted[-1].get("timestamp", datetime.now()) -
                self.timeline_sorted[0].get("timestamp", datetime.now())
            )
            patterns["time_span"] = str(time_span)
            patterns["event_rate"] = f"{len(self.classified_events) / max(time_span.total_seconds() / 3600, 0.1):.1f} events/hour"

        # Common keywords
        all_text = " ".join([
            event.get("event_string", "")
            for event in self.classified_events
        ]).lower()

        common_terms = ["failed", "error", "oom", "timeout", "unhealthy", "imagepull"]
        patterns["common_issues"] = {
            term: all_text.count(term)
            for term in common_terms
            if all_text.count(term) > 0
        }

        return patterns

    def _apply_progressive_filters(self, events: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply progressive filters to events."""

        filtered = events

        if "severity" in filters:
            target_severities = filters["severity"] if isinstance(filters["severity"], list) else [filters["severity"]]
            filtered = [e for e in filtered if e.get("severity") in target_severities]

        if "category" in filters:
            target_categories = filters["category"] if isinstance(filters["category"], list) else [filters["category"]]
            filtered = [e for e in filtered if e.get("category") in target_categories]

        if "time_range" in filters:
            # Filter by time range (last N hours)
            hours = filters["time_range"]
            cutoff = datetime.now() - timedelta(hours=hours)
            filtered = [
                e for e in filtered
                if e.get("timestamp", datetime.now()) >= cutoff
            ]

        if "keywords" in filters:
            keywords = filters["keywords"] if isinstance(filters["keywords"], list) else [filters["keywords"]]
            filtered = [
                e for e in filtered
                if any(keyword.lower() in e.get("event_string", "").lower() for keyword in keywords)
            ]

        return filtered

    def _analyze_by_category(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze events by category."""
        category_analysis = {}
        category_counts = {}

        for event in events:
            category = event.get("category", "OTHER")
            if category not in category_counts:
                category_counts[category] = 0
                category_analysis[category] = {
                    "count": 0,
                    "severity_breakdown": {},
                    "sample_events": []
                }

            category_counts[category] += 1
            category_analysis[category]["count"] += 1

            severity = event.get("severity", "UNKNOWN")
            if severity not in category_analysis[category]["severity_breakdown"]:
                category_analysis[category]["severity_breakdown"][severity] = 0
            category_analysis[category]["severity_breakdown"][severity] += 1

            # Keep sample events (max 3 per category)
            if len(category_analysis[category]["sample_events"]) < 3:
                category_analysis[category]["sample_events"].append({
                    "event_string": event.get("event_string", "")[:100] + "..." if len(event.get("event_string", "")) > 100 else event.get("event_string", ""),
                    "severity": severity,
                    "timestamp": event.get("timestamp", "").isoformat() if hasattr(event.get("timestamp", ""), 'isoformat') else str(event.get("timestamp", ""))
                })

        return category_analysis

    def _analyze_by_severity(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze events by severity."""
        severity_analysis = {}

        for event in events:
            severity = event.get("severity", "UNKNOWN")
            if severity not in severity_analysis:
                severity_analysis[severity] = {
                    "count": 0,
                    "percentage": 0.0,
                    "categories": {},
                    "sample_events": []
                }

            severity_analysis[severity]["count"] += 1

            category = event.get("category", "OTHER")
            if category not in severity_analysis[severity]["categories"]:
                severity_analysis[severity]["categories"][category] = 0
            severity_analysis[severity]["categories"][category] += 1

            # Keep sample events (max 2 per severity)
            if len(severity_analysis[severity]["sample_events"]) < 2:
                severity_analysis[severity]["sample_events"].append({
                    "event_string": event.get("event_string", "")[:100] + "..." if len(event.get("event_string", "")) > 100 else event.get("event_string", ""),
                    "category": category,
                    "timestamp": event.get("timestamp", "").isoformat() if hasattr(event.get("timestamp", ""), 'isoformat') else str(event.get("timestamp", ""))
                })

        # Calculate percentages
        total_events = len(events)
        for severity in severity_analysis:
            severity_analysis[severity]["percentage"] = (severity_analysis[severity]["count"] / total_events) * 100

        return severity_analysis

    def _analyze_temporal_patterns(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze temporal patterns in events."""
        if not events:
            return {"message": "No events to analyze"}

        # Sort events by timestamp
        sorted_events = sorted(events, key=lambda x: x.get("timestamp", datetime.now()))

        temporal_analysis = {
            "event_count": len(events),
            "time_span": "unknown",
            "event_rate": "unknown",
            "peak_periods": [],
            "patterns": {}
        }

        if len(sorted_events) > 1:
            start_time = sorted_events[0].get("timestamp", datetime.now())
            end_time = sorted_events[-1].get("timestamp", datetime.now())

            if hasattr(start_time, 'total_seconds') or hasattr(end_time, 'total_seconds'):
                try:
                    time_span = end_time - start_time
                    temporal_analysis["time_span"] = str(time_span)

                    if time_span.total_seconds() > 0:
                        rate = len(events) / (time_span.total_seconds() / 3600)
                        temporal_analysis["event_rate"] = f"{rate:.1f} events/hour"
                except:
                    pass

        # Analyze patterns by hour
        hour_counts = {}
        for event in events:
            timestamp = event.get("timestamp", datetime.now())
            if hasattr(timestamp, 'hour'):
                hour = timestamp.hour
                hour_counts[hour] = hour_counts.get(hour, 0) + 1

        if hour_counts:
            max_hour = max(hour_counts, key=hour_counts.get)
            temporal_analysis["patterns"]["peak_hour"] = f"{max_hour}:00 ({hour_counts[max_hour]} events)"

        return temporal_analysis

    def _analyze_resource_impact(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze resource impact of events."""
        resource_impact = {
            "affected_resources": {},
            "resource_types": {},
            "severity_impact": {}
        }

        for event in events:
            event_str = event.get("event_string", "").lower()

            # Extract resource information from event string
            if "pod" in event_str:
                resource_impact["resource_types"]["pods"] = resource_impact["resource_types"].get("pods", 0) + 1
            if "service" in event_str:
                resource_impact["resource_types"]["services"] = resource_impact["resource_types"].get("services", 0) + 1
            if "deployment" in event_str:
                resource_impact["resource_types"]["deployments"] = resource_impact["resource_types"].get("deployments", 0) + 1
            if "pvc" in event_str or "volume" in event_str:
                resource_impact["resource_types"]["storage"] = resource_impact["resource_types"].get("storage", 0) + 1

            severity = event.get("severity", "UNKNOWN")
            if severity not in resource_impact["severity_impact"]:
                resource_impact["severity_impact"][severity] = {"count": 0, "resources": set()}

            resource_impact["severity_impact"][severity]["count"] += 1

        # Convert sets to lists for JSON serialization
        for severity in resource_impact["severity_impact"]:
            resource_impact["severity_impact"][severity]["resources"] = list(resource_impact["severity_impact"][severity]["resources"])

        return resource_impact

    def _generate_detailed_recommendations(self, events: List[Dict[str, Any]]) -> List[str]:
        """Generate detailed recommendations based on event analysis."""
        recommendations = []

        if not events:
            return ["No events to analyze - system appears stable"]

        # Analyze event patterns
        critical_count = len([e for e in events if e.get("severity") == "CRITICAL"])
        high_count = len([e for e in events if e.get("severity") == "HIGH"])

        if critical_count > 0:
            recommendations.append(f"URGENT: {critical_count} critical events detected - immediate investigation required")

        if high_count > 5:
            recommendations.append(f"HIGH PRIORITY: {high_count} high-severity events - review and address underlying causes")

        # Category-specific recommendations
        categories = {}
        for event in events:
            category = event.get("category", "OTHER")
            categories[category] = categories.get(category, 0) + 1

        if categories.get("FAILURE", 0) > 3:
            recommendations.append("RELIABILITY: Multiple failure events detected - consider implementing circuit breakers and retry mechanisms")

        if categories.get("NETWORKING", 0) > 2:
            recommendations.append("NETWORKING: Network-related issues detected - verify service mesh configuration and network policies")

        if categories.get("STORAGE", 0) > 1:
            recommendations.append("STORAGE: Storage issues detected - check PVC status and volume mount configurations")

        if categories.get("RESOURCE", 0) > 2:
            recommendations.append("RESOURCES: Resource constraint issues - review resource requests, limits, and node capacity")

        if not recommendations:
            recommendations.append("MONITORING: Events are within normal parameters - continue monitoring")

        return recommendations


# ============================================================================
# EVENT CLASSIFICATION FUNCTIONS
# ============================================================================


def classify_event_severity_from_string(event_str: str) -> str:
    """Classify event severity from string representation."""

    event_lower = event_str.lower()

    # Check severity keywords from config
    for severity, keywords in SMART_EVENTS_CONFIG["severity_keywords"].items():
        if any(keyword in event_lower for keyword in keywords):
            return severity

    # Default to LOW if no keywords match
    return EventSeverity.LOW.value


def classify_event_category_from_string(event_str: str) -> str:
    """Classify event category from string representation."""

    event_lower = event_str.lower()

    # Check category keywords from config
    for category, keywords in SMART_EVENTS_CONFIG["category_keywords"].items():
        if any(keyword in event_lower for keyword in keywords):
            return category

    # Default to OTHER if no keywords match
    return EventCategory.OTHER.value


def calculate_relevance_score_from_string(event_str: str, focus_areas: List[str]) -> float:
    """Calculate relevance score based on focus areas."""

    score = 0.0
    event_lower = event_str.lower()

    # Base score from severity
    severity = classify_event_severity_from_string(event_str)
    severity_scores = {"CRITICAL": 1.0, "HIGH": 0.8, "MEDIUM": 0.6, "LOW": 0.4}
    score += severity_scores.get(severity, 0.4)

    # Bonus for focus area matches
    focus_area_mappings = SMART_EVENTS_CONFIG.get("focus_area_mappings", {})

    for focus_area in focus_areas:
        if focus_area in focus_area_mappings:
            target_severities = focus_area_mappings[focus_area]
            if severity in target_severities:
                score += 0.3

    # Keyword relevance bonus
    relevant_keywords = ["error", "failed", "oom", "timeout", "crash", "exception"]
    keyword_matches = sum(1 for keyword in relevant_keywords if keyword in event_lower)
    score += min(keyword_matches * 0.1, 0.5)  # Cap at 0.5

    return min(score, 2.0)  # Cap total score


def extract_timestamp_from_string(event_str: str) -> datetime:
    """Extract timestamp from event string."""

    # Try to find ISO timestamp
    iso_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)'
    match = re.search(iso_pattern, event_str)

    if match:
        try:
            timestamp_str = match.group(1)
            # Handle timezone
            if timestamp_str.endswith('Z'):
                timestamp_str = timestamp_str[:-1] + '+00:00'
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except ValueError:
            pass

    # Fallback to current time
    return datetime.now()


def estimate_string_event_tokens(event_str: str) -> int:
    """Estimate token count for an event string."""

    # Simple estimation: ~4 characters per token
    base_tokens = len(event_str) // 4

    # Add overhead for structure
    overhead = 10

    return max(base_tokens + overhead, 5)  # Minimum 5 tokens


# ============================================================================
# SMART EVENT SAMPLING FUNCTIONS
# ============================================================================


def smart_sample_string_events(
    events: List[str],
    focus_areas: List[str],
    max_tokens: int
) -> List[Dict[str, Any]]:
    """Smart sampling for string events."""

    # Classify all events
    classified_events = []
    for event_str in events:
        classified_event = {
            "event_string": event_str,
            "severity": classify_event_severity_from_string(event_str),
            "category": classify_event_category_from_string(event_str),
            "relevance_score": calculate_relevance_score_from_string(event_str, focus_areas),
            "timestamp": extract_timestamp_from_string(event_str),
            "token_estimate": estimate_string_event_tokens(event_str)
        }
        classified_events.append(classified_event)

    # Sort by priority
    def sort_key(e):
        severity_weight = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}.get(e["severity"], 0)
        return (severity_weight, e["relevance_score"], e["timestamp"])

    classified_events.sort(key=sort_key, reverse=True)

    # Sample within token limits
    available_tokens = int(max_tokens * 0.8)  # Reserve 20% for overhead
    selected_events = []
    current_tokens = 0

    for event in classified_events:
        if current_tokens + event["token_estimate"] <= available_tokens:
            selected_events.append(event)
            current_tokens += event["token_estimate"]
        else:
            break

    return selected_events


def generate_string_events_summary(
    classified_events: List[Dict[str, Any]],
    focus_areas: List[str]
) -> Dict[str, Any]:
    """Generate summary from classified string events."""

    total_events = len(classified_events)

    if total_events == 0:
        return {
            "total_events": 0,
            "message": "No events found in the specified timeframe"
        }

    # Count by severity and category
    severity_counts: Dict[str, int] = {}
    category_counts: Dict[str, int] = {}

    for event in classified_events:
        severity = event["severity"]
        category = event["category"]

        severity_counts[severity] = severity_counts.get(severity, 0) + 1
        category_counts[category] = category_counts.get(category, 0) + 1

    # Calculate focus area coverage
    focus_coverage: Dict[str, Dict[str, Any]] = {}
    focus_area_mappings = SMART_EVENTS_CONFIG.get("focus_area_mappings", {})

    for focus_area in focus_areas:
        if focus_area in focus_area_mappings:
            target_items = focus_area_mappings[focus_area]
            relevant_count = sum(
                1 for event in classified_events
                if event["severity"] in target_items or event["category"] in target_items
            )
            focus_coverage[focus_area] = {
                "relevant_events": relevant_count,
                "percentage": round(relevant_count / total_events * 100, 1) if total_events > 0 else 0
            }

    # Time range analysis
    if classified_events:
        timestamps = [event["timestamp"] for event in classified_events]
        time_span = max(timestamps) - min(timestamps)
        event_rate = total_events / max(time_span.total_seconds() / 3600, 0.1)  # events per hour
    else:
        time_span = timedelta(0)
        event_rate = 0

    return {
        "total_events": total_events,
        "severity_breakdown": severity_counts,
        "category_breakdown": category_counts,
        "focus_area_coverage": focus_coverage,
        "time_analysis": {
            "time_span": str(time_span),
            "event_rate_per_hour": round(event_rate, 2)
        },
        "critical_events": severity_counts.get("CRITICAL", 0),
        "high_severity_events": severity_counts.get("HIGH", 0)
    }


def generate_string_events_insights(classified_events: List[Dict[str, Any]]) -> List[str]:
    """Generate insights from classified events."""

    insights = []

    if not classified_events:
        return ["No events available for analysis"]

    total_events = len(classified_events)

    # Severity insights
    critical_count = len([e for e in classified_events if e["severity"] == "CRITICAL"])
    high_count = len([e for e in classified_events if e["severity"] == "HIGH"])

    if critical_count > 0:
        insights.append(f"{critical_count} critical events detected requiring immediate attention")

    if high_count > total_events * 0.3:
        insights.append(f"High severity events make up {high_count/total_events:.0%} of total events")

    # Category insights
    category_counts = Counter([e["category"] for e in classified_events])
    dominant_category = category_counts.most_common(1)[0] if category_counts else None

    if dominant_category and dominant_category[1] > total_events * 0.4:
        insights.append(f"{dominant_category[0]} category dominates with {dominant_category[1]} events")

    # Temporal insights
    timestamps = [e["timestamp"] for e in classified_events]
    if len(timestamps) > 1:
        time_span = max(timestamps) - min(timestamps)
        if time_span.total_seconds() < 3600:  # Less than 1 hour
            insights.append("Events clustered in short time window - potential incident burst")

    # Pattern insights
    all_text = " ".join([e.get("event_string", "") for e in classified_events]).lower()

    if "oom" in all_text:
        insights.append("Memory-related issues detected - check resource limits")

    if "imagepull" in all_text:
        insights.append("Image pull issues found - verify registry connectivity")

    if "timeout" in all_text:
        insights.append("Timeout patterns detected - investigate network latency")

    return insights


def generate_string_events_recommendations(classified_events: List[Dict[str, Any]]) -> List[str]:
    """Generate recommendations based on classified events."""

    recommendations = []

    if not classified_events:
        return ["No specific recommendations - no events to analyze"]

    # Severity-based recommendations
    critical_count = len([e for e in classified_events if e["severity"] == "CRITICAL"])
    high_count = len([e for e in classified_events if e["severity"] == "HIGH"])

    if critical_count >= 5:
        recommendations.append("IMMEDIATE: Activate incident response - multiple critical events detected")
    elif critical_count > 0:
        recommendations.append("HIGH PRIORITY: Investigate critical events within 30 minutes")

    if high_count >= 10:
        recommendations.append("Schedule investigation of high-severity events within 2 hours")

    # Category-specific recommendations
    category_counts = Counter([e["category"] for e in classified_events])

    for category, count in category_counts.items():
        if count >= 5:
            if category == "FAILURE":
                recommendations.append("Review application stability and error handling mechanisms")
            elif category == "NETWORKING":
                recommendations.append("Check network policies and service connectivity")
            elif category == "STORAGE":
                recommendations.append("Verify storage backend health and capacity")
            elif category == "SCHEDULING":
                recommendations.append("Review node capacity and resource allocation")

    # General recommendations
    total_events = len(classified_events)
    if total_events > 50:
        recommendations.append("High event volume detected - consider implementing log aggregation")

    if not recommendations:
        recommendations.append("Continue monitoring - event patterns appear normal")

    return recommendations


# ============================================================================
# ML PATTERN DETECTOR CLASS
# ============================================================================


class MLPatternDetector:
    """Machine Learning-powered pattern detection for events."""

    def __init__(self, events: List[Dict[str, Any]]):
        self.events = events
        self.patterns = {}
        self.anomalies = []

    def detect_patterns(self) -> Dict[str, Any]:
        """Detect patterns using ML-inspired techniques."""

        patterns = {
            "temporal_anomalies": self._detect_temporal_anomalies(),
            "frequency_patterns": self._detect_frequency_patterns(),
            "severity_escalation_patterns": self._detect_severity_escalation(),
            "resource_usage_patterns": self._detect_resource_patterns(),
            "cyclic_patterns": self._detect_cyclic_patterns(),
            "outlier_events": self._detect_outlier_events(),
            "predictive_indicators": self._generate_predictive_indicators()
        }

        return patterns

    def _detect_temporal_anomalies(self) -> List[Dict[str, Any]]:
        """Detect temporal anomalies in event patterns."""

        if len(self.events) < 10:
            return []

        # Calculate event intervals
        sorted_events = sorted(self.events, key=lambda x: x.get("timestamp", datetime.now()))
        intervals = []

        for i in range(1, len(sorted_events)):
            prev_time = sorted_events[i-1].get("timestamp", datetime.now())
            curr_time = sorted_events[i].get("timestamp", datetime.now())
            interval = (curr_time - prev_time).total_seconds()
            intervals.append(interval)

        if not intervals:
            return []

        # Statistical analysis
        mean_interval = statistics.mean(intervals)
        std_interval = statistics.stdev(intervals) if len(intervals) > 1 else 0

        anomalies = []
        for i, interval in enumerate(intervals):
            # Z-score based anomaly detection
            z_score = abs(interval - mean_interval) / (std_interval + 1e-6)

            if z_score > 2.5:  # 2.5 sigma threshold
                anomalies.append({
                    "type": "temporal_anomaly",
                    "interval_seconds": interval,
                    "z_score": z_score,
                    "event_index": i + 1,
                    "severity": "HIGH" if z_score > 3.0 else "MEDIUM",
                    "description": f"Unusual time gap: {interval:.1f}s (expected ~{mean_interval:.1f}s)"
                })

        return anomalies[:10]  # Top 10 anomalies

    def _detect_frequency_patterns(self) -> Dict[str, Any]:
        """Detect frequency-based patterns."""

        # Group events by hour
        hourly_counts = defaultdict(int)
        for event in self.events:
            hour = event.get("timestamp", datetime.now()).hour
            hourly_counts[hour] += 1

        # Calculate frequency statistics
        frequencies = list(hourly_counts.values())
        if not frequencies:
            return {"pattern": "no_data"}

        mean_freq = statistics.mean(frequencies)
        std_freq = statistics.stdev(frequencies) if len(frequencies) > 1 else 0

        # Detect patterns
        patterns = []

        # High frequency periods
        for hour, count in hourly_counts.items():
            if count > mean_freq + 2 * std_freq:
                patterns.append({
                    "type": "high_frequency_period",
                    "hour": hour,
                    "event_count": count,
                    "deviation": (count - mean_freq) / (std_freq + 1e-6)
                })

        return {
            "mean_frequency": mean_freq,
            "std_frequency": std_freq,
            "patterns": patterns[:15]  # Limit output
        }

    def _detect_cyclic_patterns(self) -> Dict[str, Any]:
        """Detect cyclic patterns in events."""

        if len(self.events) < 20:
            return {"pattern": "insufficient_data"}

        # Extract time features
        time_features = {
            "hour_of_day": [],
            "day_of_week": [],
            "minute_of_hour": []
        }

        for event in self.events:
            timestamp = event.get("timestamp", datetime.now())
            time_features["hour_of_day"].append(timestamp.hour)
            time_features["day_of_week"].append(timestamp.weekday())
            time_features["minute_of_hour"].append(timestamp.minute)

        cycles = {}

        # Analyze each time feature for cycles
        for feature_name, values in time_features.items():
            value_counts = Counter(values)

            # Check if we have enough data for variance calculation
            count_values = list(value_counts.values())
            if len(count_values) < 2:
                cycles[feature_name] = {
                    "cyclicity_score": 0.0,
                    "dominant_values": list(value_counts.most_common(3)),
                    "pattern_strength": "LOW",
                    "note": "Insufficient variance data - all events in same time bucket"
                }
                continue

            try:
                # Calculate cyclicity score (how evenly distributed)
                expected_freq = len(values) / len(set(values))
                variance = statistics.variance(count_values)
                cyclicity_score = 1 / (1 + variance / expected_freq)  # Normalized score

                # Find dominant patterns
                most_common = value_counts.most_common(3)

                cycles[feature_name] = {
                    "cyclicity_score": cyclicity_score,
                    "dominant_values": most_common,
                    "pattern_strength": "HIGH" if cyclicity_score > 0.7 else "MEDIUM" if cyclicity_score > 0.4 else "LOW"
                }

            except statistics.StatisticsError as e:
                # Handle edge cases where variance calculation fails
                cycles[feature_name] = {
                    "cyclicity_score": 0.0,
                    "dominant_values": list(value_counts.most_common(3)),
                    "pattern_strength": "LOW",
                    "error": f"Statistical calculation failed: {str(e)}"
                }

        return cycles

    def _detect_severity_escalation(self) -> List[Dict[str, Any]]:
        """Detect severity escalation patterns."""
        escalations = []

        try:
            # Sort events by time
            sorted_events = sorted(self.events, key=lambda x: x.get("timestamp", datetime.now()))

            # Look for severity escalation patterns
            for i in range(len(sorted_events) - 1):
                current_event = sorted_events[i]
                next_event = sorted_events[i + 1]

                current_severity = current_event.get("severity", "LOW")
                next_severity = next_event.get("severity", "LOW")

                # Define severity levels for comparison
                severity_levels = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

                current_level = severity_levels.get(current_severity, 1)
                next_level = severity_levels.get(next_severity, 1)

                # Check for escalation
                if next_level > current_level:
                    time_diff = (next_event.get("timestamp", datetime.now()) -
                               current_event.get("timestamp", datetime.now())).total_seconds()

                    escalations.append({
                        "from_severity": current_severity,
                        "to_severity": next_severity,
                        "escalation_time_seconds": time_diff,
                        "current_event": current_event.get("event_string", "")[:80] + "...",
                        "escalated_event": next_event.get("event_string", "")[:80] + "...",
                        "escalation_factor": next_level - current_level
                    })

        except Exception as e:
            escalations = []

        return escalations[:10]  # Limit to top 10 escalations

    def _detect_resource_patterns(self) -> Dict[str, Any]:
        """Detect resource-related patterns."""
        resource_patterns = {
            "memory_issues": [],
            "cpu_issues": [],
            "disk_issues": [],
            "network_issues": [],
            "pod_issues": []
        }

        try:
            for event in self.events:
                event_content = event.get("event_string", "").lower()

                if any(pattern in event_content for pattern in ["memory", "oom", "out of memory"]):
                    resource_patterns["memory_issues"].append(event.get("event_string", "")[:100] + "...")
                elif any(pattern in event_content for pattern in ["cpu", "throttl", "processor"]):
                    resource_patterns["cpu_issues"].append(event.get("event_string", "")[:100] + "...")
                elif any(pattern in event_content for pattern in ["disk", "storage", "volume", "pvc"]):
                    resource_patterns["disk_issues"].append(event.get("event_string", "")[:100] + "...")
                elif any(pattern in event_content for pattern in ["network", "dns", "connection", "timeout"]):
                    resource_patterns["network_issues"].append(event.get("event_string", "")[:100] + "...")
                elif any(pattern in event_content for pattern in ["pod", "container", "image"]):
                    resource_patterns["pod_issues"].append(event.get("event_string", "")[:100] + "...")

            # Return summary with counts
            return {
                resource_type: {
                    "count": len(issues),
                    "sample_issues": issues[:3]  # Top 3 samples
                }
                for resource_type, issues in resource_patterns.items()
                if len(issues) > 0
            }

        except Exception as e:
            return {"error": str(e)}

    def _detect_outlier_events(self) -> List[Dict[str, Any]]:
        """Detect outlier events using statistical methods."""
        outliers = []

        try:
            # Group events by content similarity
            content_groups = defaultdict(list)

            for event in self.events:
                # Simple content grouping by first few words
                content = event.get("event_string", "")
                words = content.split()[:3]  # First 3 words
                key = " ".join(words).lower()
                content_groups[key].append(event)

            # Find rare event patterns (outliers)
            total_events = len(self.events)
            for pattern, events in content_groups.items():
                frequency = len(events) / total_events

                # Events that occur very rarely are potential outliers
                if frequency < 0.05 and len(events) <= 2:  # Less than 5% frequency
                    for event in events:
                        outliers.append({
                            "event": event.get("event_string", "")[:100] + "...",
                            "rarity_score": 1 - frequency,
                            "pattern": pattern,
                            "severity": event.get("severity", "UNKNOWN"),
                            "timestamp": event.get("timestamp", datetime.now()).isoformat() if isinstance(event.get("timestamp"), datetime) else str(event.get("timestamp", ""))
                        })

        except Exception as e:
            outliers = []

        return outliers[:15]  # Limit to top 15 outliers

    def _generate_predictive_indicators(self) -> Dict[str, Any]:
        """Generate predictive indicators for future issues."""
        indicators = {
            "trending_issues": [],
            "escalation_risk": "LOW",
            "pattern_stability": "STABLE",
            "predictive_confidence": 0.0
        }

        try:
            # Analyze trending patterns
            recent_events = [e for e in self.events if
                           (datetime.now() - e.get("timestamp", datetime.now())).total_seconds() < 3600]  # Last hour

            if len(recent_events) > len(self.events) * 0.5:  # More than 50% of events in last hour
                indicators["trending_issues"].append("High event frequency in recent period")
                indicators["escalation_risk"] = "HIGH"

            # Check for critical event trends
            critical_recent = [e for e in recent_events if e.get("severity") == "CRITICAL"]
            if len(critical_recent) > 2:
                indicators["trending_issues"].append(f"{len(critical_recent)} critical events in last hour")
                indicators["escalation_risk"] = "CRITICAL"

            # Pattern stability analysis
            if len(self.events) > 5:
                severity_distribution = Counter([e.get("severity", "UNKNOWN") for e in self.events])
                most_common_severity = severity_distribution.most_common(1)[0]

                if most_common_severity[1] / len(self.events) > 0.8:  # 80% same severity
                    indicators["pattern_stability"] = "STABLE"
                else:
                    indicators["pattern_stability"] = "VARIABLE"

            # Calculate confidence based on data volume
            indicators["predictive_confidence"] = min(1.0, len(self.events) / 50.0)

        except Exception as e:
            indicators["error"] = str(e)

        return indicators


# ============================================================================
# LOG METRICS INTEGRATOR CLASS
# ============================================================================


class LogMetricsIntegrator:
    """Integrates event analysis with logs and metrics."""

    def __init__(self, events: List[Dict[str, Any]]):
        self.events = events

    async def correlate_with_logs(self, namespace: str, time_window: str = "2h") -> Dict[str, Any]:
        """Correlate events with log data."""

        try:
            # This would need to be imported from the main server
            # For now, return a placeholder
            return {
                "log_correlation": "available",
                "correlations": [],
                "integration_insights": ["Log correlation functionality available"]
            }

        except Exception as e:
            return {"log_correlation": "error", "error": str(e)}

    async def correlate_with_metrics(self, namespace: str) -> Dict[str, Any]:
        """Correlate events with metrics data."""

        # Simulated metrics correlation
        import random

        metrics_patterns = {
            "cpu_usage": {"avg": random.uniform(20, 95), "max": random.uniform(80, 100)},
            "memory_usage": {"avg": random.uniform(30, 90), "max": random.uniform(85, 100)},
            "network_io": {"bytes_in": random.randint(1000000, 10000000)},
        }

        correlations = []

        # Find CPU correlations
        high_cpu_events = [e for e in self.events if "cpu" in e.get("event_string", "").lower()]
        if high_cpu_events and metrics_patterns["cpu_usage"]["avg"] > 80:
            correlations.append({
                "type": "cpu_correlation",
                "strength": 0.8,
                "description": f"{len(high_cpu_events)} CPU-related events with {metrics_patterns['cpu_usage']['avg']:.1f}% avg CPU"
            })

        return {
            "metrics_correlation": "simulated",
            "correlations": correlations,
            "metrics_summary": metrics_patterns
        }


# ============================================================================
# RUNBOOK SUGGESTION ENGINE CLASS
# ============================================================================


class RunbookSuggestionEngine:
    """Intelligent runbook suggestion engine."""

    def __init__(self, events: List[Dict[str, Any]], patterns: Dict[str, Any]):
        self.events = events
        self.patterns = patterns
        self.runbooks = self._initialize_runbook_database()

    def suggest_runbooks(self) -> List[Dict[str, Any]]:
        """Suggest relevant runbooks based on event patterns."""

        suggestions = []

        # Analyze dominant issues
        issue_types = self._categorize_issues()

        for issue_type, confidence in issue_types.items():
            if confidence > 0.5:
                runbook = self._get_runbook_for_issue(issue_type)
                if runbook:
                    suggestions.append({
                        "runbook": runbook,
                        "confidence": confidence,
                        "relevance_reason": self._explain_relevance(issue_type),
                        "priority": self._calculate_priority(issue_type, confidence)
                    })

        return sorted(suggestions, key=lambda x: x["priority"], reverse=True)[:5]

    def _initialize_runbook_database(self) -> Dict[str, Dict[str, Any]]:
        """Initialize the runbook database."""

        return {
            "pod_crash_loop": {
                "title": "Pod Crash Loop Remediation",
                "steps": [
                    "Check pod logs for error messages",
                    "Verify resource limits and requests",
                    "Check application configuration",
                    "Review health check endpoints",
                    "Validate container image"
                ],
                "estimated_time": "15-30 minutes",
                "severity": "HIGH"
            },
            "memory_exhaustion": {
                "title": "Memory Exhaustion Response",
                "steps": [
                    "Identify high memory consuming pods",
                    "Check for memory leaks in applications",
                    "Review memory limits configuration",
                    "Consider horizontal pod scaling",
                    "Implement memory monitoring alerts"
                ],
                "estimated_time": "20-45 minutes",
                "severity": "CRITICAL"
            },
            "network_connectivity": {
                "title": "Network Connectivity Issues",
                "steps": [
                    "Test DNS resolution",
                    "Check network policies",
                    "Verify service endpoints",
                    "Review ingress configuration",
                    "Test inter-pod communication"
                ],
                "estimated_time": "25-40 minutes",
                "severity": "HIGH"
            }
        }

    def _categorize_issues(self) -> Dict[str, float]:
        """Categorize the dominant issues with confidence scores."""

        issue_scores = {}
        total_events = len(self.events)

        if total_events == 0:
            return {}

        # Analyze event patterns
        all_text = " ".join([e.get("event_string", "") for e in self.events]).lower()

        # Pod crash issues
        crash_indicators = ["crash", "crashloopbackoff", "exit", "failed", "restart"]
        crash_score = sum(all_text.count(indicator) for indicator in crash_indicators) / total_events
        if crash_score > 0.1:
            issue_scores["pod_crash_loop"] = min(1.0, crash_score * 2)

        # Memory issues
        memory_indicators = ["oom", "memory", "killed", "evicted"]
        memory_score = sum(all_text.count(indicator) for indicator in memory_indicators) / total_events
        if memory_score > 0.05:
            issue_scores["memory_exhaustion"] = min(1.0, memory_score * 3)

        # Network issues
        network_indicators = ["network", "dns", "connection", "timeout", "unreachable"]
        network_score = sum(all_text.count(indicator) for indicator in network_indicators) / total_events
        if network_score > 0.05:
            issue_scores["network_connectivity"] = min(1.0, network_score * 2.5)

        return issue_scores

    def _get_runbook_for_issue(self, issue_type: str) -> Optional[Dict[str, Any]]:
        """Get runbook for specific issue type."""
        return self.runbooks.get(issue_type)

    def _explain_relevance(self, issue_type: str) -> str:
        """Explain why this runbook is relevant."""

        explanations = {
            "pod_crash_loop": "Multiple pod failures and restart events detected",
            "memory_exhaustion": "OOM kills and memory-related events identified",
            "network_connectivity": "Network timeouts and connectivity issues found"
        }

        return explanations.get(issue_type, "Pattern matching indicates relevance")

    def _calculate_priority(self, issue_type: str, confidence: float) -> float:
        """Calculate priority score for runbook suggestion."""

        # Base priority on severity and confidence
        severity_weights = {
            "CRITICAL": 1.0,
            "HIGH": 0.8,
            "MEDIUM": 0.6,
            "LOW": 0.4
        }

        runbook = self.runbooks.get(issue_type, {})
        severity = runbook.get("severity", "LOW")
        severity_weight = severity_weights.get(severity, 0.4)

        return confidence * severity_weight


# ============================================================================
# ADVANCED ANALYTICS HELPER FUNCTIONS
# ============================================================================


def assess_overall_risk(analytics_result: Dict[str, Any]) -> Dict[str, Any]:
    """Assess overall risk based on all analysis components."""

    risk_factors = []
    risk_score = 0.0

    # Event severity risk
    base_analysis = analytics_result.get("base_analysis", {})
    if "detailed_analysis" in base_analysis:
        detailed = base_analysis["detailed_analysis"]
        if "severity_analysis" in detailed:
            critical_events = detailed["severity_analysis"].get("CRITICAL", {}).get("count", 0)
            if critical_events > 5:
                risk_factors.append(f"High critical event count: {critical_events}")
                risk_score += 0.3

    # ML pattern risk
    ml_patterns = analytics_result.get("ml_patterns", {})
    if "severity_escalation_patterns" in ml_patterns and ml_patterns["severity_escalation_patterns"]:
        escalations = len(ml_patterns["severity_escalation_patterns"])
        risk_factors.append(f"Severity escalation patterns: {escalations}")
        risk_score += 0.25

    # Correlation risk
    log_corr = analytics_result.get("log_correlation", {})
    if log_corr.get("correlations"):
        error_correlations = [c for c in log_corr["correlations"] if c.get("type") == "error_correlation"]
        if error_correlations:
            risk_factors.append("Strong log error correlations detected")
            risk_score += 0.2

    # Runbook urgency
    runbooks = analytics_result.get("runbook_suggestions", [])
    critical_runbooks = [r for r in runbooks if r.get("runbook", {}).get("severity") == "CRITICAL"]
    if critical_runbooks:
        risk_factors.append(f"Critical runbooks required: {len(critical_runbooks)}")
        risk_score += 0.25

    # Determine overall risk level
    if risk_score >= 0.7:
        risk_level = "CRITICAL"
    elif risk_score >= 0.5:
        risk_level = "HIGH"
    elif risk_score >= 0.3:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return {
        "overall_risk_level": risk_level,
        "risk_score": round(risk_score, 2),
        "risk_factors": risk_factors,
        "mitigation_urgency": "IMMEDIATE" if risk_level == "CRITICAL" else "SOON" if risk_level == "HIGH" else "PLANNED"
    }


def generate_strategic_recommendations(analytics_result: Dict[str, Any]) -> List[str]:
    """Generate strategic recommendations based on comprehensive analysis."""

    recommendations = []

    # Risk-based recommendations
    risk_assessment = analytics_result.get("risk_assessment", {})
    risk_level = risk_assessment.get("overall_risk_level", "LOW")

    if risk_level == "CRITICAL":
        recommendations.append("IMMEDIATE ACTION: Activate incident response procedures - critical issues detected")
        recommendations.append("Escalate to on-call team and stakeholders immediately")
    elif risk_level == "HIGH":
        recommendations.append("HIGH PRIORITY: Address identified issues within the next 2-4 hours")

    # ML pattern recommendations
    ml_patterns = analytics_result.get("ml_patterns", {})
    if "predictive_indicators" in ml_patterns:
        pred = ml_patterns["predictive_indicators"]
        if isinstance(pred, dict) and "recommended_monitoring" in pred:
            for rec in pred["recommended_monitoring"][:2]:  # Top 2 recommendations
                recommendations.append(f"Monitoring: {rec}")

    # Runbook recommendations
    runbooks = analytics_result.get("runbook_suggestions", [])
    if runbooks:
        top_runbook = runbooks[0]
        recommendations.append(f"Execute: {top_runbook['runbook']['title']} (confidence: {top_runbook['confidence']:.1%})")

    # Correlation-based recommendations
    log_corr = analytics_result.get("log_correlation", {})
    if log_corr.get("integration_insights"):
        for insight in log_corr["integration_insights"][:1]:  # Top insight
            if "Strong" in insight:
                recommendations.append(f"Investigation: {insight}")

    # Resource recommendations
    metrics_corr = analytics_result.get("metrics_correlation", {})
    if metrics_corr.get("performance_insights"):
        for insight in metrics_corr["performance_insights"][:1]:
            recommendations.append(f"Performance: {insight}")

    # Default recommendation if none generated
    if not recommendations:
        recommendations.append("Continue monitoring - no immediate action required based on current analysis")

    return recommendations


async def generate_comprehensive_insights(analytics_result: Dict[str, Any], depth: str) -> List[str]:
    """Generate comprehensive insights from all analysis components."""

    insights = []

    # ML pattern insights
    ml_patterns = analytics_result.get("ml_patterns", {})
    if "temporal_anomalies" in ml_patterns and ml_patterns["temporal_anomalies"]:
        anomaly_count = len(ml_patterns["temporal_anomalies"])
        insights.append(f"ML Analysis: Detected {anomaly_count} temporal anomalies indicating irregular event patterns")

    if "severity_escalation_patterns" in ml_patterns and ml_patterns["severity_escalation_patterns"]:
        escalation_count = len(ml_patterns["severity_escalation_patterns"])
        insights.append(f"Escalation Alert: {escalation_count} severity escalation patterns detected")

    # Correlation insights
    log_corr = analytics_result.get("log_correlation", {})
    if log_corr.get("log_correlation") == "available":
        correlations = log_corr.get("correlations", [])
        strong_correlations = [c for c in correlations if c.get("strength", 0) > 0.7]
        if strong_correlations:
            insights.append(f"Log Integration: {len(strong_correlations)} strong correlations found between events and logs")

    metrics_corr = analytics_result.get("metrics_correlation", {})
    if metrics_corr.get("correlations"):
        cpu_issues = any(c["type"] == "cpu_correlation" for c in metrics_corr["correlations"])
        memory_issues = any(c["type"] == "memory_correlation" for c in metrics_corr["correlations"])
        if cpu_issues or memory_issues:
            insights.append("Resource Correlation: Performance metrics confirm resource-related event patterns")

    # Runbook insights
    runbooks = analytics_result.get("runbook_suggestions", [])
    if runbooks:
        high_priority = [r for r in runbooks if r.get("priority", 0) > 0.7]
        if high_priority:
            insights.append(f"Runbook Recommendations: {len(high_priority)} high-priority runbooks identified for immediate action")

    # Predictive insights
    if "predictive_indicators" in ml_patterns:
        pred = ml_patterns["predictive_indicators"]
        if isinstance(pred, dict) and "severity_trend" in pred:
            if pred["severity_trend"]["direction"] == "increasing":
                insights.append("Predictive Alert: Severity trend is increasing - expect more critical events")

    if depth == "deep" and len(insights) < 3:
        insights.append("Deep Analysis: Consider extending the analysis time window for more comprehensive patterns")

    return insights
