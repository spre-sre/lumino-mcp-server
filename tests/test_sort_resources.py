"""
Tests for sort_resources and related resource search helper functions.

These tests cover the sort_resources function in src/helpers/utils.py which
sorts Kubernetes resource dictionaries by name, namespace, creation_time,
or labels, in ascending or descending order.

Test cases map to the feature plan for adding sort functionality to list output:
- Sort by name (ascending, descending)
- Sort by namespace
- Sort by creation_time (due-date analog)
- Sort by labels (priority analog -- label count as weight)
- Default/unknown sort field returns resources unchanged
- Empty resource list
- Tie-breaking (stable sort)
- Combined with extract_resource_info
- Combined with calculate_namespace_distribution
- Combined with analyze_labels
- Combined with build_advanced_label_selector
"""

from src.helpers.utils import (
    sort_resources,
    extract_resource_info,
    analyze_labels,
    calculate_namespace_distribution,
    build_advanced_label_selector,
    get_resource_api_info,
)


# ---------------------------------------------------------------------------
# Helpers to build test resource dicts
# ---------------------------------------------------------------------------


def make_resource(name, namespace="default", creation_time="", labels=None):
    """Build a minimal resource dict matching the structure sort_resources expects."""
    return {
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creation_timestamp": creation_time,
            "labels": labels or {},
        },
        "kind": "Pod",
    }


# ---------------------------------------------------------------------------
# sort_resources -- sort by name
# ---------------------------------------------------------------------------


class TestSortByName:
    """Sort resources by metadata.name field."""

    def test_sort_by_name_ascending(self):
        resources = [
            make_resource("zebra"),
            make_resource("apple"),
            make_resource("banana"),
        ]
        result = sort_resources(resources, "name", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["apple", "banana", "zebra"]

    def test_sort_by_name_descending(self):
        resources = [
            make_resource("apple"),
            make_resource("zebra"),
            make_resource("banana"),
        ]
        result = sort_resources(resources, "name", "desc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["zebra", "banana", "apple"]

    def test_sort_by_name_case_sensitive(self):
        """Python sorted is case-sensitive; uppercase sorts before lowercase."""
        resources = [
            make_resource("zebra"),
            make_resource("Apple"),
            make_resource("banana"),
        ]
        result = sort_resources(resources, "name", "asc")
        names = [r["metadata"]["name"] for r in result]
        # Uppercase 'A' < lowercase 'b' < lowercase 'z' in ASCII
        assert names == ["Apple", "banana", "zebra"]

    def test_sort_by_name_single_resource(self):
        resources = [make_resource("only")]
        result = sort_resources(resources, "name", "asc")
        assert len(result) == 1
        assert result[0]["metadata"]["name"] == "only"

    def test_sort_by_name_preserves_all_fields(self):
        """Sorting should not lose any data from the resource dicts."""
        resources = [
            make_resource("beta", namespace="ns-1", labels={"app": "web"}),
            make_resource("alpha", namespace="ns-2", labels={"app": "api"}),
        ]
        result = sort_resources(resources, "name", "asc")
        assert result[0]["metadata"]["name"] == "alpha"
        assert result[0]["metadata"]["namespace"] == "ns-2"
        assert result[0]["metadata"]["labels"] == {"app": "api"}


# ---------------------------------------------------------------------------
# sort_resources -- sort by namespace
# ---------------------------------------------------------------------------


class TestSortByNamespace:
    """Sort resources by metadata.namespace field."""

    def test_sort_by_namespace_ascending(self):
        resources = [
            make_resource("pod-1", namespace="zeta-ns"),
            make_resource("pod-2", namespace="alpha-ns"),
            make_resource("pod-3", namespace="mid-ns"),
        ]
        result = sort_resources(resources, "namespace", "asc")
        namespaces = [r["metadata"]["namespace"] for r in result]
        assert namespaces == ["alpha-ns", "mid-ns", "zeta-ns"]

    def test_sort_by_namespace_descending(self):
        resources = [
            make_resource("pod-1", namespace="alpha-ns"),
            make_resource("pod-2", namespace="zeta-ns"),
            make_resource("pod-3", namespace="mid-ns"),
        ]
        result = sort_resources(resources, "namespace", "desc")
        namespaces = [r["metadata"]["namespace"] for r in result]
        assert namespaces == ["zeta-ns", "mid-ns", "alpha-ns"]


# ---------------------------------------------------------------------------
# sort_resources -- sort by creation_time
# ---------------------------------------------------------------------------


class TestSortByCreationTime:
    """Sort resources by metadata.creation_timestamp field (lexicographic ISO dates)."""

    def test_sort_by_creation_time_ascending(self):
        resources = [
            make_resource("pod-a", creation_time="2025-03-01T00:00:00Z"),
            make_resource("pod-b", creation_time="2024-01-15T00:00:00Z"),
            make_resource("pod-c", creation_time="2025-06-20T00:00:00Z"),
        ]
        result = sort_resources(resources, "creation_time", "asc")
        times = [r["metadata"]["creation_timestamp"] for r in result]
        assert times == [
            "2024-01-15T00:00:00Z",
            "2025-03-01T00:00:00Z",
            "2025-06-20T00:00:00Z",
        ]

    def test_sort_by_creation_time_descending(self):
        resources = [
            make_resource("pod-a", creation_time="2025-03-01T00:00:00Z"),
            make_resource("pod-b", creation_time="2024-01-15T00:00:00Z"),
            make_resource("pod-c", creation_time="2025-06-20T00:00:00Z"),
        ]
        result = sort_resources(resources, "creation_time", "desc")
        times = [r["metadata"]["creation_timestamp"] for r in result]
        assert times == [
            "2025-06-20T00:00:00Z",
            "2025-03-01T00:00:00Z",
            "2024-01-15T00:00:00Z",
        ]

    def test_sort_by_creation_time_empty_timestamps_sort_first(self):
        """Resources with empty creation_timestamp sort to the beginning (asc)."""
        resources = [
            make_resource("pod-a", creation_time="2025-03-01T00:00:00Z"),
            make_resource("pod-b", creation_time=""),
            make_resource("pod-c", creation_time="2024-01-15T00:00:00Z"),
        ]
        result = sort_resources(resources, "creation_time", "asc")
        times = [r["metadata"]["creation_timestamp"] for r in result]
        # Empty string sorts before any date string in lexicographic order
        assert times == ["", "2024-01-15T00:00:00Z", "2025-03-01T00:00:00Z"]

    def test_sort_by_creation_time_identical_timestamps(self):
        """Resources with identical creation times preserve original order (stable sort)."""
        resources = [
            make_resource("pod-a", creation_time="2025-03-01T00:00:00Z"),
            make_resource("pod-b", creation_time="2025-03-01T00:00:00Z"),
            make_resource("pod-c", creation_time="2025-03-01T00:00:00Z"),
        ]
        result = sort_resources(resources, "creation_time", "asc")
        names = [r["metadata"]["name"] for r in result]
        # Python sorted() is stable, so original order is preserved for equal keys
        assert names == ["pod-a", "pod-b", "pod-c"]


# ---------------------------------------------------------------------------
# sort_resources -- sort by labels (label count)
# ---------------------------------------------------------------------------


class TestSortByLabels:
    """Sort resources by label count (analog of sorting by priority weight)."""

    def test_sort_by_labels_ascending(self):
        resources = [
            make_resource("pod-many", labels={"a": "1", "b": "2", "c": "3"}),
            make_resource("pod-none", labels={}),
            make_resource("pod-one", labels={"x": "1"}),
        ]
        result = sort_resources(resources, "labels", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["pod-none", "pod-one", "pod-many"]

    def test_sort_by_labels_descending(self):
        resources = [
            make_resource("pod-none", labels={}),
            make_resource("pod-many", labels={"a": "1", "b": "2", "c": "3"}),
            make_resource("pod-one", labels={"x": "1"}),
        ]
        result = sort_resources(resources, "labels", "desc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["pod-many", "pod-one", "pod-none"]

    def test_sort_by_labels_tie_break_preserves_order(self):
        """Resources with the same label count preserve insertion order (stable sort)."""
        resources = [
            make_resource("pod-c", labels={"env": "prod"}),
            make_resource("pod-a", labels={"app": "web"}),
            make_resource("pod-b", labels={"tier": "frontend"}),
        ]
        result = sort_resources(resources, "labels", "asc")
        names = [r["metadata"]["name"] for r in result]
        # All have 1 label, so original order is preserved
        assert names == ["pod-c", "pod-a", "pod-b"]


# ---------------------------------------------------------------------------
# sort_resources -- unknown field / default behavior
# ---------------------------------------------------------------------------


class TestSortDefault:
    """Unknown sort field returns resources unchanged (backward compatibility)."""

    def test_sort_unknown_field_returns_unchanged(self):
        resources = [
            make_resource("third"),
            make_resource("first"),
            make_resource("second"),
        ]
        result = sort_resources(resources, "nonexistent_field", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["third", "first", "second"]

    def test_sort_by_id_analog_returns_unchanged(self):
        """Sorting by 'id' (not a recognized field) returns resources in original order."""
        resources = [
            make_resource("pod-3"),
            make_resource("pod-1"),
            make_resource("pod-2"),
        ]
        result = sort_resources(resources, "id", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["pod-3", "pod-1", "pod-2"]

    def test_sort_default_matches_no_sort(self):
        """Output with unknown sort field is byte-identical to unsorted input order."""
        resources = [
            make_resource("pod-b", namespace="ns-2"),
            make_resource("pod-a", namespace="ns-1"),
            make_resource("pod-c", namespace="ns-3"),
        ]
        no_sort_names = [r["metadata"]["name"] for r in resources]
        sorted_names = [r["metadata"]["name"] for r in sort_resources(resources, "unknown", "asc")]
        assert no_sort_names == sorted_names


# ---------------------------------------------------------------------------
# sort_resources -- empty list
# ---------------------------------------------------------------------------


class TestSortEmptyList:
    """Sorting an empty resource list returns an empty list without error."""

    def test_sort_empty_list_by_name(self):
        result = sort_resources([], "name", "asc")
        assert result == []

    def test_sort_empty_list_by_creation_time(self):
        result = sort_resources([], "creation_time", "desc")
        assert result == []

    def test_sort_empty_list_by_labels(self):
        result = sort_resources([], "labels", "asc")
        assert result == []

    def test_sort_empty_list_by_unknown_field(self):
        result = sort_resources([], "whatever", "asc")
        assert result == []


# ---------------------------------------------------------------------------
# sort_resources -- invalid sort field validation
# ---------------------------------------------------------------------------


class TestSortInvalidField:
    """Sorting with an invalid sort field returns resources unchanged (no error)."""

    def test_invalid_field_returns_resources_unchanged(self):
        resources = [make_resource("a"), make_resource("b")]
        result = sort_resources(resources, "invalid", "asc")
        assert len(result) == 2
        assert result[0]["metadata"]["name"] == "a"
        assert result[1]["metadata"]["name"] == "b"

    def test_empty_string_field_returns_resources_unchanged(self):
        resources = [make_resource("x"), make_resource("y")]
        result = sort_resources(resources, "", "asc")
        assert [r["metadata"]["name"] for r in result] == ["x", "y"]


# ---------------------------------------------------------------------------
# sort_resources -- combined with filters (extract_resource_info pipeline)
# ---------------------------------------------------------------------------


class TestSortWithExtractResourceInfo:
    """Sort resources that have been processed through extract_resource_info."""

    def test_sort_extracted_resources_by_name(self):
        raw_resources = [
            {
                "metadata": {"name": "zebra-pod", "namespace": "default", "labels": {}},
                "kind": "Pod",
            },
            {
                "metadata": {"name": "apple-pod", "namespace": "default", "labels": {}},
                "kind": "Pod",
            },
            {
                "metadata": {"name": "mango-pod", "namespace": "default", "labels": {}},
                "kind": "Pod",
            },
        ]
        extracted = [extract_resource_info(r, False, False) for r in raw_resources]
        result = sort_resources(extracted, "name", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["apple-pod", "mango-pod", "zebra-pod"]

    def test_sort_extracted_resources_by_creation_time(self):
        raw_resources = [
            {
                "metadata": {
                    "name": "new",
                    "namespace": "ns",
                    "creationTimestamp": "2025-06-01T00:00:00Z",
                },
                "kind": "Pod",
            },
            {
                "metadata": {
                    "name": "old",
                    "namespace": "ns",
                    "creationTimestamp": "2024-01-01T00:00:00Z",
                },
                "kind": "Pod",
            },
        ]
        extracted = [extract_resource_info(r, False, False) for r in raw_resources]
        result = sort_resources(extracted, "creation_time", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["old", "new"]

    def test_sort_with_priority_filter_analog(self):
        """
        Filter by label (analog of --priority filter), then sort.
        Creates resources with different 'priority' labels, filters to 'high',
        sorts remaining by name, and verifies count.
        """
        resources = [
            make_resource("task-d", labels={"priority": "high"}),
            make_resource("task-a", labels={"priority": "low"}),
            make_resource("task-c", labels={"priority": "high"}),
            make_resource("task-b", labels={"priority": "medium"}),
        ]
        # Filter: only high priority
        high_priority = [r for r in resources if r["metadata"]["labels"].get("priority") == "high"]
        assert len(high_priority) == 2
        # Sort filtered results by name
        result = sort_resources(high_priority, "name", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["task-c", "task-d"]

    def test_sort_with_overdue_filter_analog(self):
        """
        Filter by creation_time (analog of --overdue filter), then sort by time.
        Resources created before a cutoff are 'overdue'.
        """
        resources = [
            make_resource("recent", creation_time="2025-06-01T00:00:00Z"),
            make_resource("old-1", creation_time="2024-01-15T00:00:00Z"),
            make_resource("future", creation_time="2026-01-01T00:00:00Z"),
            make_resource("old-2", creation_time="2024-06-01T00:00:00Z"),
        ]
        cutoff = "2025-01-01T00:00:00Z"
        overdue = [
            r
            for r in resources
            if r["metadata"]["creation_timestamp"] and r["metadata"]["creation_timestamp"] < cutoff
        ]
        result = sort_resources(overdue, "creation_time", "asc")
        names = [r["metadata"]["name"] for r in result]
        assert names == ["old-1", "old-2"]


# ---------------------------------------------------------------------------
# build_advanced_label_selector
# ---------------------------------------------------------------------------


class TestBuildAdvancedLabelSelector:
    """Test the label selector builder used in resource search."""

    def test_equals_operator(self):
        selectors = [{"key": "app", "value": "web", "operator": "equals"}]
        assert build_advanced_label_selector(selectors) == "app=web"

    def test_exists_operator(self):
        selectors = [{"key": "tier", "operator": "exists"}]
        assert build_advanced_label_selector(selectors) == "tier"

    def test_not_equals_operator(self):
        selectors = [{"key": "env", "value": "staging", "operator": "not_equals"}]
        assert build_advanced_label_selector(selectors) == "env!=staging"

    def test_in_operator(self):
        selectors = [{"key": "env", "value": "prod,staging", "operator": "in"}]
        assert build_advanced_label_selector(selectors) == "env in (prod,staging)"

    def test_not_in_operator(self):
        selectors = [{"key": "env", "value": "dev,test", "operator": "not_in"}]
        assert build_advanced_label_selector(selectors) == "env notin (dev,test)"

    def test_multiple_selectors(self):
        selectors = [
            {"key": "app", "value": "web", "operator": "equals"},
            {"key": "env", "value": "prod", "operator": "equals"},
        ]
        result = build_advanced_label_selector(selectors)
        assert result == "app=web,env=prod"

    def test_empty_selectors(self):
        assert build_advanced_label_selector([]) == ""

    def test_selector_with_empty_key_skipped(self):
        selectors = [{"key": "", "value": "val", "operator": "equals"}]
        assert build_advanced_label_selector(selectors) == ""


# ---------------------------------------------------------------------------
# get_resource_api_info
# ---------------------------------------------------------------------------


class TestGetResourceApiInfo:
    """Test API info lookup for Kubernetes resource types."""

    def test_pods_api_info(self):
        info = get_resource_api_info("pods")
        assert info is not None
        assert info["api"] == "core_v1"
        assert info["namespaced"] is True

    def test_deployments_api_info(self):
        info = get_resource_api_info("deployments")
        assert info is not None
        assert info["api"] == "apps_v1"

    def test_pipelineruns_api_info(self):
        info = get_resource_api_info("pipelineruns")
        assert info is not None
        assert info["api"] == "custom"
        assert info["group"] == "tekton.dev"

    def test_unknown_resource_returns_none(self):
        assert get_resource_api_info("foobar") is None

    def test_case_insensitive_lookup(self):
        info = get_resource_api_info("Pods")
        assert info is not None
        assert info["api"] == "core_v1"


# ---------------------------------------------------------------------------
# extract_resource_info
# ---------------------------------------------------------------------------


class TestExtractResourceInfo:
    """Test resource info extraction from raw Kubernetes resource dicts."""

    def test_basic_extraction(self):
        raw = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": "my-pod",
                "namespace": "default",
                "labels": {"app": "web"},
                "annotations": {"note": "test"},
                "creationTimestamp": "2025-01-01T00:00:00Z",
                "resourceVersion": "12345",
                "uid": "abc-123",
            },
        }
        result = extract_resource_info(raw, include_spec=False, include_status=False)
        assert result["kind"] == "Pod"
        assert result["metadata"]["name"] == "my-pod"
        assert result["metadata"]["namespace"] == "default"
        assert result["metadata"]["labels"] == {"app": "web"}
        assert result["metadata"]["creation_timestamp"] == "2025-01-01T00:00:00Z"

    def test_extraction_with_spec(self):
        raw = {
            "kind": "Deployment",
            "metadata": {"name": "dep-1", "namespace": "ns"},
            "spec": {"replicas": 3},
        }
        result = extract_resource_info(raw, include_spec=True, include_status=False)
        assert result["spec"] == {"replicas": 3}

    def test_extraction_with_status(self):
        raw = {
            "kind": "Deployment",
            "metadata": {"name": "dep-1", "namespace": "ns"},
            "status": {"phase": "Running", "conditions": [], "readyReplicas": 3},
        }
        result = extract_resource_info(raw, include_spec=False, include_status=True)
        assert result["status"]["phase"] == "Running"
        assert result["status"]["ready_replicas"] == 3

    def test_extraction_with_missing_metadata(self):
        """Resources with missing metadata fields should use empty defaults."""
        raw = {"kind": "Pod", "metadata": {}}
        result = extract_resource_info(raw, include_spec=False, include_status=False)
        assert result["metadata"]["name"] == ""
        assert result["metadata"]["namespace"] == ""
        assert result["metadata"]["labels"] == {}

    def test_extraction_with_resource_type_hint(self):
        """When kind is missing from resource, use resource_type_hint as fallback."""
        raw = {"metadata": {"name": "my-pod", "namespace": "default"}}
        result = extract_resource_info(
            raw, include_spec=False, include_status=False, resource_type_hint="pods"
        )
        assert result["kind"] == "Pod"

    def test_extraction_snake_case_keys(self):
        """Handle Python client snake_case keys (from to_dict())."""
        raw = {
            "kind": "Pod",
            "api_version": "v1",
            "metadata": {
                "name": "my-pod",
                "namespace": "default",
                "creation_timestamp": "2025-01-01T00:00:00Z",
                "resource_version": "999",
            },
        }
        result = extract_resource_info(raw, include_spec=False, include_status=False)
        assert result["metadata"]["creation_timestamp"] == "2025-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# analyze_labels
# ---------------------------------------------------------------------------


class TestAnalyzeLabels:
    """Test label analysis across resources."""

    def test_analyze_common_labels(self):
        resources = [
            make_resource("pod-1", labels={"app": "web", "env": "prod"}),
            make_resource("pod-2", labels={"app": "web", "env": "staging"}),
            make_resource("pod-3", labels={"app": "api"}),
        ]
        analysis = analyze_labels(resources)
        assert "common_labels" in analysis
        # 'app' appears in all 3 resources
        app_label = next((lbl for lbl in analysis["common_labels"] if lbl["key"] == "app"), None)
        assert app_label is not None
        assert app_label["frequency"] == 3

    def test_analyze_unique_labels(self):
        resources = [
            make_resource("pod-1", labels={"singleton": "value"}),
            make_resource("pod-2", labels={"other": "x"}),
        ]
        analysis = analyze_labels(resources)
        assert "unique_labels" in analysis
        # Each label key has exactly one unique value
        for ul in analysis["unique_labels"]:
            assert "key" in ul
            assert "value" in ul

    def test_analyze_empty_labels(self):
        resources = [
            make_resource("pod-1", labels={}),
            make_resource("pod-2", labels={}),
        ]
        analysis = analyze_labels(resources)
        assert analysis["common_labels"] == []
        assert analysis["unique_labels"] == []

    def test_analyze_no_resources(self):
        analysis = analyze_labels([])
        assert analysis["common_labels"] == []
        assert analysis["unique_labels"] == []


# ---------------------------------------------------------------------------
# calculate_namespace_distribution
# ---------------------------------------------------------------------------


class TestCalculateNamespaceDistribution:
    """Test namespace distribution calculation."""

    def test_single_namespace(self):
        resources = [
            make_resource("pod-1", namespace="default"),
            make_resource("pod-2", namespace="default"),
        ]
        dist = calculate_namespace_distribution(resources)
        assert len(dist) == 1
        assert dist[0]["namespace"] == "default"
        assert dist[0]["resource_count"] == 2

    def test_multiple_namespaces(self):
        resources = [
            make_resource("pod-1", namespace="ns-a"),
            make_resource("pod-2", namespace="ns-b"),
            make_resource("pod-3", namespace="ns-a"),
        ]
        dist = calculate_namespace_distribution(resources)
        assert len(dist) == 2
        # Should be sorted by resource count descending
        assert dist[0]["resource_count"] >= dist[1]["resource_count"]

    def test_empty_resources(self):
        dist = calculate_namespace_distribution([])
        assert dist == []

    def test_distribution_includes_resource_types(self):
        resources = [
            {**make_resource("pod-1", namespace="ns"), "kind": "Pod"},
            {**make_resource("svc-1", namespace="ns"), "kind": "Service"},
        ]
        dist = calculate_namespace_distribution(resources)
        assert len(dist) == 1
        assert set(dist[0]["resource_types"]) == {"Pod", "Service"}
