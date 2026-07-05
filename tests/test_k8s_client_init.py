"""
Tests for Kubernetes client initialization in server-mcp.py.

These tests verify the structural and behavioural properties of the K8s
client initialization code, guarding against:

- Duplicate config loading (config.load_incluster_config / load_kube_config
  called more than once).
- Split-brain: KubeArchiveEndpointDiscovery holding references to client
  objects from one init pass while the rest of the application uses objects
  from a different init pass.
- Missing k8s_autoscaling_api in the consolidated init block.
- Inconsistent None-setting when client construction fails.
- Accidental removal or repositioning of cache variables.

The tests are source-analysis based (reading the .py file as text) so they
run without importing the full module and its heavy side-effects.
"""

import re
import subprocess

import pytest


# =========================================================================
# 1. test_grep_single_config_calls
#    Run 'grep -c' on the source and assert counts are exactly 1.
# =========================================================================


class TestGrepSingleConfigCalls:
    """Static structural test: no duplicate config loading calls."""

    def test_single_incluster_config_call(self, server_mcp_path):
        """config.load_incluster_config must appear exactly once."""
        result = subprocess.run(
            ["grep", "-c", "config.load_incluster_config", str(server_mcp_path)],
            capture_output=True,
            text=True,
        )
        count = int(result.stdout.strip())
        assert count == 1, (
            f"Expected exactly 1 call to config.load_incluster_config, found {count}. "
            "Duplicate Kubernetes config loading detected."
        )

    def test_single_kubeconfig_call(self, server_mcp_path):
        """config.load_kube_config must appear exactly once."""
        result = subprocess.run(
            ["grep", "-c", "config.load_kube_config", str(server_mcp_path)],
            capture_output=True,
            text=True,
        )
        count = int(result.stdout.strip())
        assert count == 1, (
            f"Expected exactly 1 call to config.load_kube_config, found {count}. "
            "Duplicate Kubernetes config loading detected."
        )


# =========================================================================
# 2. test_single_config_load_call
#    Each K8s API client variable is assigned via client.*Api() exactly once.
# =========================================================================


class TestSingleInitBlock:
    """Verify exactly one initialization for each K8s API client variable."""

    @pytest.mark.parametrize(
        "var_name, class_name",
        [
            ("k8s_core_api", "CoreV1Api"),
            ("k8s_apps_api", "AppsV1Api"),
            ("k8s_custom_api", "CustomObjectsApi"),
            ("k8s_storage_api", "StorageV1Api"),
            ("k8s_batch_api", "BatchV1Api"),
            ("k8s_autoscaling_api", "AutoscalingV2Api"),
        ],
    )
    def test_single_client_init(self, source_code, var_name, class_name):
        """Each main client should be initialised exactly once."""
        pattern = rf"^\s*{var_name}\s*=\s*client\.{class_name}\(\)"
        matches = re.findall(pattern, source_code, re.MULTILINE)
        assert len(matches) == 1, (
            f"Expected 1 init of {var_name} = client.{class_name}(), "
            f"found {len(matches)}. Duplicate client initialization detected."
        )


# =========================================================================
# 3. test_incluster_config_succeeds  (structural proxy)
#    When load_incluster_config succeeds, load_kube_config should not be
#    called.  Verified structurally: load_kube_config is inside an except
#    block triggered only when load_incluster_config raises.
# =========================================================================


class TestInclusterConfigSucceeds:
    """
    The config loading structure must ensure load_kube_config is only
    called when load_incluster_config raises ConfigException.
    """

    def test_kubeconfig_inside_except_branch(self, source_code):
        """load_kube_config must appear inside an except block, not at top level."""
        # Find lines containing load_kube_config and check indentation
        lines = source_code.split("\n")
        for i, line in enumerate(lines):
            if "config.load_kube_config()" in line and not line.strip().startswith("#"):
                indent = len(line) - len(line.lstrip())
                assert indent >= 8, (
                    f"config.load_kube_config() at line {i+1} has indent {indent}. "
                    "It should be inside a nested except block (indent >= 8), "
                    "not at the same level as load_incluster_config."
                )


# =========================================================================
# 4. test_incluster_fails_kubeconfig_succeeds
#    The nested try/except pattern handles fallback to kubeconfig.
# =========================================================================
# (Covered implicitly by TestRobustConfigHandling below.)


# =========================================================================
# 5. test_both_config_methods_fail
#    The nested try/except logs a warning without crashing.
# =========================================================================


class TestRobustConfigHandling:
    """
    The init block must use nested try/except so that when BOTH
    load_incluster_config and load_kube_config fail, a warning is logged
    instead of crashing.
    """

    def test_nested_try_except_for_config(self, source_code):
        """
        Expected pattern:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                try:
                    config.load_kube_config()
                except config.ConfigException:
                    logger.warning(...)
        """
        nested_pattern = re.compile(
            r"try:\s*\n\s*config\.load_incluster_config\(\)"
            r".*?"
            r"except\s+config\.ConfigException:\s*\n"
            r"\s*try:\s*\n"
            r"\s*config\.load_kube_config\(\)"
            r".*?"
            r"except\s+config\.ConfigException:\s*\n"
            r"\s*logger\.warning\(",
            re.DOTALL,
        )
        assert nested_pattern.search(source_code) is not None, (
            "Could not find the robust nested try/except pattern for config loading. "
            "Expected: try load_incluster_config -> except: try load_kube_config -> except: warn."
        )

    def test_warning_message_for_missing_config(self, source_code):
        """The warning about missing config must be present."""
        assert "No Kubernetes configuration found" in source_code, (
            "Missing warning message for when no Kubernetes configuration is found."
        )


# =========================================================================
# 6. test_client_construction_fails_all_none
#    When client construction raises, ALL seven client vars are set to None.
# =========================================================================


class TestClientNoneOnFailure:
    """When client construction fails, every client variable must be None."""

    def test_all_main_clients_set_to_none_in_except(self, source_code):
        """The except branch must set all six main clients to None."""
        required = [
            "k8s_core_api",
            "k8s_apps_api",
            "k8s_custom_api",
            "k8s_storage_api",
            "k8s_batch_api",
            "k8s_autoscaling_api",
        ]

        # Find except blocks that handle client-init failure.
        except_blocks = re.findall(
            r"except\s+Exception.*?:\s*\n((?:\s+.*\n)*)",
            source_code,
        )

        block_with_core_none = None
        for block in except_blocks:
            if "k8s_core_api = None" in block:
                block_with_core_none = block
                break

        assert block_with_core_none is not None, (
            "No except block found that sets k8s_core_api = None."
        )

        for var in required:
            assert f"{var} = None" in block_with_core_none, (
                f"{var} is not set to None in the except block. "
                "All client variables must be set to None when init fails."
            )

    def test_networking_api_set_to_none_on_failure(self, source_code):
        """k8s_networking_api must be set to None when NetworkingV1Api fails."""
        assert "k8s_networking_api = None" in source_code, (
            "k8s_networking_api is never set to None on failure."
        )


# =========================================================================
# 7. test_networking_api_failure_independent
#    NetworkingV1Api has its own try/except so its failure does not null-out
#    the other six clients.
# =========================================================================


class TestNetworkingApiIndependence:
    """NetworkingV1Api must be initialised in its own independent try/except."""

    def test_separate_try_except_for_networking(self, source_code):
        """There must be exactly one standalone try/except for NetworkingV1Api."""
        pattern = (
            r"try:\s*\n"
            r"\s*k8s_networking_api\s*=\s*client\.NetworkingV1Api\(\)\s*\n"
            r"\s*except"
        )
        matches = re.findall(pattern, source_code)
        assert len(matches) == 1, (
            f"Expected 1 independent try/except for NetworkingV1Api, "
            f"found {len(matches)}."
        )

    def test_networking_not_in_main_init_block(self, source_code):
        """NetworkingV1Api must NOT be in the same try block as CoreV1Api."""
        # Split source at try:/except boundaries and look for blocks
        # containing both CoreV1Api and NetworkingV1Api.
        segments = re.split(r"^\s*(?:try:|except\s)", source_code, flags=re.MULTILINE)
        for seg in segments:
            if "k8s_core_api = client.CoreV1Api()" in seg:
                assert "k8s_networking_api = client.NetworkingV1Api()" not in seg, (
                    "NetworkingV1Api is in the same try block as CoreV1Api. "
                    "A NetworkingV1Api failure would null-out ALL clients."
                )


# =========================================================================
# 8. test_no_split_brain_same_objects
#    Core regression test: no config loading or client re-init happens AFTER
#    KubeArchiveEndpointDiscovery is constructed.  If it did, KubeArchive
#    would hold stale references while other tools use fresh ones.
# =========================================================================


class TestNoSplitBrain:
    """
    KubeArchiveEndpointDiscovery must receive the SAME client objects that
    the rest of the application uses -- no second init pass afterwards.
    """

    def test_no_config_load_after_kubearchive(self, source_code):
        """No config loading must occur after KubeArchiveEndpointDiscovery."""
        marker = "kubearchive_endpoint_discovery = KubeArchiveEndpointDiscovery("
        pos = source_code.find(marker)
        assert pos != -1, "KubeArchiveEndpointDiscovery construction not found"

        after = source_code[pos:]
        incluster = re.findall(r"config\.load_incluster_config\(\)", after)
        kubeconfig = re.findall(r"config\.load_kube_config\(\)", after)

        assert len(incluster) == 0, (
            f"Found {len(incluster)} call(s) to config.load_incluster_config AFTER "
            "KubeArchiveEndpointDiscovery. This causes split-brain."
        )
        assert len(kubeconfig) == 0, (
            f"Found {len(kubeconfig)} call(s) to config.load_kube_config AFTER "
            "KubeArchiveEndpointDiscovery. This causes split-brain."
        )

    def test_no_client_reinit_after_kubearchive(self, source_code):
        """No k8s_*_api reassignment must occur after KubeArchiveEndpointDiscovery."""
        marker = "kubearchive_endpoint_discovery = KubeArchiveEndpointDiscovery("
        pos = source_code.find(marker)
        assert pos != -1

        after = source_code[pos:]
        reassignments = re.findall(
            r"^\s*k8s_(?:core|apps|custom|storage|batch)_api\s*=\s*client\.",
            after,
            re.MULTILINE,
        )
        assert len(reassignments) == 0, (
            f"Found {len(reassignments)} K8s client reassignment(s) AFTER "
            "KubeArchiveEndpointDiscovery. KubeArchive would hold stale references."
        )

    def test_all_inits_before_kubearchive(self, source_code):
        """Every k8s_core_api = client.CoreV1Api() must precede KubeArchiveEndpointDiscovery."""
        ka_pos = source_code.find(
            "kubearchive_endpoint_discovery = KubeArchiveEndpointDiscovery("
        )
        assert ka_pos != -1

        for m in re.finditer(r"k8s_core_api\s*=\s*client\.CoreV1Api\(\)", source_code):
            assert m.start() < ka_pos, (
                f"k8s_core_api init at char offset {m.start()} appears AFTER "
                f"KubeArchiveEndpointDiscovery at {ka_pos}."
            )


# =========================================================================
# 9. test_autoscaling_api_available_at_module_level
#    k8s_autoscaling_api is initialised in the consolidated block (not only
#    in a later, separate block).
# =========================================================================


class TestAutoscalingApiIncluded:
    """k8s_autoscaling_api must be part of the single consolidated init block."""

    def test_autoscaling_near_core_api(self, source_lines):
        """
        AutoscalingV2Api init must be within 10 lines of CoreV1Api init,
        confirming they live in the same block.  Requires exactly one
        occurrence of each.
        """
        core = [i for i, l in enumerate(source_lines) if "k8s_core_api = client.CoreV1Api()" in l]
        auto = [
            i
            for i, l in enumerate(source_lines)
            if "k8s_autoscaling_api = client.AutoscalingV2Api()" in l
        ]

        assert len(core) == 1, (
            f"Expected 1 CoreV1Api init, found {len(core)}. "
            "There must be a single consolidated init block."
        )
        assert len(auto) == 1, (
            f"Expected 1 AutoscalingV2Api init, found {len(auto)}."
        )
        assert abs(core[0] - auto[0]) <= 10, (
            f"CoreV1Api at line {core[0]+1} and AutoscalingV2Api at line {auto[0]+1} "
            f"are {abs(core[0] - auto[0])} lines apart -- they should be in the same block."
        )

    def test_autoscaling_before_kubearchive(self, source_code):
        """AutoscalingV2Api init must appear before KubeArchiveEndpointDiscovery."""
        auto_match = re.search(
            r"k8s_autoscaling_api\s*=\s*client\.AutoscalingV2Api\(\)", source_code
        )
        ka_match = re.search(
            r"kubearchive_endpoint_discovery\s*=\s*KubeArchiveEndpointDiscovery\(",
            source_code,
        )
        assert auto_match is not None, "AutoscalingV2Api init not found"
        assert ka_match is not None, "KubeArchiveEndpointDiscovery not found"
        assert auto_match.start() < ka_match.start(), (
            "k8s_autoscaling_api is initialised AFTER KubeArchiveEndpointDiscovery. "
            "It must be part of the consolidated init block that precedes KubeArchive."
        )


# =========================================================================
# 10. test_cache_variables_after_init
#     _kubearchive_host_cache and KUBEARCHIVE_CACHE_TTL_SEC exist and appear
#     after KubeArchiveEndpointDiscovery construction.
# =========================================================================


class TestCacheVariablesAfterInit:
    """Cache variables must exist with correct defaults, positioned after KubeArchive init."""

    def test_cache_variables_exist(self, source_code):
        """Both cache variables must be present in the source."""
        assert "_kubearchive_host_cache" in source_code, (
            "_kubearchive_host_cache variable not found"
        )
        assert "KUBEARCHIVE_CACHE_TTL_SEC" in source_code, (
            "KUBEARCHIVE_CACHE_TTL_SEC variable not found"
        )

    def test_cache_defaults(self, source_code):
        """Cache variables must have the expected default values."""
        assert '{"host": None, "ts": 0}' in source_code, (
            '_kubearchive_host_cache not initialised with {"host": None, "ts": 0}'
        )
        assert "KUBEARCHIVE_CACHE_TTL_SEC = 300" in source_code, (
            "KUBEARCHIVE_CACHE_TTL_SEC not set to 300"
        )

    def test_cache_after_kubearchive_discovery(self, source_code):
        """Cache variables must appear after KubeArchiveEndpointDiscovery construction."""
        ka_pos = source_code.find(
            "kubearchive_endpoint_discovery = KubeArchiveEndpointDiscovery("
        )
        cache_pos = source_code.find("_kubearchive_host_cache")
        ttl_pos = source_code.find("KUBEARCHIVE_CACHE_TTL_SEC = 300")

        assert ka_pos > 0, "KubeArchiveEndpointDiscovery construction not found"
        assert cache_pos > ka_pos, (
            "_kubearchive_host_cache appears before KubeArchiveEndpointDiscovery"
        )
        assert ttl_pos > ka_pos, (
            "KUBEARCHIVE_CACHE_TTL_SEC appears before KubeArchiveEndpointDiscovery"
        )
