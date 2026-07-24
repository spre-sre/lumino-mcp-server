"""
KubeArchive API Client Module

This module provides functionality to interact with KubeArchive API for retrieving
archived Kubernetes and Tekton resources and their logs.

KubeArchive stores Kubernetes resources off-cluster and provides a REST API
for accessing historical resource states and logs.
"""

import asyncio
import os
import re
import logging
import aiohttp
import ssl
import base64
import tempfile
import subprocess
import socket
import yaml
import re
import atexit
import signal
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime
from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger("lumino-mcp.kubearchive")

# Global KubeArchive client instance (cached to avoid re-discovery)
ka_client = None

async def setup_kubearchive_client(
    endpoint_discovery: 'KubeArchiveEndpointDiscovery',
    k8s_core_api: client.CoreV1Api
) -> Optional['KubeArchiveClient']:
    """
    Initialize and cache the KubeArchive client singleton.

    Discovers the KubeArchive API endpoint via endpoint_discovery and
    constructs a KubeArchiveClient on first call. The client is cached in
    the module-level ka_client global, so subsequent calls skip
    re-discovery and return the same instance.

    Args:
        endpoint_discovery: Discovery helper used to locate the KubeArchive
            API endpoint in the cluster.
        k8s_core_api: Kubernetes CoreV1Api client, passed through to
            KubeArchiveClient for fetching TLS certificates.

    Returns:
        The cached KubeArchiveClient, or None if endpoint discovery fails.
    """
    global ka_client
    if ka_client is None:
        logger.info(f"KubeArchive client is not initialized, setting up...")

        # Discover KubeArchive endpoint using auto-discovery
        logger.info(f"Discovering KubeArchive API endpoint...")
        ka_endpoint = await endpoint_discovery.discover_endpoint()

        if not ka_endpoint:
            return None

        logger.info(f"Using KubeArchive endpoint: {ka_endpoint}")

        # Initialize KubeArchive client with discovered endpoint
        ka_client = KubeArchiveClient(
            endpoint_discovery=endpoint_discovery,
            k8s_core_api=k8s_core_api
        )
    else:
        logger.info(f"Reusing cached KubeArchive client")

    return ka_client

# ============================================================================
# KUBEARCHIVE ENDPOINT DISCOVERY
# ============================================================================

class KubeArchiveEndpointDiscovery:
    """
    Handles auto-detection and caching of KubeArchive API endpoint.

    Auto-detection strategy:
    1. Check for KUBEARCHIVE_HOST environment variable
    2. Check for kubearchive-api-server Route in common namespaces (OpenShift)
    3. Check for kubearchive-api-server Ingress in common namespaces (Kubernetes)
    4. Check for kubearchive-api-server Service in common namespaces (fallback for both)
    5. Infer Route URL from kubeconfig cluster domain (constructs candidate URLs
       from the API server URL pattern and probes them with health checks)
    6. Auto-setup kubectl port-forward for local development (if in-cluster endpoint detected)
    7. Cache results at startup; re-probe on failure

    Supports both OpenShift (via Routes) and Kubernetes (via Ingress) clusters.

    Local Development Mode:
    - Automatically detects when running outside the cluster
    - Sets up kubectl port-forward to enable local access
    - Manages port-forward lifecycle (start/stop/cleanup)
    - Falls back to manual configuration if kubectl unavailable
    """

    def __init__(self, k8s_core_api: client.CoreV1Api, k8s_custom_api: client.CustomObjectsApi, k8s_networking_api: Optional[client.NetworkingV1Api] = None, auto_port_forward: bool = True):
        self.k8s_core_api = k8s_core_api
        self.k8s_custom_api = k8s_custom_api
        self.k8s_networking_api = k8s_networking_api
        self._cached_endpoint: Optional[str] = None
        self._enabled = os.getenv('KUBEARCHIVE_ENABLED', 'true').lower() != 'false'
        self._common_namespaces = ['kubearchive', 'product-kubearchive', 'default']
        self._auto_port_forward = auto_port_forward
        self._port_forward_process: Optional[subprocess.Popen] = None
        self._port_forward_port: Optional[int] = None
        self._discovered_namespace: Optional[str] = None
        self._discovered_port: Optional[int] = None
        self._atexit_registered = False
        self._signal_handlers_registered = False
        self._original_sigterm_handler = None
        self._original_sigint_handler = None

    async def discover_endpoint(self, force_refresh: bool = False) -> Optional[str]:
        """
        Discover KubeArchive API endpoint using auto-detection strategy.

        Auto-detection tries the following in order:
        1. KUBEARCHIVE_HOST environment variable
        2. OpenShift Route (for OpenShift clusters)
        3. Kubernetes Ingress (for Kubernetes clusters)
        4. Kubernetes Service (fallback for both, uses in-cluster DNS)
        5. Kubeconfig-based Route inference (constructs candidate URLs from
           the cluster domain in the kubeconfig and probes them)

        Args:
            force_refresh: Force re-discovery even if cached endpoint exists

        Returns:
            KubeArchive API endpoint URL or None if not found
        """
        if not self._enabled:
            logger.info("KubeArchive integration is disabled via KUBEARCHIVE_ENABLED")
            return None

        # Return cached endpoint if available and not forcing refresh
        if self._cached_endpoint and not force_refresh:
            logger.debug(f"Using cached KubeArchive endpoint: {self._cached_endpoint}")
            return self._cached_endpoint

        # Log deployment context
        if self._is_running_in_cluster():
            logger.info("Running inside Kubernetes cluster")
        else:
            logger.info("Running locally (outside cluster)")

        # Detect platform type
        is_openshift = self._is_openshift_cluster()
        if is_openshift:
            logger.info("Detected OpenShift platform")
        else:
            logger.info("Detected Kubernetes platform")

        # Step 1: Check environment variable
        env_host = os.getenv('KUBEARCHIVE_HOST')
        if env_host:
            logger.info(f"KubeArchive endpoint from KUBEARCHIVE_HOST: {env_host}")
            self._cached_endpoint = env_host
            return env_host

        # Step 2: Check for OpenShift Route (OpenShift clusters)
        endpoint = await self._check_route()
        if endpoint:
            logger.info(f"KubeArchive endpoint discovered via OpenShift Route: {endpoint}")
            self._cached_endpoint = endpoint
            return endpoint

        # Step 3: Check for Kubernetes Ingress (Kubernetes clusters)
        endpoint = await self._check_ingress()
        if endpoint:
            logger.info(f"KubeArchive endpoint discovered via Kubernetes Ingress: {endpoint}")
            self._cached_endpoint = endpoint
            return endpoint

        # Step 4: Check for Service (fallback for both)
        endpoint = await self._check_service()
        if endpoint:
            logger.info(f"KubeArchive endpoint discovered via Service: {endpoint}")

            # Check if we need to setup port-forward for local development
            if self._auto_port_forward and self._is_in_cluster_endpoint(endpoint) and not self._is_running_in_cluster():
                logger.info("Detected in-cluster endpoint while running locally - attempting automatic port-forward")
                local_endpoint = await self._setup_port_forward(endpoint, self._discovered_namespace or 'kubearchive')
                if local_endpoint:
                    self._cached_endpoint = local_endpoint
                    return local_endpoint
                else:
                    logger.warning("Port-forward setup failed, using in-cluster endpoint (will likely fail)")

            self._cached_endpoint = endpoint
            return endpoint

        # Step 5: Infer Route URL from kubeconfig cluster domain
        endpoint = await self._check_kubeconfig_route_inference()
        if endpoint:
            logger.info(f"KubeArchive endpoint discovered via kubeconfig route inference: {endpoint}")
            self._cached_endpoint = endpoint
            return endpoint

        logger.warning("KubeArchive endpoint not found. Set KUBEARCHIVE_HOST or deploy kubearchive-api-server")
        return None

    async def _check_route(self) -> Optional[str]:
        """Check for kubearchive-api-server Route in common namespaces (OpenShift)."""
        try:
            for namespace in self._common_namespaces:
                try:
                    route = self.k8s_custom_api.get_namespaced_custom_object(
                        group='route.openshift.io',
                        version='v1',
                        namespace=namespace,
                        plural='routes',
                        name='kubearchive-api-server'
                    )

                    # Extract host from route spec
                    host = route.get('spec', {}).get('host')
                    if host:
                        # Determine protocol (tls vs non-tls)
                        tls = route.get('spec', {}).get('tls')
                        protocol = 'https' if tls else 'http'
                        return f"{protocol}://{host}"

                except ApiException as e:
                    if e.status == 404:
                        continue  # Try next namespace
                    logger.debug(f"Error checking route in {namespace}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error checking OpenShift routes: {e}")

        return None

    async def _check_ingress(self) -> Optional[str]:
        """Check for kubearchive-api-server Ingress in common namespaces (Kubernetes)."""
        if not self.k8s_networking_api:
            logger.debug("NetworkingV1Api not available, skipping Ingress check")
            return None

        try:
            for namespace in self._common_namespaces:
                try:
                    ingress = self.k8s_networking_api.read_namespaced_ingress(
                        name='kubearchive-api-server',
                        namespace=namespace
                    )

                    # Extract host from ingress rules
                    if ingress.spec and ingress.spec.rules:
                        for rule in ingress.spec.rules:
                            if rule.host:
                                # Determine protocol from TLS configuration
                                protocol = 'http'
                                if ingress.spec.tls:
                                    # Check if this host is in TLS config
                                    for tls_config in ingress.spec.tls:
                                        if tls_config.hosts and rule.host in tls_config.hosts:
                                            protocol = 'https'
                                            break

                                return f"{protocol}://{rule.host}"

                    # Alternative: check ingress status loadBalancer
                    if ingress.status and ingress.status.load_balancer and ingress.status.load_balancer.ingress:
                        for lb in ingress.status.load_balancer.ingress:
                            if lb.hostname:
                                protocol = 'https' if ingress.spec.tls else 'http'
                                return f"{protocol}://{lb.hostname}"
                            elif lb.ip:
                                protocol = 'https' if ingress.spec.tls else 'http'
                                return f"{protocol}://{lb.ip}"

                except ApiException as e:
                    if e.status == 404:
                        continue  # Try next namespace
                    logger.debug(f"Error checking ingress in {namespace}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error checking Kubernetes ingress: {e}")

        return None

    async def _check_kubeconfig_route_inference(self) -> Optional[str]:
        """
        Infer KubeArchive Route URLs from the kubeconfig cluster server URL.

        On OpenShift, Routes follow a predictable pattern:
            https://<route-name>-<namespace>.apps.<cluster-domain>

        From an API server URL like https://api.cluster-foo.example.com:6443,
        we extract the cluster domain (cluster-foo.example.com) and construct
        candidate KubeArchive Route URLs for each namespace in
        _common_namespaces.

        Each candidate is probed with an HTTP health check (/livez) to verify
        reachability before being returned.

        Returns:
            Reachable KubeArchive endpoint URL or None if no candidate is reachable.
        """
        try:
            from kubernetes import config as k8s_config

            # Load kubeconfig contexts to get the active cluster server URL
            try:
                contexts, active_context = k8s_config.list_kube_config_contexts()
            except Exception as e:
                logger.debug(f"Could not load kubeconfig contexts: {e}")
                return None

            if not active_context:
                logger.debug("No active kubeconfig context found")
                return None

            cluster_name = active_context.get('context', {}).get('cluster')
            if not cluster_name:
                logger.debug("No cluster name in active kubeconfig context")
                return None

            # Load the kubeconfig file to extract the cluster server URL
            kubeconfig_path = os.getenv('KUBECONFIG', os.path.expanduser('~/.kube/config'))
            try:
                with open(kubeconfig_path, 'r') as f:
                    kubeconfig = yaml.safe_load(f)
            except Exception as e:
                logger.debug(f"Could not read kubeconfig file at {kubeconfig_path}: {e}")
                return None

            # Find the cluster entry matching the active context
            server_url = None
            for cluster_entry in kubeconfig.get('clusters', []):
                if cluster_entry.get('name') == cluster_name:
                    server_url = cluster_entry.get('cluster', {}).get('server')
                    break

            if not server_url:
                logger.debug(f"No server URL found for cluster '{cluster_name}' in kubeconfig")
                return None

            logger.debug(f"Kubeconfig cluster server URL: {server_url}")

            match = re.match(r'https?://api\.(.+?)(?::\d+)?/?$', server_url)
            if not match:
                logger.debug(
                    f"API server URL '{server_url}' does not match expected "
                    f"pattern 'https://api.<cluster-domain>:<port>'"
                )
                return None

            cluster_domain = match.group(1)
            logger.info(f"Extracted cluster domain from kubeconfig: {cluster_domain}")

            # Construct candidate Route URLs for each namespace.
            # OpenShift Route pattern: https://<route-name>-<namespace>.apps.<cluster-domain>
            route_name = 'kubearchive-api-server'
            candidates = []
            for namespace in self._common_namespaces:
                url = f"https://{route_name}-{namespace}.apps.{cluster_domain}"
                candidates.append(url)

            logger.info(
                f"Probing {len(candidates)} candidate KubeArchive Route URLs "
                f"inferred from kubeconfig"
            )

            # Probe each candidate with an HTTP health check
            for candidate_url in candidates:
                logger.debug(f"Probing candidate: {candidate_url}/livez")
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"{candidate_url}/livez",
                            ssl=False,
                            timeout=aiohttp.ClientTimeout(total=5),
                        ) as response:
                            if response.status == 200:
                                logger.info(
                                    f"Candidate reachable: {candidate_url} "
                                    f"(status {response.status})"
                                )
                                return candidate_url
                            else:
                                logger.debug(
                                    f"Candidate {candidate_url} returned "
                                    f"status {response.status}"
                                )
                except aiohttp.ClientError as e:
                    logger.debug(f"Candidate {candidate_url} unreachable: {e}")
                except Exception as e:
                    logger.debug(f"Error probing candidate {candidate_url}: {e}")

            logger.debug("No inferred KubeArchive Route candidate was reachable")
            return None

        except Exception as e:
            logger.debug(f"Error in kubeconfig-based route inference: {e}")
            return None

    async def _check_service(self) -> Optional[str]:
        """Check for kubearchive-api-server Service in common namespaces."""
        try:
            for namespace in self._common_namespaces:
                try:
                    service = self.k8s_core_api.read_namespaced_service(
                        name='kubearchive-api-server',
                        namespace=namespace
                    )

                    # Build service URL
                    # In-cluster access: https://kubearchive-api-server.<namespace>.svc.cluster.local:8081
                    # KubeArchive typically uses HTTPS (TLS)
                    service_name = service.metadata.name
                    port = 8081  # Default KubeArchive port
                    protocol = 'https'  # Default to HTTPS for KubeArchive

                    # Try to get port from service spec
                    if service.spec.ports:
                        port = service.spec.ports[0].port
                        # Check port name to determine protocol
                        port_name = service.spec.ports[0].name
                        if port_name and 'http' in port_name.lower() and 'https' not in port_name.lower():
                            protocol = 'http'

                    # Store namespace and port for potential port-forwarding
                    self._discovered_namespace = namespace
                    self._discovered_port = port

                    return f"{protocol}://{service_name}.{namespace}.svc.cluster.local:{port}"

                except ApiException as e:
                    if e.status == 404:
                        continue  # Try next namespace
                    logger.debug(f"Error checking service in {namespace}: {e}")
                    continue

        except Exception as e:
            logger.debug(f"Error checking Kubernetes services: {e}")

        return None

    def _is_in_cluster_endpoint(self, endpoint: str) -> bool:
        """Check if endpoint is an in-cluster DNS name."""
        return '.svc.cluster.local' in endpoint

    def _is_running_in_cluster(self) -> bool:
        """Check if we're running inside a Kubernetes cluster."""
        # Check for in-cluster service account token
        return os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token')

    def _is_openshift_cluster(self) -> bool:
        """
        Check if we're running on an OpenShift cluster.

        Returns:
            True if OpenShift cluster, False otherwise
        """
        try:
            # Try to list routes in any namespace - if this works, it's OpenShift
            self.k8s_custom_api.list_cluster_custom_object(
                group='route.openshift.io',
                version='v1',
                plural='routes',
                limit=1
            )
            logger.debug("Detected OpenShift cluster (route.openshift.io API available)")
            return True
        except Exception as e:
            logger.debug(f"Detected plain Kubernetes cluster (no OpenShift routes): {type(e).__name__}")
            return False

    def _find_available_port(self, preferred_port: int = 8081) -> int:
        """
        Find an available local port.

        Args:
            preferred_port: Port to try first

        Returns:
            Available port number
        """
        # Try preferred port first
        ports_to_try = [preferred_port] + list(range(8081, 8091))

        for port in ports_to_try:
            try:
                # Try to bind to the port
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('localhost', port))
                    return port
            except OSError:
                continue  # Port in use, try next

        # If all standard ports are taken, let the system assign one
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', 0))
            return s.getsockname()[1]

    async def _setup_port_forward(self, in_cluster_endpoint: str, namespace: str) -> Optional[str]:
        """
        Setup kubectl port-forward for local development.

        Args:
            in_cluster_endpoint: The in-cluster endpoint URL
            namespace: Kubernetes namespace

        Returns:
            Local endpoint URL or None if setup failed
        """
        try:
            # Extract protocol and port from in-cluster endpoint
            protocol = 'https' if in_cluster_endpoint.startswith('https://') else 'http'
            remote_port = self._discovered_port or 8081

            # Find available local port
            local_port = self._find_available_port(remote_port)

            # Start kubectl port-forward
            cmd = [
                'kubectl', 'port-forward',
                '-n', namespace,
                'svc/kubearchive-api-server',
                f'{local_port}:{remote_port}'
            ]

            logger.info(f"Starting port-forward: {' '.join(cmd)}")

            self._port_forward_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Wait for port-forward to establish.
            # Using asyncio.sleep (not time.sleep) to avoid blocking the event loop,
            # which would freeze all concurrent MCP tool requests for the duration.
            await asyncio.sleep(2)

            # Check if process is still running
            if self._port_forward_process.poll() is not None:
                stderr = self._port_forward_process.stderr.read().decode()
                logger.error(f"Port-forward failed to start: {stderr}")
                return None

            self._port_forward_port = local_port
            local_endpoint = f"{protocol}://localhost:{local_port}"

            # Register cleanup handlers
            self._register_cleanup_handlers()

            logger.info(f"✓ Port-forward established: {local_endpoint} -> {in_cluster_endpoint}")
            return local_endpoint

        except FileNotFoundError:
            logger.warning("kubectl not found in PATH. Cannot setup automatic port-forward")
            logger.info("Please run manually: kubectl port-forward -n kubearchive svc/kubearchive-api-server 8081:8081")
            return None
        except Exception as e:
            logger.error(f"Error setting up port-forward: {e}")
            return None

    def stop_port_forward(self, _from_signal_handler: bool = False) -> None:
        """
        Stop the port-forward process if running.

        Args:
            _from_signal_handler: Internal flag indicating call from signal handler.
                When True, skips signal handler restoration to prevent dual execution.
        """
        if self._port_forward_process:
            try:
                logger.info("Stopping port-forward...")
                self._port_forward_process.terminate()
                self._port_forward_process.wait(timeout=5)
                logger.info("✓ Port-forward stopped")
            except Exception as e:
                logger.debug(f"Error stopping port-forward: {e}")
                try:
                    self._port_forward_process.kill()
                except:
                    pass
            finally:
                self._port_forward_process = None
                self._port_forward_port = None

        self._unregister_cleanup_handlers(restore_signal_handlers=not _from_signal_handler)

    def _register_cleanup_handlers(self) -> None:
        """Register atexit and signal handlers for cleanup."""
        if not self._atexit_registered:
            atexit.register(self.stop_port_forward)
            self._atexit_registered = True

        if not self._signal_handlers_registered:
            try:
                self._original_sigterm_handler = signal.signal(signal.SIGTERM, self._signal_handler)
                try:
                    self._original_sigint_handler = signal.signal(signal.SIGINT, self._signal_handler)
                    self._signal_handlers_registered = True
                except (ValueError, OSError) as e:
                    # Rollback SIGTERM handler if SIGINT registration failed
                    signal.signal(signal.SIGTERM, self._original_sigterm_handler)
                    logger.debug(f"Could not register SIGINT handler (rolling back SIGTERM): {e}")
            except (ValueError, OSError) as e:
                logger.debug(f"Could not register signal handlers: {e}")

    def _unregister_cleanup_handlers(self, restore_signal_handlers: bool = True) -> None:
        """
        Unregister atexit and signal handlers.

        Args:
            restore_signal_handlers: If True, restore original signal handlers.
                Set to False when called from signal handler to prevent dual execution.
        """
        if self._atexit_registered:
            try:
                atexit.unregister(self.stop_port_forward)
                self._atexit_registered = False
            except Exception as e:
                logger.debug(f"Error unregistering atexit handler: {e}")

        if self._signal_handlers_registered and restore_signal_handlers:
            try:
                if self._original_sigterm_handler is not None:
                    signal.signal(signal.SIGTERM, self._original_sigterm_handler)
                if self._original_sigint_handler is not None:
                    signal.signal(signal.SIGINT, self._original_sigint_handler)
                self._signal_handlers_registered = False
            except (ValueError, OSError) as e:
                logger.debug(f"Error restoring signal handlers: {e}")
        elif not restore_signal_handlers:
            # Mark handlers as unregistered without restoring them
            # (signal handler will restore them itself)
            self._signal_handlers_registered = False
    def _signal_handler(self, signum: int, frame) -> None:
        """
        Handle signals by cleaning up port-forward and calling original handler.

        Properly distinguishes between:
        - signal.SIG_DFL (0, falsy but valid): restore default and re-raise signal
        - signal.SIG_IGN (1): ignore signal after cleanup
        - callable handler: chain to original handler

        IMPORTANT: Calls stop_port_forward with _from_signal_handler=True to prevent
        dual handler execution (restore + explicit call).
        """
        logger.debug(f"Received signal {signum}, cleaning up port-forward")
        self.stop_port_forward(_from_signal_handler=True)

        # Determine the original handler based on signal type
        original_handler = None
        if signum == signal.SIGTERM:
            original_handler = self._original_sigterm_handler
        elif signum == signal.SIGINT:
            original_handler = self._original_sigint_handler

        # Handle different original handler types
        # CRITICAL: signal.SIG_DFL is 0 (falsy), so we must use identity check
        if original_handler is signal.SIG_DFL:
            # Restore default handler and re-raise signal to terminate process
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)
        elif original_handler is signal.SIG_IGN:
            # Original handler was SIG_IGN, do nothing after cleanup
            pass
        elif callable(original_handler):
            # Chain to original callable handler
            original_handler(signum, frame)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit: stop port-forward process."""
        self.stop_port_forward()
        return False

    def __del__(self):
        """Cleanup: stop port-forward process (last resort fallback)."""
        self.stop_port_forward()

    def clear_cache(self) -> None:
        """Clear cached endpoint (useful when endpoint becomes unavailable)."""
        self._cached_endpoint = None
    def get_cached_endpoint(self) -> Optional[str]:
        """Get cached KubeArchive endpoint if available."""
        return self._cached_endpoint


# ============================================================================
# KUBEARCHIVE API CLIENT
# ============================================================================

class KubeArchiveClient:
    """
    Client for interacting with KubeArchive REST API.

    Supports querying archived Kubernetes resources, filtering by labels/fields,
    time ranges, and retrieving container logs.
    """

    def __init__(self, endpoint_discovery: KubeArchiveEndpointDiscovery, k8s_auth_token: Optional[str] = None, k8s_core_api: Optional[client.CoreV1Api] = None):
        """
        Initialize KubeArchive client.

        Args:
            endpoint_discovery: KubeArchiveEndpointDiscovery instance
            k8s_auth_token: Kubernetes bearer token for authentication (optional, will be auto-detected)
            k8s_core_api: Kubernetes CoreV1Api for fetching TLS certificates (optional)
        """
        self.endpoint_discovery = endpoint_discovery
        self._auth_token = k8s_auth_token
        self.k8s_core_api = k8s_core_api
        self._ssl_context: Optional[ssl.SSLContext] = None
        self._ca_cert_path: Optional[str] = None
        self._ca_namespaces = ['kubearchive', 'product-kubearchive', 'default']
        self._ca_secret_names = ['kubearchive-ca', 'kubearchive-api-server-tls']

    async def _get_auth_token(self) -> Optional[str]:
        """
        Get Kubernetes bearer token for authentication.

        Returns token from (in priority order):
        1. Provided token (from constructor)
        2. In-cluster service account token (/var/run/secrets/kubernetes.io/serviceaccount/token)
        3. Existing k8s client token (from the API client initialized at server startup — guarantees
           the token matches the cluster the MCP server is connected to)
        4. OpenShift oc CLI token (oc whoami -t) - for OpenShift clusters only
        5. Auto-created token for local development (kubectl create token) - Kubernetes only, not OpenShift
        """
        if self._auth_token:
            return self._auth_token

        # Try in-cluster service account token
        token_path = '/var/run/secrets/kubernetes.io/serviceaccount/token'
        if os.path.exists(token_path):
            try:
                with open(token_path, 'r') as f:
                    token = f.read().strip()
                    logger.debug("Using in-cluster service account token")
                    return token
            except Exception as e:
                logger.debug(f"Error reading in-cluster token: {e}")

        # Extract token from the existing k8s client that was initialized at server startup.
        # This is the safest source: it guarantees the token matches the cluster the server
        # is actually connected to, even if the user switches kubeconfig contexts later.
        token = self._extract_token_from_client()
        if token:
            return token

        # For OpenShift clusters, try to get token from oc CLI (user's current session)
        if self._is_openshift_cluster():
            logger.info("Detected OpenShift cluster, attempting to use oc login token")
            oc_token = await self._get_openshift_token()
            if oc_token:
                logger.info("Using token from oc login session")
                return oc_token
            else:
                logger.warning("Could not get token from oc CLI. Please ensure you're logged in: oc login")

        # Only create service account for non-OpenShift Kubernetes clusters
        if not self._is_openshift_cluster():
            logger.info("Attempting to create service account token for Kubernetes cluster")
            token = await self._create_local_dev_token()
            if token:
                return token
        else:
            logger.warning("OpenShift cluster detected but no token available. Please run: oc login")

        logger.warning("Could not auto-detect or create auth token. Set explicitly or ensure kubeconfig is available")
        return None

    def _extract_token_from_client(self) -> Optional[str]:
        """Extract bearer token from the existing Kubernetes API client."""
        if not self.k8s_core_api:
            return None
        try:
            config = self.k8s_core_api.api_client.configuration
            api_key = config.api_key.get('authorization')
            if api_key:
                if api_key.lower().startswith('bearer '):
                    logger.info("Using token from existing Kubernetes client")
                    return api_key[7:]
                return api_key

            bearer = config.api_key.get('BearerToken')
            if bearer:
                logger.info("Using BearerToken from existing Kubernetes client")
                return bearer
        except Exception as e:
            logger.debug(f"Could not extract token from existing client: {e}")
        return None

    def _is_openshift_cluster(self) -> bool:
        """
        Check if the current cluster is an OpenShift cluster.

        Returns:
            True if OpenShift cluster, False otherwise
        """
        try:
            # Try to list OpenShift-specific API resources (routes)
            self.k8s_core_api.api_client.call_api(
                '/apis/route.openshift.io/v1',
                'GET',
                response_type=object,
                _preload_content=False
            )
            logger.debug("Detected OpenShift cluster (route.openshift.io API available)")
            return True
        except Exception as e:
            logger.debug(f"Not an OpenShift cluster (route.openshift.io API not available): {e}")
            return False

    async def _get_openshift_token(self) -> Optional[str]:
        """
        Get the current user's token from OpenShift CLI (oc whoami -t).

        This retrieves the token from the user's active oc login session,
        avoiding the need to create service accounts for local development.

        Returns:
            Bearer token from oc CLI or None if not available
        """
        try:
            # Check if oc CLI is available
            logger.debug("Checking if oc CLI is available...")
            result = subprocess.run(
                ['oc', 'version', '--client'],
                capture_output=True,
                timeout=5,
                text=True
            )
            if result.returncode != 0:
                logger.debug(f"oc CLI not available: {result.stderr}")
                return None
            logger.debug("oc CLI is available")

            # Get token using oc whoami -t
            logger.debug("Running: oc whoami -t")
            token_result = subprocess.run(
                ['oc', 'whoami', '-t'],
                capture_output=True,
                timeout=5,
                text=True
            )

            if token_result.returncode == 0:
                token = token_result.stdout.strip()
                if token:
                    logger.info(f"✓ Retrieved token from oc CLI (length: {len(token)} chars)")
                    # Also verify the token works by checking cluster connectivity
                    try:
                        verify_result = subprocess.run(
                            ['oc', 'whoami'],
                            capture_output=True,
                            timeout=5,
                            text=True
                        )
                        if verify_result.returncode == 0:
                            username = verify_result.stdout.strip()
                            logger.info(f"✓ Token verified for user: {username}")
                        else:
                            logger.warning("Token retrieved but verification failed. You may need to re-login: oc login")
                    except:
                        pass  # Verification is optional
                    return token
                else:
                    logger.warning("oc whoami -t returned empty token")
                    return None
            else:
                error = token_result.stderr.strip()
                logger.debug(f"oc whoami -t failed: {error}")
                if 'not logged in' in error.lower() or 'no token' in error.lower():
                    logger.error("=" * 70)
                    logger.error("NOT LOGGED INTO OPENSHIFT")
                    logger.error("=" * 70)
                    logger.error("Please login to OpenShift:")
                    logger.error("  1. Run: oc login <cluster-url>")
                    logger.error("  2. Or: oc login --token=<token> <cluster-url>")
                    logger.error("")
                    logger.error("If you see SSL errors after login, try:")
                    logger.error("  oc logout && oc login <cluster-url>")
                    logger.error("=" * 70)
                return None

        except FileNotFoundError:
            logger.debug("oc CLI not found in PATH")
            return None
        except subprocess.TimeoutExpired:
            logger.warning("oc CLI command timed out")
            return None
        except Exception as e:
            logger.debug(f"Error getting OpenShift token: {e}")
            return None

    async def _create_local_dev_token(self) -> Optional[str]:
        """
        Create a short-lived service account token for local development on Kubernetes clusters.

        NOTE: This method is ONLY used for vanilla Kubernetes clusters.
        For OpenShift clusters, the user's oc login token is used instead (via oc whoami -t).

        This method:
        1. Checks if we're running locally (not in cluster)
        2. Checks if this is an OpenShift cluster (if yes, returns None - use oc login instead)
        3. Checks if a kubearchive-view service account exists in default namespace
        4. Creates the service account if needed
        5. Creates ClusterRole with permissions for:
           - Pods, services, secrets, configmaps (core resources)
           - Tekton resources (PipelineRuns, TaskRuns)
           - OpenShift Routes (for endpoint discovery)
           - Kubernetes Ingresses (for endpoint discovery)
           - AppStudio resources (Releases, Snapshots)
        6. Creates ClusterRoleBinding to grant cluster-wide access
        7. Generates a short-lived token (1 hour) using kubectl create token

        Returns:
            Bearer token or None if creation failed (or if OpenShift cluster)
        """
        # Only try this for local development (not in-cluster)
        if os.path.exists('/var/run/secrets/kubernetes.io/serviceaccount/token'):
            logger.debug("Running in-cluster, skipping local dev token creation")
            return None

        # Don't create service account for OpenShift - user should use oc login token
        if self._is_openshift_cluster():
            logger.debug("OpenShift cluster detected, skipping service account creation")
            logger.debug("For OpenShift, please use: oc login <cluster-url>")
            return None

        try:
            # Check if kubectl is available
            logger.debug("Checking if kubectl is available...")
            result = subprocess.run(
                ['kubectl', 'version', '--client'],
                capture_output=True,
                timeout=5,
                text=True
            )
            if result.returncode != 0:
                logger.debug(f"kubectl not available: {result.stderr}")
                return None
            logger.debug("kubectl is available")

            logger.info("Attempting to create local development token")

            # Define service account name and namespace
            sa_name = 'kubearchive-view'
            sa_namespace = 'default'

            # Check if service account exists and create if needed
            logger.debug(f"Checking if service account {sa_name} exists...")
            check_sa = subprocess.run(
                ['kubectl', 'get', 'serviceaccount', sa_name, '-n', sa_namespace],
                capture_output=True,
                timeout=5,
                text=True
            )

            if check_sa.returncode != 0:
                logger.info(f"Service account {sa_name} not found in namespace {sa_namespace}, creating...")
                logger.info(f"This service account will be granted cluster-wide permissions to access KubeArchive")

                # Create service account
                logger.debug(f"Running: kubectl create serviceaccount {sa_name} --namespace {sa_namespace}")
                create_sa = subprocess.run(
                    ['kubectl', 'create', 'serviceaccount', sa_name, '--namespace', sa_namespace],
                    capture_output=True,
                    timeout=10,
                    text=True
                )

                if create_sa.returncode != 0:
                    logger.warning(f"Failed to create service account: {create_sa.stderr}")
                    return None
                else:
                    logger.info(f"✓ Created service account {sa_name} in namespace {sa_namespace}")
            else:
                logger.debug(f"Service account {sa_name} already exists in namespace {sa_namespace}")

            # Always ensure ClusterRole and ClusterRoleBinding exist (idempotent)
            # Create ClusterRole for accessing KubeArchive API and related resources
            logger.debug(f"Ensuring ClusterRole kubearchive-client exists...")

            # Use kubectl to create ClusterRole with proper permissions
            clusterrole_yaml = """
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kubearchive-client
rules:
- apiGroups: [""]
  resources: ["pods", "pods/log", "services", "secrets", "configmaps"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["tekton.dev"]
  resources: ["pipelineruns", "taskruns", "pipelineruns/status", "taskruns/status"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["route.openshift.io"]
  resources: ["routes"]
  verbs: ["get", "list"]
- apiGroups: ["networking.k8s.io"]
  resources: ["ingresses"]
  verbs: ["get", "list"]
- apiGroups: ["appstudio.redhat.com"]
  resources: ["releases", "snapshots"]
  verbs: ["get", "list", "watch"]
"""

            # Write YAML to temp file and apply
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(clusterrole_yaml)
                temp_file = f.name

            try:
                create_clusterrole = subprocess.run(
                    ['kubectl', 'apply', '-f', temp_file],
                    capture_output=True,
                    timeout=10,
                    text=True
                )

                if create_clusterrole.returncode != 0:
                    error_msg = create_clusterrole.stderr
                    if 'forbidden' in error_msg.lower() or 'unauthorized' in error_msg.lower():
                        logger.error(f"Insufficient permissions to create ClusterRole. You need cluster-admin privileges.")
                        logger.error(f"Ask your cluster administrator to create the ClusterRole manually:")
                        logger.error(f"  kubectl apply -f - <<EOF")
                        logger.error(clusterrole_yaml)
                        logger.error(f"  EOF")
                        return None
                    else:
                        logger.warning(f"ClusterRole creation failed: {error_msg}")
                else:
                    logger.info(f"✓ Created/Updated ClusterRole kubearchive-client")
            finally:
                # Clean up temp file
                try:
                    os.unlink(temp_file)
                except:
                    pass

            # Create ClusterRoleBinding
            logger.debug(f"Ensuring ClusterRoleBinding kubearchive-client exists...")
            create_binding = subprocess.run(
                ['kubectl', 'create', 'clusterrolebinding', 'kubearchive-client',
                 f'--serviceaccount={sa_namespace}:{sa_name}',
                 '--clusterrole=kubearchive-client'],
                capture_output=True,
                timeout=10,
                text=True
            )

            if create_binding.returncode != 0 and 'already exists' not in create_binding.stderr.lower():
                error_msg = create_binding.stderr
                if 'forbidden' in error_msg.lower() or 'unauthorized' in error_msg.lower():
                    logger.error(f"Insufficient permissions to create ClusterRoleBinding. You need cluster-admin privileges.")
                    logger.error(f"Ask your cluster administrator to create the ClusterRoleBinding manually:")
                    logger.error(f"  kubectl create clusterrolebinding kubearchive-client \\")
                    logger.error(f"    --serviceaccount={sa_namespace}:{sa_name} \\")
                    logger.error(f"    --clusterrole=kubearchive-client")
                    return None
                else:
                    logger.warning(f"ClusterRoleBinding creation failed: {error_msg}")
            elif create_binding.returncode == 0:
                logger.info(f"✓ Created ClusterRoleBinding kubearchive-client")
            else:
                logger.debug(f"ClusterRoleBinding kubearchive-client already exists")

            # Create a short-lived token (default: 1 hour)
            logger.info(f"Generating short-lived token for {sa_name}...")
            logger.debug(f"Running: kubectl create token {sa_name} --namespace {sa_namespace} --duration=1h")
            create_token = subprocess.run(
                ['kubectl', 'create', 'token', sa_name, '--namespace', sa_namespace, '--duration=1h'],
                capture_output=True,
                timeout=10,
                text=True
            )

            if create_token.returncode == 0:
                token = create_token.stdout.strip()
                if token:
                    logger.info(f"✓ Created local development token (valid for 1 hour, length: {len(token)} chars)")
                    return token
                else:
                    logger.warning("Token command succeeded but returned empty token")
                    return None
            else:
                error = create_token.stderr
                logger.warning(f"Failed to create token (exit code {create_token.returncode}): {error}")
                return None

        except FileNotFoundError:
            logger.debug("kubectl not found, cannot create local dev token")
            return None
        except subprocess.TimeoutExpired:
            logger.warning("Token creation timed out")
            return None
        except Exception as e:
            logger.debug(f"Error creating local dev token: {e}")
            return None

    async def _get_ssl_context(self):
        """
        Get or create SSL context with KubeArchive CA certificate.

        Returns SSL context from:
        1. Cached SSL context (if already created)
        2. False for localhost/port-forward (ssl=False, --insecure)
        3. System CA bundle for remote OpenShift routes (public certificates)
        4. KubeArchive CA certificate from TLS secrets (self-signed certs)
        5. False (disables SSL verification as fallback)

        Searches for secrets:
        - kubearchive-ca (standard deployment)
        - kubearchive-api-server-tls (OpenShift Konflux)

        Returns:
            SSL context for TLS verification, or False to disable verification
        """
        # Return cached SSL context if available
        if self._ssl_context is not None:
            return self._ssl_context

        # Skip SSL verification entirely when using automatic port-forward
        if hasattr(self.endpoint_discovery, '_port_forward_process') and \
           self.endpoint_discovery._port_forward_process:
            logger.info("Using automatic port-forward - disabling SSL verification (safe for local development)")
            logger.debug("Traffic is encrypted by kubectl port-forward tunnel")
            self._ssl_context = False
            return False

        # Check if connecting to localhost
        endpoint = await self.endpoint_discovery.discover_endpoint()
        if endpoint:
            import urllib.parse
            parsed = urllib.parse.urlparse(endpoint)
            hostname = parsed.hostname or ''
            if hostname.lower() in ('localhost', '127.0.0.1', '::1'):
                logger.info(f"Connecting to localhost ({hostname}) - disabling SSL verification")
                self._ssl_context = False
                return False

            # For OpenShift routes with public domains, use system CA bundle
            # These are typically signed by public CAs (Let's Encrypt, DigiCert, etc.)
            if '.apps.' in hostname.lower() or hostname.endswith('.openshiftapps.com'):
                logger.info(f"Detected OpenShift route with public certificate: {hostname}")
                logger.info("Using system CA bundle for SSL verification")
                ssl_context = ssl.create_default_context()
                self._ssl_context = ssl_context
                return ssl_context

        # Try to get CA certificate from TLS secrets for self-signed certificates
        if not self.k8s_core_api:
            logger.debug("CoreV1Api not available, cannot fetch CA certificate")
            logger.warning("Falling back to insecure SSL (certificate verification disabled)")
            self._ssl_context = False
            return False

        try:
            # Build list of namespaces to search
            # Include the discovered namespace from endpoint discovery
            namespaces_to_search = []

            # Add the discovered namespace first (highest priority)
            if hasattr(self.endpoint_discovery, '_discovered_namespace') and \
               self.endpoint_discovery._discovered_namespace:
                namespaces_to_search.append(self.endpoint_discovery._discovered_namespace)

            # Add common namespaces
            for ns in self._ca_namespaces:
                if ns not in namespaces_to_search:
                    namespaces_to_search.append(ns)

            # Try to find TLS secret in namespaces
            for namespace in namespaces_to_search:
                for secret_name in self._ca_secret_names:
                    try:
                        secret = self.k8s_core_api.read_namespaced_secret(
                            name=secret_name,
                            namespace=namespace
                        )

                        # Try multiple certificate keys (different secret formats)
                        cert_data = None
                        cert_key = None

                        if secret.data:
                            # Try ca.crt first (standard CA cert)
                            if 'ca.crt' in secret.data:
                                cert_data = secret.data['ca.crt']
                                cert_key = 'ca.crt'
                            # Try tls.crt (server cert, can be used for verification)
                            elif 'tls.crt' in secret.data:
                                cert_data = secret.data['tls.crt']
                                cert_key = 'tls.crt'
                            # Try ca-bundle.crt (some deployments use this)
                            elif 'ca-bundle.crt' in secret.data:
                                cert_data = secret.data['ca-bundle.crt']
                                cert_key = 'ca-bundle.crt'

                        if cert_data:
                            ca_cert = base64.b64decode(cert_data).decode('utf-8')

                            # Write CA cert to temporary file
                            # We need to keep this file around for the lifetime of the client
                            if not self._ca_cert_path:
                                # Create a named temporary file that we don't delete
                                with tempfile.NamedTemporaryFile(mode='w', suffix='.crt', delete=False) as f:
                                    f.write(ca_cert)
                                    self._ca_cert_path = f.name

                            # Create SSL context with the CA certificate
                            ssl_context = ssl.create_default_context(cafile=self._ca_cert_path)

                            # Check if we need to disable hostname verification
                            # This is needed when:
                            # 1. Using automatic port-forward (_port_forward_process is set)
                            # 2. Connecting via localhost (manual port-forward or KUBEARCHIVE_HOST=localhost)
                            # 3. Connecting via 127.0.0.1
                            disable_hostname_check = False

                            # Check for automatic port-forward
                            if hasattr(self.endpoint_discovery, '_port_forward_process') and \
                               self.endpoint_discovery._port_forward_process:
                                disable_hostname_check = True
                                logger.debug("Detected automatic port-forward")

                            # Check if endpoint is localhost or 127.0.0.1
                            try:
                                endpoint = await self.endpoint_discovery.discover_endpoint()
                                if endpoint:
                                    import urllib.parse
                                    parsed = urllib.parse.urlparse(endpoint)
                                    hostname = parsed.hostname or ''
                                    if hostname.lower() in ('localhost', '127.0.0.1', '::1'):
                                        disable_hostname_check = True
                                        logger.debug(f"Detected localhost endpoint: {hostname}")
                            except:
                                pass  # If we can't get endpoint, continue with current setting

                            if disable_hostname_check:
                                ssl_context.check_hostname = False
                                logger.info("Disabled hostname verification for localhost/port-forward connection")
                                logger.debug("Certificate verification is still active via CA certificate")

                            self._ssl_context = ssl_context

                            logger.info(f"Created SSL context with certificate from {namespace}/{secret_name}[{cert_key}]")
                            return ssl_context

                    except ApiException as e:
                        if e.status == 404:
                            continue  # Try next secret/namespace
                        logger.debug(f"Error reading {secret_name} secret in {namespace}: {e}")
                        continue

            logger.warning(f"TLS secrets not found. Searched for {self._ca_secret_names} in namespaces: {namespaces_to_search}")
            logger.warning("Falling back to insecure SSL (certificate verification disabled)")
            self._ssl_context = False
            return False

        except Exception as e:
            logger.warning(f"Error creating SSL context from TLS secrets: {e}")
            logger.warning("Falling back to insecure SSL (certificate verification disabled)")
            self._ssl_context = False
            return False

    def __del__(self):
        """Cleanup: remove temporary CA certificate file."""
        if self._ca_cert_path and os.path.exists(self._ca_cert_path):
            try:
                os.unlink(self._ca_cert_path)
            except Exception as e:
                logger.debug(f"Error removing temporary CA cert file: {e}")

    async def query_resources(
        self,
        resource_type: str,
        namespace: str,
        name: Optional[str] = None,
        label_selector: Optional[str] = None,
        creation_timestamp_after: Optional[str] = None,
        creation_timestamp_before: Optional[str] = None,
        limit: int = 100,
        continue_token: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Query archived resources from KubeArchive.

        Args:
            resource_type: Resource type (e.g., 'pipelinerun', 'taskrun', 'pod')
            namespace: Kubernetes namespace
            name: Optional resource name (supports wildcards: *, case-insensitive)
            label_selector: Kubernetes-style label selector
            creation_timestamp_after: RFC3339 timestamp for lower bound
            creation_timestamp_before: RFC3339 timestamp for upper bound
            limit: Maximum number of results (default: 100, max: 1000)
            continue_token: Pagination continuation token

        Returns:
            Dictionary containing query results and metadata
        """
        endpoint = await self.endpoint_discovery.discover_endpoint()
        if not endpoint:
            return {
                'status': 'error',
                'message': 'KubeArchive endpoint not available. Set KUBEARCHIVE_HOST or deploy kubearchive-api-server'
            }

        # Build API URL based on resource type
        url = self._build_resource_url(endpoint, resource_type, namespace, name)

        # Build query parameters
        params = self._build_query_params(
            label_selector=label_selector,
            creation_timestamp_after=creation_timestamp_after,
            creation_timestamp_before=creation_timestamp_before,
            limit=limit,
            continue_token=continue_token,
            name_query=name if name and '*' in name else None  # Only use name in query if wildcard
        )

        # Get auth token
        auth_token = await self._get_auth_token()
        if not auth_token:
            return {
                'status': 'error',
                'message': 'Authentication token not available'
            }

        # Make API request
        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip'
        }

        # Get SSL context for TLS verification
        ssl_context = await self._get_ssl_context()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params, ssl=ssl_context) as response:
                    if response.status == 200:
                        # Check content type
                        content_type = response.headers.get('Content-Type', '')

                        if 'application/json' in content_type:
                            data = await response.json()
                        elif 'text/plain' in content_type:
                            # Handle text/plain responses (empty results or error messages)
                            text = await response.text()
                            logger.info(f"KubeArchive returned text/plain: {text[:200]}")

                            # Try to parse as JSON anyway
                            try:
                                import json
                                data = json.loads(text)
                            except:
                                # If not JSON, treat as empty result
                                logger.info("Text response is not JSON, treating as empty result")
                                data = {'items': [], 'kind': 'List', 'apiVersion': 'v1', 'metadata': {}}
                        else:
                            # Fallback: try JSON first, then text
                            try:
                                data = await response.json()
                            except:
                                text = await response.text()
                                logger.warning(f"Unexpected content type: {content_type}, got: {text[:200]}")
                                data = {'items': []}

                        return {
                            'status': 'success',
                            'data': data,
                            'source': 'kubearchive'
                        }
                    elif response.status == 404:
                        return {
                            'status': 'success',
                            'data': {'items': []},
                            'message': 'No archived resources found',
                            'source': 'kubearchive'
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"KubeArchive API error {response.status}: {error_text}")
                        return {
                            'status': 'error',
                            'message': f'KubeArchive API returned status {response.status}: {error_text}'
                        }

        except aiohttp.ClientError as e:
            logger.error(f"Error connecting to KubeArchive API: {e}")
            # Clear cached endpoint on connection error
            self.endpoint_discovery.clear_cache()
            return {
                'status': 'error',
                'message': f'Error connecting to KubeArchive: {str(e)}'
            }
        except Exception as e:
            logger.error(f"Unexpected error querying KubeArchive: {e}")
            return {
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            }

    async def get_resource_logs(
        self,
        resource_type: str,
        namespace: str,
        name: str,
        container: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Retrieve container logs for an archived resource.

        The KubeArchive API traverses owner references to gather all logs
        associated with the given resource. Logs can be queried for any
        resource, though most resources will not have logs. The most common
        resources with logs are:
        - Pods: Direct container logs
        - TaskRuns: Logs from all pods created by the TaskRun
        - PipelineRuns: Logs from all TaskRuns and their pods

        Args:
            resource_type: Resource type (pod, taskrun, pipelinerun, etc.)
            namespace: Kubernetes namespace
            name: Resource name
            container: Optional container name filter (for multi-container pods)

        Returns:
            Dictionary containing log content with keys:
            - status: 'success' or 'error'
            - logs: Log text (if successful)
            - message: Error or status message
            - source: 'kubearchive'
        """
        endpoint = await self.endpoint_discovery.discover_endpoint()
        if not endpoint:
            return {
                'status': 'error',
                'message': 'KubeArchive endpoint not available'
            }

        # Build log URL
        url = self._build_log_url(endpoint, resource_type, namespace, name)
        logger.debug(f"Requesting logs from: {url}")

        # Build query parameters
        params = {}
        if container:
            params['container'] = container

        # Get auth token
        auth_token = await self._get_auth_token()
        if not auth_token:
            logger.error("Failed to get authentication token for log retrieval")
            return {
                'status': 'error',
                'message': 'Authentication token not available'
            }

        headers = {
            'Authorization': f'Bearer {auth_token}',
            'Accept': 'text/plain',
            'Accept-Encoding': 'gzip'
        }

        # Get SSL context for TLS verification
        ssl_context = await self._get_ssl_context()

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params, ssl=ssl_context) as response:
                    if response.status == 200:
                        logs = await response.text()
                        logger.info(f"Successfully retrieved {len(logs)/1.3} tokens of logs for {resource_type}/{name}")
                        return {
                            'status': 'success',
                            'logs': logs,
                            'source': 'kubearchive'
                        }
                    elif response.status == 404:
                        logger.info(f"No logs found for {resource_type}/{name} (404)")
                        return {
                            'status': 'success',
                            'logs': '',
                            'message': 'No logs found for archived resource',
                            'source': 'kubearchive'
                        }
                    else:
                        error_text = await response.text()
                        logger.error(f"KubeArchive API error {response.status} fetching logs for {resource_type}/{name}: {error_text}")
                        return {
                            'status': 'error',
                            'message': f'KubeArchive API returned status {response.status}: {error_text}'
                        }

        except aiohttp.ClientError as e:
            logger.error(f"Error retrieving logs from KubeArchive: {e}")
            return {
                'status': 'error',
                'message': f'Error connecting to KubeArchive: {str(e)}'
            }
        except Exception as e:
            logger.error(f"Unexpected error retrieving logs: {e}")
            return {
                'status': 'error',
                'message': f'Unexpected error: {str(e)}'
            }

    def _build_resource_url(self, endpoint: str, resource_type: str, namespace: str, name: Optional[str] = None) -> str:
        """Build KubeArchive API URL for resource queries."""
        # Map common resource types to their API paths
        resource_api_map = {
            'pipelinerun': ('apis/tekton.dev/v1', 'pipelineruns'),
            'taskrun': ('apis/tekton.dev/v1', 'taskruns'),
            'pod': ('api/v1', 'pods'),
            'deployment': ('apis/apps/v1', 'deployments'),
            'statefulset': ('apis/apps/v1', 'statefulsets'),
            'daemonset': ('apis/apps/v1', 'daemonsets'),
            'replicaset': ('apis/apps/v1', 'replicasets'),
            'service': ('api/v1', 'services'),
            'configmap': ('api/v1', 'configmaps'),
            'secret': ('api/v1', 'secrets'),
            # Add more resource types as needed
        }

        resource_lower = resource_type.lower()
        if resource_lower not in resource_api_map:
            # Generic fallback - assume it's in core API
            api_path = 'api/v1'
            plural = resource_type.lower() + 's'
        else:
            api_path, plural = resource_api_map[resource_lower]

        # Ensure endpoint doesn't have trailing slash
        endpoint = endpoint.rstrip('/')

        # Build URL
        # Format: /apis/:group/:version/namespaces/:namespace/:resourceType[/:name]
        url = f"{endpoint}/{api_path}/namespaces/{namespace}/{plural}"

        # Add name to path only if it's an exact match (no wildcards)
        if name and '*' not in name:
            url = f"{url}/{name}"

        return url

    def _build_log_url(self, endpoint: str, resource_type: str, namespace: str, name: str) -> str:
        """
        Build KubeArchive API URL for log retrieval.

        Supports logs for any resource type. The API traverses owner references
        to gather all logs associated with the given resource.

        API endpoints:
        - Core resources: /:version/namespaces/:namespace/:resourceType/:name/log
        - Non-core resources: /:group/:version/namespaces/:namespace/:resourceType/:name/log

        Args:
            endpoint: KubeArchive API endpoint
            resource_type: Resource type (pod, taskrun, pipelinerun, etc.)
            namespace: Kubernetes namespace
            name: Resource name

        Returns:
            Full URL for log retrieval
        """
        # Map common resource types to their API paths
        resource_api_map = {
            'pipelinerun': ('apis/tekton.dev/v1', 'pipelineruns'),
            'taskrun': ('apis/tekton.dev/v1', 'taskruns'),
            'pod': ('api/v1', 'pods'),
            'deployment': ('apis/apps/v1', 'deployments'),
            'statefulset': ('apis/apps/v1', 'statefulsets'),
            'daemonset': ('apis/apps/v1', 'daemonsets'),
            'replicaset': ('apis/apps/v1', 'replicasets'),
            'service': ('api/v1', 'services'),
            'configmap': ('api/v1', 'configmaps'),
            'secret': ('api/v1', 'secrets'),
        }

        resource_lower = resource_type.lower()
        if resource_lower not in resource_api_map:
            # Generic fallback - assume it's in core API
            api_path = 'api/v1'
            plural = resource_type.lower() + 's'
        else:
            api_path, plural = resource_api_map[resource_lower]

        # Ensure endpoint doesn't have trailing slash
        endpoint = endpoint.rstrip('/')

        # Build log URL according to KubeArchive API spec
        # Format: /:group/:version/namespaces/:namespace/:resourceType/:name/log
        # Example: https://localhost:3100/apis/tekton.dev/v1/namespaces/my-ns/pipelineruns/my-pr/log
        url = f"{endpoint}/{api_path}/namespaces/{namespace}/{plural}/{name}/log"
        return url

    def _build_query_params(
        self,
        label_selector: Optional[str] = None,
        creation_timestamp_after: Optional[str] = None,
        creation_timestamp_before: Optional[str] = None,
        limit: int = 100,
        continue_token: Optional[str] = None,
        name_query: Optional[str] = None
    ) -> Dict[str, Any]:
        """Build query parameters for KubeArchive API request."""
        params = {}

        if label_selector:
            params['labelSelector'] = label_selector

        if creation_timestamp_after:
            params['creationTimestampAfter'] = creation_timestamp_after

        if creation_timestamp_before:
            params['creationTimestampBefore'] = creation_timestamp_before

        if limit:
            # Ensure limit is within bounds (max 1000)
            params['limit'] = min(limit, 1000)

        if continue_token:
            params['continue'] = continue_token

        if name_query:
            params['name'] = name_query

        return params


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _derive_phase_from_conditions(conditions: List[Dict[str, Any]]) -> str:
    """
    Derive execution phase from Tekton resource conditions.
    
    Tekton PipelineRuns and TaskRuns use conditions (specifically the 'Succeeded' 
    condition) to indicate their execution state, rather than a 'phase' field.
    
    Args:
        conditions: List of Kubernetes condition objects from resource status
        
    Returns:
        Phase string: "Succeeded", "Failed", "Running", "Pending", or "Unknown"
    """
    if not conditions:
        return "Unknown"
    
    for condition in conditions:
        if condition.get('type') == 'Succeeded':
            status_value = condition.get('status')
            reason = condition.get('reason', '')
            
            if status_value == 'True':
                return 'Succeeded'
            elif status_value == 'False':
                return 'Failed'
            elif status_value == 'Unknown':
                # Check reason for more specific state
                if reason in ('Running', 'Started', 'Pending'):
                    return reason
                return 'Running'
    
    # No Succeeded condition found - check if resource has started
    # This handles edge cases where conditions are incomplete
    return "Pending"

async def check_kubearchive_availability(endpoint_discovery: KubeArchiveEndpointDiscovery) -> Dict[str, Any]:
    """
    Check if KubeArchive is available and accessible.

    Args:
        endpoint_discovery: KubeArchiveEndpointDiscovery instance

    Returns:
        Dictionary with availability status
    """
    endpoint = await endpoint_discovery.discover_endpoint()
    if not endpoint:
        return {
            'available': False,
            'message': 'KubeArchive endpoint not discovered'
        }

    # Try to access /livez endpoint
    try:
        # Create a temporary client to get proper SSL context
        # This ensures we use ssl=False (--insecure) for localhost/port-forward
        # and proper SSL verification for remote connections
        temp_client = KubeArchiveClient(endpoint_discovery)
        ssl_context = await temp_client._get_ssl_context()

        async with aiohttp.ClientSession() as session:
            async with session.get(f"{endpoint}/livez", ssl=ssl_context, timeout=aiohttp.ClientTimeout(total=5)) as response:
                if response.status == 200:
                    return {
                        'available': True,
                        'endpoint': endpoint,
                        'message': 'KubeArchive is available'
                    }
                else:
                    return {
                        'available': False,
                        'endpoint': endpoint,
                        'message': f'KubeArchive returned status {response.status}'
                    }
    except Exception as e:
        return {
            'available': False,
            'endpoint': endpoint,
            'message': f'Error connecting to KubeArchive: {str(e)}'
        }

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def normalize_to_rfc3339(value: str) -> str:
    """
    Normalize a timestamp string to RFC3339 UTC format with Z suffix.
    
    This function handles various ISO-like date/datetime formats and converts
    them to the RFC3339 format required by KubeArchive API.
    
    Args:
        value: User-provided timestamp string. Supported formats:
            - RFC3339: "2024-01-15T10:30:00Z"
            - ISO datetime: "2024-01-15T10:30:00"
            - ISO datetime with offset: "2024-01-15T10:30:00+02:00"
            - ISO date: "2024-01-15"
            - Relaxed date: "2024-1-5"
            
    Returns:
        RFC3339 UTC timestamp string with Z suffix (e.g., "2024-01-15T10:30:00Z")
        
    Raises:
        ValueError: If the input cannot be parsed as a valid timestamp
        
    Examples:
        >>> normalize_to_rfc3339("2024-01-15T10:30:00Z")
        '2024-01-15T10:30:00Z'
        >>> normalize_to_rfc3339("2024-01-15")
        '2024-01-15T00:00:00Z'
        >>> normalize_to_rfc3339("2024-1-5")
        '2024-01-05T00:00:00Z'
        >>> normalize_to_rfc3339("2024-01-15T10:30:00+02:00")
        '2024-01-15T08:30:00Z'
    """
    if not value or not isinstance(value, str):
        raise ValueError(f"Invalid timestamp: expected non-empty string, got {type(value).__name__}")
    
    value = value.strip()
    if not value:
        raise ValueError("Invalid timestamp: empty string")
    
    dt = None
    
    # Try parsing as ISO format with Z suffix (RFC3339 UTC)
    if value.endswith('Z'):
        try:
            dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            pass
    
    # Try parsing as ISO format with timezone offset
    if dt is None and ('+' in value[10:] or value[10:].count('-') > 0):
        try:
            # Handle timezone offset like +02:00 or -05:00
            dt = datetime.fromisoformat(value)
        except ValueError:
            pass
    
    # Try parsing as ISO datetime without timezone (assume UTC)
    if dt is None and 'T' in value:
        try:
            dt = datetime.fromisoformat(value)
            # If no timezone info, treat as UTC
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    
    # Try parsing as date only (various formats)
    if dt is None:
        # Match patterns like "2024-01-15", "2024-1-5", "2024-01-5", "2024-1-15"
        date_match = re.match(r'^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$', value)
        if date_match:
            try:
                year, month, day = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
                from datetime import timezone
                dt = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
            except ValueError as e:
                raise ValueError(f"Invalid date values in '{value}': {e}")
    
    if dt is None:
        raise ValueError(
            f"Invalid timestamp format: '{value}'. "
            f"Use RFC3339 format (e.g., '2024-01-15T10:30:00Z') or ISO date (e.g., '2024-01-15')."
        )
    
    # Convert to UTC if timezone-aware
    if dt.tzinfo is not None:
        from datetime import timezone
        dt = dt.astimezone(timezone.utc)
    
    # Format as RFC3339 UTC with Z suffix
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def format_timestamp_for_kubearchive(dt: Union[datetime, str]) -> str:
    """
    Format a timestamp for Kubearchive API queries.
    
    Args:
        dt: Datetime object or ISO format string
        
    Returns:
        RFC3339 formatted timestamp string with Z suffix (UTC)
        
    Raises:
        ValueError: If dt is a string that cannot be parsed as a valid timestamp
    """
    if isinstance(dt, str):
        # Use normalize_to_rfc3339 for string inputs - this will raise ValueError on failure
        return normalize_to_rfc3339(dt)
    
    if isinstance(dt, datetime):
        # Convert datetime to UTC if timezone-aware, then format with Z suffix
        if dt.tzinfo is not None:
            from datetime import timezone
            dt = dt.astimezone(timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    raise ValueError(f"Invalid timestamp type: expected datetime or str, got {type(dt).__name__}")


async def query_kubearchive_resources(
    kubearchive_client: 'KubeArchiveClient',
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
    output_format: str = "summary"
) -> Dict[str, Any]:
    """
    Unified high-level API for querying archived resources from KubeArchive.

    This function provides a convenient wrapper around KubeArchiveClient.query_resources()
    with formatted output and consistent error handling.

    Args:
        kubearchive_client: Initialized KubearchiveClient instance
        resource_type: One of: pipelinerun, taskrun, pod, release, snapshot
        namespace: Kubernetes namespace
        name: Optional specific resource name (supports wildcards)
        label_selector: Optional label selector (e.g., "app=myapp,env=prod")
        field_selector: Optional field selector
        since_time: RFC3339 timestamp for lower bound
        until_time: RFC3339 timestamp for upper bound
        include_logs: Whether to fetch logs (for pods, taskruns, pipelineruns)
        container: Optional container name when fetching pod logs (multi-container pods)
        limit: Maximum number of resources (default: 100, max: 1000)
        output_format: One of: summary, detailed, yaml

    Returns:
        Dictionary containing:
        - resources: List of resources (format depends on output_format)
        - total_count: Number of resources returned
        - time_range: Dict with 'since' and 'until' timestamps
        - kubearchive_status: 'success' or 'error'
        - error: Error message (if kubearchive_status is 'error')

    Example:
        result = await query_kubearchive_resources(
            kubearchive_client=client,
            resource_type='pipelinerun',
            namespace='default',
            since_time='2024-01-01T00:00:00Z',
            limit=50,
            output_format='summary'
        )

        if result['kubearchive_status'] == 'success':
            for resource in result['resources']:
                print(f"  - {resource['name']} ({resource['phase']})")
    """
    try:
        # Validate resource type
        valid_types = ['pipelinerun', 'taskrun', 'pod', 'release', 'snapshot']
        if resource_type.lower() not in valid_types:
            return {
                'kubearchive_status': 'error',
                'error': f"Invalid resource_type '{resource_type}'. Must be one of: {', '.join(valid_types)}",
                'resources': [],
                'total_count': 0,
                'time_range': {'since': since_time, 'until': until_time}
            }

        # Validate output format
        valid_formats = ['summary', 'detailed', 'yaml']
        if output_format not in valid_formats:
            return {
                'kubearchive_status': 'error',
                'error': f"Invalid output_format '{output_format}'. Must be one of: {', '.join(valid_formats)}",
                'resources': [],
                'total_count': 0,
                'time_range': {'since': since_time, 'until': until_time}
            }

        # Validate limit
        if limit < 1 or limit > 1000:
            limit = min(max(1, limit), 1000)
            logger.warning(f"Limit adjusted to {limit} (must be between 1 and 1000)")

        # Query resources using the client
        result = await kubearchive_client.query_resources(
            resource_type=resource_type,
            namespace=namespace,
            name=name,
            label_selector=label_selector,
            creation_timestamp_after=since_time,
            creation_timestamp_before=until_time,
            limit=limit
        )

        # Check if query was successful
        if result.get('status') != 'success':
            return {
                'kubearchive_status': 'error',
                'error': result.get('message', 'Query failed'),
                'resources': [],
                'total_count': 0,
                'time_range': {'since': since_time, 'until': until_time}
            }

        # Extract items from response
        data = result.get('data', {})
        items = data.get('items', [])

        # Format resources based on output_format
        formatted_resources = []

        for item in items:
            if output_format == 'summary':
                formatted_resource = _format_resource_summary(item, resource_type)
            elif output_format == 'detailed':
                formatted_resource = _format_resource_detailed(item, resource_type)
            else:  # yaml
                # Add resource type as a comment at the top of the YAML
                formatted_resource = f"# Resource Type: {resource_type}\n"
                formatted_resource += yaml.dump(item, default_flow_style=False)

            # Fetch logs if requested
            # KubeArchive API supports logs for any resource (traverses owner references)
            # Most commonly used for: pods, taskruns, pipelineruns
            if include_logs and resource_type in ['pod', 'taskrun', 'pipelinerun']:
                resource_name = item.get('metadata', {}).get('name')
                if resource_name:
                    logger.debug(f"Fetching logs for {resource_type}/{resource_name} in namespace {namespace}")
                    logs_result = await kubearchive_client.get_resource_logs(
                        resource_type=resource_type,
                        namespace=namespace,
                        name=resource_name,
                        container=container,
                    )
                    if logs_result.get('status') == 'success':
                        logs_content = logs_result.get('logs', '')
                        if logs_content:
                            logger.debug(f"Successfully fetched logs for {resource_type}/{resource_name} ({len(logs_content)} chars)")
                            if output_format == 'yaml':
                                formatted_resource += f"\n# Logs:\n{logs_content}"
                            else:
                                formatted_resource['logs'] = logs_content
                        else:
                            logger.info(f"No logs available for {resource_type}/{resource_name}")
                            if output_format != 'yaml':
                                formatted_resource['logs'] = ''
                                formatted_resource['logs_message'] = 'No logs available'
                    else:
                        # Log fetch failed - include error message
                        error_msg = logs_result.get('message', 'Unknown error fetching logs')
                        logger.warning(f"Failed to fetch logs for {resource_type}/{resource_name}: {error_msg}")
                        if output_format == 'yaml':
                            formatted_resource += f"\n# Logs Error: {error_msg}\n"
                        else:
                            formatted_resource['logs'] = ''
                            formatted_resource['logs_error'] = error_msg

            formatted_resources.append(formatted_resource)

        return {
            'kubearchive_status': 'success',
            'resources': formatted_resources,
            'total_count': len(formatted_resources),
            'time_range': {
                'since': since_time,
                'until': until_time
            }
        }

    except Exception as e:
        logger.error(f"Error in query_kubearchive_resources: {e}", exc_info=True)
        return {
            'kubearchive_status': 'error',
            'error': f"Unexpected error: {str(e)}",
            'resources': [],
            'total_count': 0,
            'time_range': {'since': since_time, 'until': until_time}
        }


def _format_resource_summary(item: Dict[str, Any], resource_type: str) -> Dict[str, Any]:
    """Format resource as summary (compact view)."""
    metadata = item.get('metadata', {})
    status = item.get('status', {})

    summary = {
        'type': resource_type,
        'name': metadata.get('name', 'unknown'),
        'namespace': metadata.get('namespace', 'unknown'),
        'creation_timestamp': metadata.get('creationTimestamp', 'unknown')
    }

    # Add phase/status based on resource type
    if resource_type in ['pipelinerun', 'taskrun']:
        # Try to get phase from status conditions
        conditions = status.get('conditions', [])
        summary['phase'] = _derive_phase_from_conditions(conditions)
    elif resource_type == 'pod':
        summary['phase'] = status.get('phase', 'Unknown')

    return summary


def _format_resource_detailed(item: Dict[str, Any], resource_type: str) -> Dict[str, Any]:
    """Format resource with full details."""
    metadata = item.get('metadata', {})
    status = item.get('status', {})
    spec = item.get('spec', {})

    detailed = {
        'type': resource_type,
        'name': metadata.get('name', 'unknown'),
        'namespace': metadata.get('namespace', 'unknown'),
        'creation_timestamp': metadata.get('creationTimestamp', 'unknown'),
        'metadata': metadata,
        'status': status,
        'spec': spec
    }

    # Add resource-specific fields
    if resource_type in ['pipelinerun', 'taskrun']:
        # Extract Tekton-specific timing info
        detailed['start_time'] = status.get('startTime')
        detailed['completion_time'] = status.get('completionTime')
        conditions = status.get('conditions', [])
        detailed['phase'] = _derive_phase_from_conditions(conditions)
    elif resource_type == 'pod':
        detailed['phase'] = status.get('phase', 'Unknown')
        detailed['start_time'] = status.get('startTime')

    # Add labels and annotations
    detailed['labels'] = metadata.get('labels', {})
    detailed['annotations'] = metadata.get('annotations', {})

    return detailed


def validate_kubearchive_connection(base_url: str, auth_token: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate connection to Kubearchive API.

    NOTE: This function is deprecated and uses an old API.
    Use check_kubearchive_availability() instead.

    Args:
        base_url: Base URL of Kubearchive API
        auth_token: Optional authentication token

    Returns:
        Dictionary with validation results
    """
    logger.warning("validate_kubearchive_connection() is deprecated. Use check_kubearchive_availability() instead.")

    try:
        # This function is kept for backward compatibility but may not work
        # with the new KubeArchiveClient API
        return {
            'status': 'error',
            'connected': False,
            'base_url': base_url,
            'error': 'This function is deprecated. Use check_kubearchive_availability() instead.',
            'message': 'Function deprecated'
        }

    except Exception as e:
        logger.error(f"Failed to validate Kubearchive connection: {e}")
        return {
            'status': 'error',
            'connected': False,
            'base_url': base_url,
            'error': str(e),
            'message': 'Failed to connect to Kubearchive API'
        }

