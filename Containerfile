# LUMINO MCP Server Container
# Build: podman build -t quay.io/geored/lumino-mcp-server .
# Run:   podman run -it --rm -p 8000:8000 quay.io/geored/lumino-mcp-server

# Red Hat Universal Base Image (UBI) with Python 3.11
FROM registry.access.redhat.com/ubi9/ubi:latest

# Metadata labels following OCI and Red Hat best practices
LABEL org.opencontainers.image.title="LUMINO MCP Server" \
      org.opencontainers.image.description="MCP Server for Kubernetes, OpenShift, and Tekton monitoring and analysis" \
      org.opencontainers.image.source="https://github.com/spre-sre/lumino-mcp-server" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="LUMINO Project" \
      org.opencontainers.image.version="0.1.0" \
      io.k8s.description="LUMINO MCP Server for Kubernetes SRE operations" \
      io.k8s.display-name="LUMINO MCP Server" \
      io.openshift.tags="sre,monitoring,kubernetes,tekton,mcp"

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PATH="/opt/app-root/bin:$PATH" \
    PYTHONPATH="/opt/app-root/src"

# Install Python 3.11 and required tools
RUN dnf update -y && \
    dnf install -y --allowerasing \
        python3.11 \
        python3.11-pip \
        python3.11-devel \
        git \
        curl \
        procps-ng \
        && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Create symlinks for python and pip to use Python 3.11
RUN alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    alternatives --install /usr/bin/pip3 pip3 /usr/bin/pip3.11 1 && \
    alternatives --install /usr/bin/pip pip /usr/bin/pip3.11 1

# Create non-root user and application directory
RUN useradd --uid 1001 --gid 0 --shell /bin/bash --create-home lumino && \
    mkdir -p /opt/app-root/src && \
    chown -R 1001:0 /opt/app-root && \
    chmod -R g+rwX /opt/app-root

# Set working directory
WORKDIR /opt/app-root/src

# Copy dependency files
COPY --chown=1001:0 pyproject.toml ./

# Install uv and dependencies as root (required for system packages)
RUN python -m pip install --no-cache-dir --upgrade pip && \
    python -m pip install --no-cache-dir uv

# Install Python dependencies to system
RUN python -m uv pip install --system --no-cache-dir -r pyproject.toml

# Switch to non-root user after package installation
USER 1001

# Copy source code with proper ownership
COPY --chown=1001:0 src/ ./src/
COPY --chown=1001:0 main.py ./

# Copy help file for container documentation
COPY --chown=1001:0 docs/help.1 /help.1

# Create directories for runtime data
RUN mkdir -p /opt/app-root/src/.cache /opt/app-root/src/logs && \
    chmod -R g+rwX /opt/app-root/src

# Expose port for MCP server (HTTP streaming transport)
EXPOSE 8000

# Health check - verify MCP server is listening
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import socket; s = socket.socket(socket.AF_INET, socket.SOCK_STREAM); s.settimeout(5); s.connect(('localhost', 8000)); s.close()" || exit 1

# Default command - run the MCP server
CMD ["python", "main.py"]
