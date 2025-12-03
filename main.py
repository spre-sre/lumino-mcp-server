#!/usr/bin/env python3
"""
LUMINO MCP Server - Main Entry Point

This module serves as the entry point for the LUMINO MCP (Model Context Protocol) server.
It imports and runs the MCP server with proper configuration for both local and
Kubernetes environments.
"""

import os
import sys
import logging
import importlib.util
import asyncio
from pathlib import Path

# Add the src directory to the Python path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger("lumino-mcp-main")


def main():
    """Main entry point for the LUMINO MCP Server."""
    logger.info("Starting LUMINO MCP Server...")

    try:
        # Import the MCP server module (with hyphen in filename)
        spec = importlib.util.spec_from_file_location("server_mcp", src_path / "server-mcp.py")
        server_mcp = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(server_mcp)

        # Get the MCP server instance
        mcp = server_mcp.mcp

        # Check if running in Kubernetes (via environment variable)
        if os.getenv('KUBERNETES_NAMESPACE') or os.getenv('K8S_NAMESPACE'):
            logger.info("Detected Kubernetes environment - running streamable HTTP server")
            logger.info("Note: Server will bind to 127.0.0.1:8000 (limitation of MCP SDK 1.10.1)")
            logger.info("Using modified health checks to work with localhost binding")

            # Use the standard MCP run method with streamable-http transport
            mcp.run(transport='streamable-http')

        else:
            logger.info("Running in local environment - using stdio transport")
            mcp.run()

        logger.info("MCP server finished successfully")

    except Exception as e:
        logger.error(f"Failed to start LUMINO MCP Server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
