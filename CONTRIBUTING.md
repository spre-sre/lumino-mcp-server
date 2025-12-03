# Contributing to LUMINO MCP Server

Thank you for your interest in contributing to LUMINO MCP Server! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Creating New MCP Tools](#creating-new-mcp-tools)
- [Code Style](#code-style)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)
- [Releasing](#releasing)
- [Reporting Issues](#reporting-issues)

## Code of Conduct

This project follows our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the maintainers.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/lumino-mcp-server.git
   cd lumino-mcp-server
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/spre-sre/lumino-mcp-server.git
   ```

## Development Setup

### Prerequisites

- Python 3.10 or higher
- [uv](https://docs.astral.sh/uv/) package manager (recommended)
- Access to a Kubernetes cluster (for testing Kubernetes tools)

### Installation

Using uv (recommended):

```bash
# Install dependencies including dev dependencies
uv sync

# Activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

Using pip:

```bash
python -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

### Verify Setup

```bash
# Run the server locally
uv run python main.py

# Run tests
uv run pytest
```

## Making Changes

### Branching Strategy

1. **Create a feature branch** from `main`:
   ```bash
   git checkout main
   git pull upstream main
   git checkout -b feature/your-feature-name
   ```

2. **Branch naming conventions**:
   - `feature/` - New features
   - `fix/` - Bug fixes
   - `docs/` - Documentation changes
   - `refactor/` - Code refactoring
   - `test/` - Test additions or modifications

## Creating New MCP Tools

We follow a strict **Spec-Driven Development** methodology to ensure consistency, reliability, and security across our toolset. This process consists of three stages, each requiring human review before proceeding.

For the complete guide, see [LUMINO MCP Tool Development Guide](docs/LUMINO_MCP_TOOL_DEVELOPMENT_GUIDE.md).

### The Three-Stage Workflow

```
Stage 1: Functional Spec  →  Stage 2: Implementation Spec  →  Stage 3: Code
     (What)                        (How)                         (Build)
```

### Stage 1: Functional Specification

Define *what* the tool does without implementation details.

**Create `@{tool_name}_spec.txt`** containing:
- Purpose and description
- Parameters with types
- Return value schema (JSON structure)
- High-level implementation requirements

**Review Gate:** Verify the logic satisfies the use case and data structures are sound.

### Stage 2: Implementation Specification

Define *how* the tool integrates with the existing codebase.

**Create `@{tool_name}_tool_spec.txt`** adding:
- Available imports (e.g., `kubernetes.client`, `openshift.dynamic`)
- Existing helper functions to reuse
- Kubernetes/OpenShift clients to use
- Technically specific implementation requirements

**Review Gate:** Verify technical feasibility and reuse of existing patterns.

### Stage 3: Code Implementation

Write the executable code following the implementation spec.

**Create the tool in `src/server-mcp.py`**:

```python
@mcp.tool()
async def your_new_tool(
    required_param: str,
    optional_param: Optional[str] = None
) -> Dict[str, Any]:
    """
    Brief description of what the tool does.

    Args:
        required_param: Description of this parameter.
        optional_param: Description with default behavior.

    Returns:
        Dict: Keys: field1, field2, field3. Description of structure.
    """
    try:
        # Implementation following the spec
        return {"status": "success", "data": result}
    except Exception as e:
        logger.error(f"Error in your_new_tool: {str(e)}")
        return {"error": str(e)}
```

**Review Gate:** Code review, linting, and verification against the spec.

### File Naming Conventions

| Artifact Type | Naming Pattern | Example |
|---------------|----------------|---------|
| Global Template | `@_tool_spec.txt` | `@_tool_spec.txt` |
| Functional Spec | `@{tool_name}_spec.txt` | `@audit_scc_spec.txt` |
| Implementation Spec | `@{tool_name}_tool_spec.txt` | `@audit_scc_tool_spec.txt` |
| Source Code | Added to `src/server-mcp.py` | `audit_scc` function |

### Key Guidelines for Tool Development

1. **Read-Only Mandate:** All `@mcp.tool()` functions must be strictly read-only. No write/delete operations unless explicitly authorized.

2. **Schema Compliance:** The Python return value must exactly match the JSON structure defined in Stage 1.

3. **Reuse First:** Always check for existing helper functions before writing new logic.

4. **Type Hints:** Fully typed arguments and return values.

5. **Async/Await:** Use async for all Kubernetes API calls.

6. **Error Handling:** Catch `ApiException` and return structured error data.

7. **Logging:** Include `logger.info` and `logger.error` statements.

### Modifying Helper Modules

Helper modules in `src/helpers/` contain shared logic:

| Module | Purpose |
|--------|---------|
| `constants.py` | Shared constants and configuration |
| `event_analysis.py` | Event processing logic |
| `failure_analysis.py` | Root cause analysis algorithms |
| `log_analysis.py` | Log parsing and analysis |
| `resource_topology.py` | Topology mapping functions |
| `semantic_search.py` | NLP-based search |
| `utils.py` | General utility functions |

When modifying helpers, ensure backward compatibility with existing tools.

## Code Style

### Python Style Guide

We follow [PEP 8](https://pep8.org/) with these specifics:

- **Line length**: 100 characters maximum
- **Imports**: Group in order: standard library, third-party, local
- **Type hints**: Use type hints for function parameters and returns
- **Docstrings**: Use Google-style docstrings

### Formatting

```bash
# Format code (if ruff is installed)
uv run ruff format .

# Check for linting issues
uv run ruff check .
```

## Testing

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src

# Run specific test file
uv run pytest tests/test_tools.py

# Run tests matching a pattern
uv run pytest -k "test_namespace"
```

### Writing Tests

- Place tests in the `tests/` directory
- Name test files `test_*.py`
- Use pytest fixtures for common setup
- Mock Kubernetes API calls for unit tests

## Submitting Changes

### Pull Request Process

1. **Update your branch** with the latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Push your changes**:
   ```bash
   git push origin feature/your-feature-name
   ```

3. **Create a Pull Request** on GitHub:
   - Use a clear, descriptive title
   - Fill out the PR template completely
   - Link related issues using `Fixes #123` or `Relates to #123`

4. **PR Requirements**:
   - All CI checks must pass
   - Code must be reviewed by at least one maintainer
   - No merge conflicts with `main`
   - Documentation updated if needed

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): brief description

Longer description if needed.

Fixes #123
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(tools): add pod restart history tool

fix(logs): handle empty log response gracefully

docs(readme): add Kubernetes deployment section
```

## Releasing

This section documents how releases are managed and how container images are published.

### Version Numbering

We follow [Semantic Versioning](https://semver.org/):

- **MAJOR** (`1.0.0` → `2.0.0`): Breaking changes to tool APIs or behavior
- **MINOR** (`1.0.0` → `1.1.0`): New tools or features, backward compatible
- **PATCH** (`1.0.0` → `1.0.1`): Bug fixes, backward compatible

Pre-release versions use suffixes: `v1.0.0-rc1`, `v1.0.0-beta1`

### Container Image Registry

Container images are automatically built and pushed to:

```
quay.io/geored/lumino-mcp-server
```

### Image Tagging Strategy

| Git Event | Container Tags Created | Example |
|-----------|----------------------|---------|
| Push to `main` | `latest`, `main`, `<sha>` | `latest`, `main`, `a1b2c3d` |
| Tag `v1.0.0` | `1.0.0`, `1.0`, `<sha>` | `1.0.0`, `1.0`, `a1b2c3d` |
| Tag `v1.2.3` | `1.2.3`, `1.2`, `<sha>` | `1.2.3`, `1.2`, `a1b2c3d` |
| Tag `v2.0.0-rc1` | `2.0.0-rc1`, `<sha>` | `2.0.0-rc1`, `a1b2c3d` |
| Pull Request | Build only (no push) | - |

### Creating a Release

#### Option 1: GitHub Releases UI (Recommended)

1. Go to **Releases** → **Draft a new release**
2. Click **Choose a tag** → type new tag (e.g., `v0.2.0`) → **Create new tag**
3. Set the release title (e.g., `v0.2.0 - Feature Release`)
4. Write release notes describing changes
5. Click **Publish release**

The GitHub Actions workflow will automatically:
- Build the container image for `linux/amd64` and `linux/arm64`
- Push to quay.io with appropriate tags
- Run post-build verification tests

#### Option 2: Git Command Line

```bash
# Ensure you're on main with latest changes
git checkout main
git pull origin main

# Create and push an annotated tag
git tag -a v0.2.0 -m "Release v0.2.0 - Feature Release"
git push origin v0.2.0
```

### Release Checklist

Before creating a release:

- [ ] All tests pass on `main` branch
- [ ] Version updated in `pyproject.toml` (if applicable)
- [ ] README.md reflects any new tools or features
- [ ] CHANGELOG.md updated with release notes (if maintained)
- [ ] No outstanding critical issues

After release:

- [ ] Verify container image available on quay.io
- [ ] Test pulling and running the new image:
  ```bash
  podman pull quay.io/geored/lumino-mcp-server:0.2.0
  podman run --rm quay.io/geored/lumino-mcp-server:0.2.0 python --version
  ```
- [ ] Announce release (if applicable)

### Pulling Specific Versions

Users can pull specific versions:

```bash
# Latest development (main branch)
podman pull quay.io/geored/lumino-mcp-server:latest

# Specific version
podman pull quay.io/geored/lumino-mcp-server:1.0.0

# Specific minor version (gets latest patch)
podman pull quay.io/geored/lumino-mcp-server:1.0

# Specific commit
podman pull quay.io/geored/lumino-mcp-server:a1b2c3d
```

## Reporting Issues

### Bug Reports

When reporting bugs, include:

1. **Description**: Clear description of the issue
2. **Steps to reproduce**: Minimal steps to reproduce the behavior
3. **Expected behavior**: What you expected to happen
4. **Actual behavior**: What actually happened
5. **Environment**:
   - Python version
   - Operating system
   - Kubernetes version (if applicable)
   - MCP client being used
6. **Logs**: Relevant error messages or logs

### Feature Requests

For feature requests, include:

1. **Problem statement**: What problem does this solve?
2. **Proposed solution**: How should it work?
3. **Alternatives considered**: Other approaches you've thought about
4. **Additional context**: Any other relevant information

## Questions?

- Open a [GitHub Discussion](https://github.com/spre-sre/lumino-mcp-server/discussions) for questions
- Check existing issues before creating new ones

Thank you for contributing!
