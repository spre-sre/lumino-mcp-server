# Lumino MCP Tool Development Guide

## 1. Overview
This document outlines the standard operating procedure for creating new Model Context Protocol (MCP) tools for the Lumino project. We follow a strict **Spec-Driven Development** methodology to ensure consistency, reliability, and security across our toolset.

The process consists of three distinct stages, each requiring a human review gate before proceeding to the next. This ensures that requirements are clear and implementation details are validated before any code is written.

## 2. The Workflow

The development lifecycle follows a linear progression:

1.  **Stage 1: Functional Specification**
    *   **Input:** User requirements & Template (`@_tool_spec.txt`)
    *   **Output:** Functional Spec (`@{tool_name}_spec.txt`)
    *   **Goal:** Define *what* the tool does.
2.  **Stage 2: Implementation Specification**
    *   **Input:** Functional Spec (`@{tool_name}_spec.txt`)
    *   **Output:** Implementation Prompt (`@{tool_name}_tool_spec.txt`)
    *   **Goal:** Define *how* the tool is built within the specific technical context.
3.  **Stage 3: Code Implementation**
    *   **Input:** Implementation Prompt (`@{tool_name}_tool_spec.txt`)
    *   **Output:** Python Code (`{tool_name}_tool.py`)
    *   **Goal:** Write the executable code.

---

## 3. Detailed Steps

### Stage 1: Functional Specification
**Objective:** Create a high-level definition of the tool's purpose, inputs, and outputs without getting bogged down in implementation code.

*   **Process:**
    1.  Start with the global template `@_tool_spec.txt`.
    2.  Define the tool's **Purpose**, **Description**, **Parameters** (with types), and a precise **Return Value** schema.
    3.  List high-level **Implementation Requirements** (business logic).
*   **Review Gate:** Verify that the logic satisfies the business use case and the data structures are sound.

**Example Artifact:** `@audit_security_context_constraints_spec.txt`
> *Defines the `audit_security_context_constraints` tool, specifying complex return objects like `audit_summary`, `scc_evaluation`, and `violations`.*

### Stage 2: Implementation Specification
**Objective:** Translate the functional requirements into a technical prompt for the developer or coding agent. This step bridges the gap between "what we want" and "what the codebase supports."

*   **Process:**
    1.  Take the content from Stage 1.
    2.  Contextualize it with the project's existing capabilities:
        *   **Imports:** List specific libraries available (e.g., `openshift.dynamic`, `kubernetes.client`).
        *   **Helpers:** Identify existing helper functions to reuse (e.g., `SCC assignment analyzers`).
        *   **Clients:** Specify which K8s/OpenShift clients to use.
    3.  Refine **Implementation Requirements** to be technically specific (e.g., "Query OpenShift Security API for SecurityContextConstraints").
*   **Review Gate:** Verify technical feasibility and ensure reuse of existing patterns.

**Example Artifact:** `@audit_security_context_constraints_tool_spec.txt`
> *Adds sections like `EXISTING IMPORTS AVAILABLE` and `EXISTING HELPER FUNCTIONS AVAILABLE`, ensuring the implementation integrates seamlessly with the current environment.*

### Stage 3: Code Implementation
**Objective:** Generate the actual Python code for the MCP tool.

*   **Process:**
    1.  Strictly follow the instructions in the Stage 2 Implementation Spec.
    2.  Implement the function using the `@mcp.tool()` decorator.
    3.  Adhere to standard practices:
        *   **Type Hints:** Fully typed arguments and return values.
        *   **Async/Await:** For all API calls.
        *   **Error Handling:** Catch `ApiException` and return structured error data.
        *   **Logging:** Include `logger.info` and `logger.error` statements.
*   **Review Gate:** Code review, linting, and verification against the Spec.

**Example Artifact:** `audit_security_context_constraints_tool.py`
> *Contains the `async def audit_security_context_constraints(...)` function, helper functions like `_get_security_context_constraints`, and comprehensive error handling.*

---

## 4. Using the Global Template

The global template `docs/_tool_spec.txt` provides a structured starting point for creating Implementation Specifications (Stage 2). This template ensures consistency and prevents hallucinations by explicitly defining available resources.

### Template Location

```
docs/_tool_spec.txt
```

### Template Structure

The template contains placeholder sections that must be filled in:

```
EXISTING IMPORTS AVAILABLE:
{{ imports_from_current_project }}

EXISTING HELPER FUNCTIONS AVAILABLE:
{{ helper_functions_from_current_project }}

EXISTING KUBERNETES CLIENTS AVAILABLE:
{{ existing_k8s_clients_from_current_project }}

EXISTING PROMETHEUS ENDPOINTS:
{{ metrics_endpoints_from_current_project }}

FUNCTIONALITY REQUIREMENTS:
{{ user_defined_mcp.tool_requirements_for_this_tool }}
```

### How to Fill the Placeholders

| Placeholder | How to Populate |
| :--- | :--- |
| `{{ imports_from_current_project }}` | Review `src/server-mcp.py` imports section. List relevant libraries: `kubernetes.client`, `openshift.dynamic`, `pandas`, etc. |
| `{{ helper_functions_from_current_project }}` | Check `src/helpers/*.py` for reusable functions. List function names and brief descriptions. |
| `{{ existing_k8s_clients_from_current_project }}` | List available clients: `k8s_core_api`, `k8s_apps_api`, `k8s_custom_api`, `dyn_client`, etc. |
| `{{ metrics_endpoints_from_current_project }}` | If tool needs Prometheus, list available endpoints and query patterns. |
| `{{ user_defined_mcp.tool_requirements_for_this_tool }}` | Copy from Stage 1 Functional Spec: purpose, parameters, return schema, requirements. |

### Example: Filling the Template

For a new tool `get_pod_restart_history`:

```
EXISTING IMPORTS AVAILABLE:
- kubernetes.client (CoreV1Api, AppsV1Api)
- from helpers.utils import format_timestamp, parse_duration
- from helpers.log_analysis import extract_error_patterns

EXISTING HELPER FUNCTIONS AVAILABLE:
- format_timestamp(dt) - Converts datetime to ISO string
- parse_duration(duration_str) - Parses "1h", "30m" to seconds
- get_pod_containers(pod) - Extracts container info from pod spec

EXISTING KUBERNETES CLIENTS AVAILABLE:
- k8s_core_api: CoreV1Api for pods, events, namespaces
- k8s_apps_api: AppsV1Api for deployments, statefulsets

EXISTING PROMETHEUS ENDPOINTS:
- Not required for this tool

FUNCTIONALITY REQUIREMENTS:
Purpose: Retrieve restart history for pods in a namespace
Parameters:
  - namespace (str): Target namespace
  - pod_name (str, optional): Filter to specific pod
  - hours (int, default=24): Time window
Return Value:
  - pods: List of pod restart records with timestamps and reasons
  - summary: Total restarts, most affected pods
```

---

## 5. File Naming Conventions

To maintain organization, use the following naming pattern for all new tools:

| Artifact Type | Naming Pattern | Location | Example |
| :--- | :--- | :--- | :--- |
| **Global Template** | `_tool_spec.txt` | `docs/` | `docs/_tool_spec.txt` |
| **Functional Spec** | `@{tool_name}_spec.txt` | `docs/` | `docs/@get_pod_restart_history_spec.txt` |
| **Impl. Spec** | `@{tool_name}_tool_spec.txt` | `docs/` | `docs/@get_pod_restart_history_tool_spec.txt` |
| **Source Code** | Added to `server-mcp.py` | `src/` | Function in `src/server-mcp.py` |

## 6. Key Guidelines for Developers

1.  **Read-Only Mandate:** `@mcp.tool()` functions must be strictly read-only. Do not implement write/delete operations unless explicitly authorized and safeguarded.
2.  **Schema Compliance:** The Python return value must *exactly* match the JSON structure defined in the Stage 1 Spec.
3.  **Reuse:** Always check for existing helper functions in `@{tool_name}_tool_spec.txt` before writing new logic.

## 7. Methodology Comparison: Lumino vs. Industry Standard

To understand why we follow this strict process, it helps to compare it with the general industry standard for AI Spec-Driven Development.

### Industry Standard (AIDD) vs. Lumino Process

| Feature | Industry Standard (AIDD) | Lumino Process |
| :--- | :--- | :--- |
| **Specification Depth** | Often one comprehensive spec (natural language or formal). | **Dual Specs:** Separates "Business Logic" (Stage 1) from "Technical Context" (Stage 2). |
| **Context Handling** | LLM often retrieves context dynamically (RAG). | **Static Context Injection:** The Implementation Spec hardcodes available imports/helpers to prevent hallucinations. |
| **Human Oversight** | "Human-in-the-loop" usually at the end. | **Strict Gates:** Human review is required *between* specs, ensuring alignment before code is generated. |
| **Verification** | Emphasizes *Test-Driven* (generating tests first). | Emphasizes *Schema-Driven* (strictly matching the JSON return value defined in Stage 1). |

### Why This Matters

1.  **Hallucination Prevention:** By creating an intermediate "Implementation Spec" that lists *exactly* which imports and helpers are available, we solve a major AI coding issue: hallucinating libraries that don't exist in the project.
2.  **Security:** The "Read-Only" mandate and strict schema definitions in the Functional Spec prevent scope creep better than a generic prompt would.
3.  **Reliability:** We trade the speed of end-to-end automation for the safety of intermediate structural checks, which is critical for infrastructure tooling.