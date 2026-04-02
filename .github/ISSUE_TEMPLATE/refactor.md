---
name: "🔧 Refactor / Code Quality"
about: "Propose a code quality improvement, refactor, or technical debt reduction."
title: "[REFACTOR] <short description>"
labels: ["refactor", "tech-debt"]
assignees: []
---

## Refactor / Code Quality Proposal

### Summary
<!-- One-sentence description of the refactor or code quality improvement. -->

### Affected File(s) / Function(s)
<!-- List the specific files and functions involved. -->

### Current State (the problem)
<!-- Describe the current code structure and why it is problematic.
     Include file paths, function names, and line numbers where possible. -->

```python
# Example of current problematic code
```

### Proposed State (the solution)
<!-- Describe the desired code structure after the refactor. -->

```python
# Example of proposed improved code
```

### Motivation / Benefits
<!-- Why should we make this change? Check all that apply: -->
- [ ] Reduces code duplication (DRY)
- [ ] Improves readability / maintainability
- [ ] Reduces cyclomatic complexity
- [ ] Improves testability
- [ ] Reduces technical debt
- [ ] Improves performance
- [ ] Aligns with project conventions
- [ ] Enables a future feature
- [ ] Other: <!-- describe -->

### Risk Assessment
<!-- What is the risk of making this change? Could it introduce regressions? -->
- **Risk level:** <!-- Low / Medium / High -->
- **Affected tools:** <!-- list MCP tools that call the refactored code -->
- **Test coverage needed:** <!-- describe what tests should be added/updated -->

### Checklist
- [ ] I have searched existing issues and this is not a duplicate.
- [ ] I have identified all call sites of the code being changed.
- [ ] I am willing to submit a PR for this refactor (optional but appreciated!).
