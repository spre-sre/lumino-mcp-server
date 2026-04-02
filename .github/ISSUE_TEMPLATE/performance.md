---
name: "⚡ Performance Issue"
about: "Report a tool that is slow, consumes excessive memory, or makes too many API calls."
title: "[PERF] <short description>"
labels: ["performance", "needs-triage"]
assignees: []
---

## Performance Issue

### Summary
<!-- One-sentence description of the performance problem. -->

### Affected Tool / Component
<!-- e.g. `advanced_event_analytics`, `ml_persistence.py` -->

### Environment
| Field | Value |
|---|---|
| lumino-mcp-server version / SHA | |
| Python version | |
| OpenShift / k8s version | |
| Namespace size (approx. pod count) | |
| Deployment mode | |

### Observed Performance
<!-- Describe the slow/expensive behaviour with concrete numbers where possible. -->

- **Latency:** <!-- e.g. tool takes ~45 s to respond -->
- **Memory:** <!-- e.g. process RSS grows to 2 GB -->
- **API calls:** <!-- e.g. 500+ Kubernetes API requests per invocation -->
- **CPU:** <!-- e.g. pegged at 100% for 30 s -->

### Expected Performance
<!-- What would acceptable performance look like? -->

### Steps to Reproduce
1. 
2. 
3. 

### Profiling Data (optional)
```
<!-- paste cProfile / py-spy / memory_profiler output here -->
```

### Root Cause Hypothesis (optional)
<!-- e.g. "list_pods_in_namespace is called 4 times sequentially inside the tool instead of once." -->

### Proposed Fix (optional)
<!-- e.g. "Cache the pod list result and pass it to sub-functions." -->

### Checklist
- [ ] I have confirmed this is reproducible.
- [ ] I have included concrete numbers (latency, memory, API call count).
- [ ] I have searched existing issues and this is not a duplicate.
