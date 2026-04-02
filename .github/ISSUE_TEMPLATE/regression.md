---
name: "🔁 Regression Report"
about: "Something that used to work is now broken after a recent change."
title: "[REGRESSION] <short description>"
labels: ["regression", "bug", "needs-triage"]
assignees: []
---

## Regression Report

### Summary
<!-- One-sentence description of what broke and when it started. -->

### Affected Tool / Component
<!-- e.g. `investigate_tls_certificate_issues`, `list_pods_in_namespace`, `resource_topology.py` -->

### Last Known Good Version / Commit
<!-- The commit SHA or tag where this still worked correctly. -->
- **Last good:** `<!-- e.g. 43feeb3 -->`
- **First bad:** `<!-- e.g. 4859e41 -->`

### Steps to Reproduce
1. 
2. 
3. 

### Expected Behaviour (before regression)
<!-- What did the tool return / do before the regression? -->

### Actual Behaviour (after regression)
<!-- What does it return / do now? -->

### Relevant Logs / Stack Trace
```
<!-- paste here -->
```

### Bisection / Investigation Notes
<!-- If you have already narrowed down the offending commit or code change, share it here. -->

### Checklist
- [ ] I have confirmed this worked in a previous version.
- [ ] I have identified (or attempted to identify) the first bad commit.
- [ ] I have searched existing issues and this is not a duplicate.
- [ ] This is NOT a security vulnerability.
