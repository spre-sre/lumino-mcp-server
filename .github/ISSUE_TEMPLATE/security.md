---
name: "🔒 Security Issue (Non-sensitive)"
about: "Report a non-sensitive security concern such as a hardened-config gap or dependency CVE. For actual vulnerabilities, use Security Advisories."
title: "[SECURITY] <short description>"
labels: ["security", "needs-triage"]
assignees: []
---

> ⚠️ **STOP — Is this a real vulnerability?**
> If this issue could allow an attacker to compromise a cluster, exfiltrate secrets, or escalate privileges, please **do NOT open a public issue**.
> Instead, use the [GitHub Security Advisory](https://github.com/spre-sre/lumino-mcp-server/security/advisories/new) to report it privately.
> See [SECURITY.md](../../SECURITY.md) for the full disclosure policy.

---

## Security Concern (Non-sensitive)

### Summary
<!-- One-sentence description of the security concern. -->

### Category
<!-- Check all that apply -->
- [ ] Dependency with known CVE (non-exploitable in this context)
- [ ] Missing input validation / injection risk
- [ ] Overly broad RBAC / permissions
- [ ] Sensitive data logged or returned in tool output
- [ ] Insecure default configuration
- [ ] Container hardening gap (Containerfile)
- [ ] Other: <!-- describe -->

### Affected Component
<!-- e.g. `pyproject.toml` dependency, `Containerfile`, `server-mcp.py` -->

### Description
<!-- Describe the concern in detail. Include CVE IDs, CWE IDs, or references where applicable. -->

### Impact Assessment
<!-- What is the realistic impact if this is not addressed? -->

### Suggested Remediation
<!-- What change would fix or mitigate this concern? -->

### References
<!-- Links to CVE advisories, OWASP pages, upstream issues, etc. -->

### Checklist
- [ ] I have confirmed this is NOT a directly exploitable vulnerability (those go to Security Advisories).
- [ ] I have included a CVE ID or reference where applicable.
- [ ] I have searched existing issues and this is not a duplicate.
