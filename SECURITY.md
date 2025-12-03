# Security Policy

## Supported Versions

We release patches for security vulnerabilities in the following versions:

| Version | Supported          |
|---------|--------------------|
| 0.1.x   | :white_check_mark: |

As the project matures, this table will be updated to reflect our support policy.

## Reporting a Vulnerability

We take the security of LUMINO MCP Server seriously. If you discover a security vulnerability, please report it responsibly.

### How to Report

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of these methods:

1. **GitHub Security Advisories** (Preferred)
   - Go to the [Security Advisories](https://github.com/spre-sre/lumino-mcp-server/security/advisories) page
   - Click "New draft security advisory"
   - Fill in the details of the vulnerability

2. **Email**
   - Send an email to the maintainers (see repository for contact)
   - Use a descriptive subject line: `[SECURITY] Brief description`

### What to Include

Please include the following information in your report:

- **Type of vulnerability** (e.g., privilege escalation, information disclosure, injection)
- **Affected component** (e.g., specific tool name, helper module)
- **Steps to reproduce** the vulnerability
- **Proof of concept** code or commands (if applicable)
- **Impact assessment** - what an attacker could achieve
- **Suggested fix** (if you have one)

### What to Expect

1. **Acknowledgment**: We will acknowledge receipt of your report within 48 hours.

2. **Initial Assessment**: Within 7 days, we will provide an initial assessment of the vulnerability and an estimated timeline for a fix.

3. **Status Updates**: We will keep you informed about the progress of addressing the vulnerability.

4. **Resolution**: Once the vulnerability is fixed:
   - We will notify you before the public disclosure
   - We will credit you in the security advisory (unless you prefer to remain anonymous)
   - A new release will be published with the fix

### Disclosure Policy

- We follow a coordinated disclosure process
- We aim to fix critical vulnerabilities within 30 days
- Public disclosure occurs after a fix is available
- We will coordinate with you on the disclosure timeline

## Security Considerations

### Read-Only by Design

LUMINO MCP Server tools are designed to be **read-only**. They query and analyze Kubernetes resources but do not modify cluster state. This architectural decision significantly reduces the attack surface.

### Kubernetes RBAC

The server operates with the permissions granted to its service account or kubeconfig. We recommend:

- **Principle of least privilege**: Grant only the permissions needed for the tools you use
- **Separate service accounts**: Use dedicated service accounts for different environments
- **Audit logging**: Enable Kubernetes audit logging to track API access

### Recommended RBAC Configuration

For read-only monitoring operations:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: lumino-mcp-reader
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log", "events", "namespaces", "nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["tekton.dev"]
    resources: ["pipelineruns", "taskruns", "pipelines", "tasks"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["apps"]
    resources: ["deployments", "replicasets", "statefulsets", "daemonsets"]
    verbs: ["get", "list", "watch"]
```

### Network Security

When running in Kubernetes with HTTP transport:

- Use network policies to restrict access to the MCP server
- Consider using service mesh for mTLS
- Do not expose the server directly to the internet

### Secrets and Credentials

- Never commit kubeconfig files or credentials to the repository
- Use Kubernetes secrets or external secret management
- The `.gitignore` excludes common credential files

## Security Best Practices for Contributors

When contributing code:

1. **No hardcoded credentials**: Never include API keys, passwords, or tokens
2. **Input validation**: Validate and sanitize all user inputs
3. **Error handling**: Don't expose sensitive information in error messages
4. **Dependency management**: Keep dependencies updated to patch known vulnerabilities
5. **Code review**: All changes require security-conscious code review

## Known Security Limitations

1. **Log exposure**: Tools that retrieve logs may expose sensitive information contained in those logs. Users should be aware of what data their applications log.

2. **Prometheus queries**: The `prometheus_query` tool executes user-provided PromQL. While read-only, complex queries could impact Prometheus performance.

3. **Resource enumeration**: Tools that list resources across namespaces reveal the cluster structure to authorized users.

## Acknowledgments

We thank the security researchers and community members who help keep LUMINO MCP Server secure.

<!-- Security researchers who have responsibly disclosed vulnerabilities will be listed here -->
