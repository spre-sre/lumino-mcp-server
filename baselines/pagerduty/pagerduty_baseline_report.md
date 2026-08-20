# PagerDuty 6-Month Incident Baseline Report

<!-- **Ticket:** [SPRE-5966](https://redhat.atlassian.net/browse/SPRE-5966)
**Epic:** [SPRE-5901](https://redhat.atlassian.net/browse/SPRE-5901) --> Agentic AI Workflows for SPRE
**Period:** 2026-02-19 to 2026-08-19 (6 months)
**Teams:** SP Resilience Engineering, Layered Products SRE
**Generated:** 2026-08-19

---

## Key Findings

### 1. MTTR (Mean Time to Resolve)

| Metric | Value |
|--------|-------|
| Total resolved incidents | 28,988 |
| **Median MTTR** | **25.1 minutes** |
| Mean MTTR | 20.8 hours |
| p75 MTTR | 11.4 hours |
| p90 MTTR | 2.4 days |
| p95 MTTR | 5.1 days |
| p99 MTTR | 11.3 days |

The median MTTR of 25 minutes indicates most incidents are auto-resolved or resolved quickly. However, the large gap between median (25m) and mean (20.8h) reveals a long tail of incidents that take days to resolve, heavily skewing the average.

### 2. Triage Time Distribution

| Metric | Value |
|--------|-------|
| Incidents with acknowledgement data | 8 |
| Median triage time | 45 seconds |
| Mean triage time | 1.0 day |
| p90 triage time | 8.1 days |

Only 8 out of 29,160 incidents have explicit acknowledgement records. This indicates the vast majority of incidents are either auto-resolved without human intervention or resolved directly without a formal acknowledge step. The few that are acknowledged show very fast initial response (median 45s) but some outliers sit for days.

### 3. Top Alerting Services

| Rank | Service | Incidents | % of Total | Alerts |
|------|---------|-----------|------------|--------|
| 1 | Service Lifecycle Soaking | 21,680 | 74.3% | 48,103 |
| 2 | Service Lifecycle Silent Test | 5,165 | 17.7% | 5,165 |
| 3 | Service Lifecycle SRE | 1,114 | 3.8% | 1,118 |
| 4 | Managed API Service - Hive Cluster A | 474 | 1.6% | 474 |
| 5 | Managed API Service - Hive Cluster B | 310 | 1.1% | 310 |
| 6 | Konflux Dataplane | 170 | 0.6% | 291 |
| 7 | CSSRE Pager RHOAM | 55 | 0.2% | 0 |
| 8 | SPRE Traditional Pipelines | 55 | 0.2% | 59 |
| 9 | SPRE-5431 PoC - Konflux SLO Alerts | 40 | 0.1% | 29 |
| 10 | SLSRE Package Operator | 18 | 0.1% | 18 |

**Service Lifecycle Soaking alone accounts for 74.3% of all incidents.** The top 3 services (all Service Lifecycle related) account for 95.8% of total volume. This is a significant signal — most alert noise comes from soaking/testing services, not production.

### 4. Alert Volume by Urgency

| Urgency | Count | % |
|---------|-------|---|
| High | 15,466 | 53% |
| Low | 13,694 | 47% |

Roughly even split between high and low urgency. Over half of all incidents are classified as high urgency.

### 5. Monthly Incident Volume Trend

| Month | Incidents | Trend |
|-------|-----------|-------|
| Feb 2026 | 9,101 | Peak |
| Mar 2026 | 5,691 | -37% |
| Apr 2026 | 4,789 | -16% |
| May 2026 | 2,328 | -51% |
| Jun 2026 | 2,945 | +27% |
| Jul 2026 | 2,162 | -27% |
| Aug 2026 | 2,144 | -1% (partial month) |

Volume dropped 76% from February (9,101) to July/August (~2,100). This suggests either noise reduction efforts were effective or soaking test activity decreased.

### 6. Top Incident Patterns

| Pattern | Count | Category |
|---------|-------|----------|
| ACMHostedClusterKubeConfigSecretCopyFailure | ~5,284 | ACM/HyperShift config issue |
| Multiple Alerts on Managed API Hive Clusters | ~727 | Hive cluster health |
| OADPRecoveryJobStale | ~316 | Backup recovery staleness |
| CertManagerCertNotReady | ~177 | Certificate renewal failure |

The single most frequent alert pattern — **ACMHostedClusterKubeConfigSecretCopyFailure** — fires across 15+ management clusters and accounts for thousands of incidents. This is a prime candidate for automated triage or suppression.

---

## Summary

| Metric | Value |
|--------|-------|
| Total incidents (6 months) | 29,160 |
| Resolved | 28,988 (99.4%) |
| Median MTTR | 25.1 minutes |
| Mean MTTR | 20.8 hours |
| p90 MTTR | 2.4 days |
| Top service by volume | Service Lifecycle Soaking (74.3%) |
| Top alert pattern | ACMHostedClusterKubeConfigSecretCopyFailure |
| Monthly trend | 76% decrease (Feb to Jul) |
| High urgency ratio | 53% |

## Implications for Agentic AI (SPRE-5901)

1. **Alert noise reduction** — 92% of incidents come from soaking/testing services. Agentic triage could auto-classify and suppress these, reducing human attention load.
2. **Long-tail MTTR** — While median is 25m, the p90 is 2.4 days. The biggest MTTR wins will come from accelerating diagnosis on the ~10% of incidents that drag for days.
3. **Repetitive patterns** — ACMHostedClusterKubeConfigSecretCopyFailure alone could be a first runbook automation target, covering thousands of incidents.
4. **Low acknowledgement rate** — Only 8 of 29,160 incidents were formally acknowledged, suggesting most are handled through auto-resolution or direct resolution without triage. Agentic workflows should focus on the incidents that actually require human investigation.

---

## Deliverables

| File | Description |
|------|-------------|
| `pagerduty_baseline_metrics.json` | Structured JSON with MTTR, triage time, alert volume by service/severity |
| `pagerduty_incidents_raw.json` | Raw incident data (29,160 incidents) with timestamps, status, service, assignments |
| `pagerduty_baseline_report.md` | This summary report |
| `extract_pagerduty_baseline.py` | Extraction script (reproducible) |

*This baseline is the foundation for measuring improvements from Agentic AI workflows (SPRE-5901).*
