# Devlake DORA Metrics Baseline Report

<!-- **Ticket:** [SPRE-5967](https://redhat.atlassian.net/browse/SPRE-5967)
**Epic:** [SPRE-5901](https://redhat.atlassian.net/browse/SPRE-5901) —   --> Agentic AI Workflows for SPRE 
**Period:** 2026-02-19 to 2026-08-19 (6 months) <br>
**Projects:** Secureflow - Konflux - Global, Secureflow - Konflux - Infrastructure Team
**Generated:** 2026-08-19

---

## DORA Metrics vs Industry Benchmarks

| DORA Metric | Current Value | DORA Level | Elite Benchmark |
|-------------|---------------|------------|-----------------|
| **Deployment Frequency** | 4.96 days/week (Global) | **High** | >= 5 days/week |
| **Lead Time for Changes** | 3.0 days (code-to-merge) | **High** | < 1 hour |
| **Lead Time (end-to-end)** | 56.1 days (merge-to-prod) | **Low** | < 1 hour |
| **Change Failure Rate** | ~42.1% (approx, see caveats) | **Low** (likely overstated) | < 5% |
| **MTTR** | 25.1 min median (from SPRE-5966) | **Elite** | < 1 hour |

---

## 1. Deployment Frequency

### Konflux Global
| Metric | Value |
|--------|-------|
| Total deployments | 649 |
| Unique deployment days | 134 / 181 days |
| Avg deployments/week | 24.0 |
| Avg deployment days/week | **4.96** |
| **DORA Level** | **High** (just below Elite threshold of 5) |

### Konflux Infrastructure Team
| Metric | Value |
|--------|-------|
| Total deployments | 406 |
| Unique deployment days | 87 / 181 days |
| Avg deployments/week | 15.6 |
| Avg deployment days/week | **3.35** |
| **DORA Level** | **High** |

### Monthly Deployment Trend

| Month | Global Deploys | Infra Deploys | Global Days | Infra Days |
|-------|---------------|---------------|-------------|------------|
| Feb 2026 | 62 | 51 | 6 | 6 |
| Mar 2026 | 258 | 212 | 26 | 26 |
| Apr 2026 | 128 | 92 | 25 | 19 |
| May 2026 | 41 | 9 | 19 | 7 |
| Jun 2026 | 68 | 16 | 23 | 11 |
| Jul 2026 | 65 | 18 | 22 | 12 |
| Aug 2026 | 27 | 8 | 13 | 6 |

Deployment volume peaked in March and dropped significantly from May onward, especially for the Infrastructure team.

---

## 2. Lead Time for Changes

| Breakdown | Avg Hours | Human |
|-----------|-----------|-------|
| Coding time | 10.0 | 10 hours |
| Pickup time (PR submitted → 1st review) | 16.1 | 16 hours |
| Review time (1st review → merged) | 45.2 | 1.9 days |
| Code-to-merge subtotal | 71.3| 3.0 days |
| Deploy time (merged → production) | 1,227.9 | 51.2 days |
| **Total lead time** | **1,345.5** | **56.1 days** |

**Key insight:** The code-to-merge pipeline (3 days) rates as **DORA High**. However, the merge-to-production deploy time (51.2 days) drags the overall lead time to **DORA Low**. This deploy lag is the single biggest opportunity for improvement.

---

## 3. PR Cycle Time

Based on 2,876 merged PRs in the Infrastructure Team.

| Metric | Value |
|--------|-------|
| Avg total cycle time | **4.1 days** (98.5 hours) |
| Avg coding time | 11.1 hours |
| Avg pickup time | 22.0 hours |
| Avg review time | 1.7 days (39.7 hours) |

### Cycle Time by PR Size

| Size | PR Count | Avg Cycle Time | Avg Review Time |
|------|----------|---------------|-----------------|
| XS (1-50 lines) | 2,296 | 3.6 days | 1.3 days |
| S (51-200 lines) | 327 | 5.0 days | 2.5 days |
| M (201-500 lines) | 142 | 6.2 days | 3.8 days |
| L (501-1000 lines) | 59 | 10.3 days | 3.4 days |
| XL (>1000 lines) | 52 | 6.2 days | 3.6 days |

Smaller PRs merge significantly faster. 80% of PRs are XS (< 50 lines), indicating good PR hygiene.

### Top Repos by Cycle Time

| Repository | Merged PRs | Avg Cycle Time |
|------------|-----------|----------------|
| infra-deployments | 1,506 | 3.1 days |
| infra-common-deployments | 444 | 1.6 days |
| namespace-lister | 133 | 4.2 days |
| oauth2-proxy | 130 | 1.4 days |
| tekton-kueue | 127 | 15.8 days |
| multi-platform-controller | 92 | 19.6 days |
| etcd-shield | 77 | 10.1 days |

---

## 4. PR Statistics

| Metric | Value |
|--------|-------|
| Total PRs (6 months) | 4,766 |
| Merged | 2,794 (58.6%) |
| Open | 250 |
| Closed (not merged) | 1,722 |
| Stale (> 14 days) | 168 |
| Engineering PRs | 3,722 (57.8% merge rate) |
| Dependency bot PRs | 1,021 (63.1% merge rate) |

---

## 5. Change Failure Rate

Cross-referenced Devlake deployment data (649 deployments) with PagerDuty incident data (SPRE-5966 baseline), filtered to Konflux-specific production services only.

| Month | Deployments | Konflux Incidents | Approximate CFR |
|-------|-------------|-------------------|-----------------|
| Feb 2026 | 62 | 13 | 21.0% |
| Mar 2026 | 258 | 45 | 17.4% |
| Apr 2026 | 128 | 40 | 31.2% |
| May 2026 | 41 | 38 | 92.7% |
| Jun 2026 | 68 | 73 | 107.4% |
| Jul 2026 | 65 | 44 | 67.7% |
| Aug 2026 | 27 | 20 | 74.1% |
| **Total** | **649** | **273** | **42.1%** |

**Important caveats:**
- This is a **temporal approximation**, not a causal mapping. PagerDuty incidents are not tagged to specific deployments.
- Many incidents are **recurring alerts** (ACMHostedCluster, cert-manager) unrelated to individual deployments.
- Months with fewer deployments (May-Aug) show inflated CFR because the same recurring alerts fire regardless of deployment activity.
- A precise CFR would require deployment-incident linkage at the event level, which neither DevLake nor PagerDuty currently provides out of the box.

**Approximate DORA Level:** Low (42.1%), but likely overstated due to the caveats above. The true deployment-caused failure rate is almost certainly lower.

---

## Summary & Implications for Agentic AI (SPRE-5901)

### Strengths
- **MTTR is Elite-tier** (25.1 min median) — incidents resolve fast
- **Deployment frequency is High-tier** (nearly 5 days/week globally) — the team ships regularly
- **PR hygiene is good** — 80% of PRs are XS, and merge rates are healthy

### Improvement Opportunities
1. **Merge-to-production deploy lag (51.2 days)** — This is the dominant bottleneck. Agentic workflows could accelerate promotion through automated validation and staged rollout checks.
2. **Review time (1.7 days avg)** — Agentic code review assistance could reduce pickup and review time, especially for the repos with 10-20 day cycle times (tekton-kueue, multi-platform-controller).
3. **168 stale PRs** — Automated triage and nudging could reduce PR staleness.
4. **Change failure rate unknown** — Building an automated cross-reference between deployments and incidents would close this gap.

---

## Deliverables

| File | Description |
|------|-------------|
| `devlake_dora_baseline_metrics.json` | Structured JSON with all DORA metrics and benchmark comparison |
| `devlake_dora_baseline_report.md` | This summary report |

*This baseline supplements the PagerDuty baseline (SPRE-5966) and together they form the foundation for measuring Agentic AI improvements (SPRE-5901).*
