#!/usr/bin/env python3
"""
Extract 6-month PagerDuty incident baseline.
Computes MTTR, triage time, alert volume by service/severity.
Outputs JSON artifact + summary report.
"""

import json
import os
import sys
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import statistics
import httpx

API_TOKEN = os.getenv("PAGERDUTY_API_TOKEN")
if not API_TOKEN:
    raise ValueError("PAGERDUTY_API_TOKEN environment variable is required")
BASE_URL = "https://api.pagerduty.com"
HEADERS = {
    "Authorization": f"Token token={API_TOKEN}",
    "Accept": "application/vnd.pagerduty+json;version=2",
    "Content-Type": "application/json",
}

SINCE = "2026-02-19T00:00:00Z"
UNTIL = "2026-08-19T23:59:59Z"
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

SPRE_TEAM_ID = "Team_ID"
RELATED_TEAM_IDS = [
    "Team_ID",  # SP Resilience Engineering
    "Team_ID_2",  # Layered Products SRE
]


def fetch_all_incidents():
    """Fetch all incidents in the 6-month window, chunked by month."""
    incidents = []
    start = datetime(2026, 2, 19, tzinfo=timezone.utc)
    end = datetime(2026, 8, 19, 23, 59, 59, tzinfo=timezone.utc)

    chunks = []
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + timedelta(days=14), end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end

    with httpx.Client(base_url=BASE_URL, headers=HEADERS, timeout=60) as client:
        for chunk_since, chunk_until in chunks:
            offset = 0
            limit = 100
            since_str = chunk_since.strftime("%Y-%m-%dT%H:%M:%SZ")
            until_str = chunk_until.strftime("%Y-%m-%dT%H:%M:%SZ")

            while True:
                params = {
                    "since": since_str,
                    "until": until_str,
                    "limit": limit,
                    "offset": offset,
                    "team_ids[]": RELATED_TEAM_IDS,
                }
                print(f"  {since_str[:10]}..{until_str[:10]} offset={offset}...", end=" ", flush=True)
                resp = client.get("/incidents", params=params)
                resp.raise_for_status()
                data = resp.json()

                batch = data.get("incidents", [])
                incidents.extend(batch)
                print(f"got {len(batch)} (total: {len(incidents)})")

                if not data.get("more", False):
                    break
                offset += limit

    return incidents


def parse_ts(ts_str):
    if not ts_str:
        return None
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def compute_metrics(incidents):
    """Compute all baseline metrics from raw incidents."""

    resolved = []
    acknowledged = []
    by_service = defaultdict(lambda: {"total": 0, "resolved": 0, "triggered": 0, "acknowledged": 0, "alerts": 0})
    by_urgency = defaultdict(int)
    by_month = defaultdict(int)
    by_title = defaultdict(int)
    mttr_values = []
    triage_values = []

    for inc in incidents:
        created = parse_ts(inc.get("created_at"))
        resolved_at = parse_ts(inc.get("resolved_at"))
        last_change = parse_ts(inc.get("last_status_change_at"))
        status = inc.get("status", "unknown")
        urgency = inc.get("urgency", "unknown")
        service_name = inc.get("service", {}).get("summary", "unknown")
        service_id = inc.get("service", {}).get("id", "unknown")
        alert_count = inc.get("alert_counts", {}).get("all", 0)
        title = inc.get("title", "unknown")

        # By service
        by_service[service_name]["total"] += 1
        by_service[service_name]["alerts"] += alert_count
        by_service[service_name][status] += 1
        by_service[service_name]["service_id"] = service_id

        # By urgency
        by_urgency[urgency] += 1

        # By month
        if created:
            by_month[created.strftime("%Y-%m")] += 1

        # By title pattern
        by_title[title] += 1

        # MTTR (only for resolved incidents)
        if status == "resolved" and created and resolved_at:
            mttr_seconds = (resolved_at - created).total_seconds()
            if mttr_seconds >= 0:
                mttr_values.append(mttr_seconds)
                resolved.append(inc)

        # Triage time (time to first acknowledge)
        if status in ("acknowledged", "resolved"):
            ack_at = None
            for assignment in inc.get("acknowledgements", []):
                ack_ts = parse_ts(assignment.get("at"))
                if ack_ts:
                    if ack_at is None or ack_ts < ack_at:
                        ack_at = ack_ts
            if not ack_at and last_change and status == "acknowledged":
                ack_at = last_change
            if ack_at and created:
                triage_seconds = (ack_at - created).total_seconds()
                if triage_seconds >= 0:
                    triage_values.append(triage_seconds)

    def percentiles(values, ps):
        if not values:
            return {f"p{p}": None for p in ps}
        sorted_v = sorted(values)
        result = {}
        for p in ps:
            idx = int(len(sorted_v) * p / 100)
            idx = min(idx, len(sorted_v) - 1)
            result[f"p{p}"] = round(sorted_v[idx], 1)
        return result

    def fmt_duration(seconds):
        if seconds is None:
            return "N/A"
        if seconds < 60:
            return f"{seconds:.0f}s"
        if seconds < 3600:
            return f"{seconds/60:.1f}m"
        if seconds < 86400:
            return f"{seconds/3600:.1f}h"
        return f"{seconds/86400:.1f}d"

    mttr_stats = {
        "count": len(mttr_values),
        "mean_seconds": round(statistics.mean(mttr_values), 1) if mttr_values else None,
        "median_seconds": round(statistics.median(mttr_values), 1) if mttr_values else None,
        **percentiles(mttr_values, [50, 75, 90, 95, 99]),
    }

    triage_stats = {
        "count": len(triage_values),
        "mean_seconds": round(statistics.mean(triage_values), 1) if triage_values else None,
        "median_seconds": round(statistics.median(triage_values), 1) if triage_values else None,
        **percentiles(triage_values, [50, 75, 90, 95, 99]),
    }

    top_services = sorted(by_service.items(), key=lambda x: x[1]["total"], reverse=True)

    return {
        "period": {"since": SINCE, "until": UNTIL},
        "total_incidents": len(incidents),
        "by_status": {
            "resolved": len([i for i in incidents if i["status"] == "resolved"]),
            "triggered": len([i for i in incidents if i["status"] == "triggered"]),
            "acknowledged": len([i for i in incidents if i["status"] == "acknowledged"]),
        },
        "mttr": mttr_stats,
        "triage_time": triage_stats,
        "alert_volume": {
            "by_service": [
                {"service": name, "service_id": data.get("service_id"), **{k: v for k, v in data.items() if k != "service_id"}}
                for name, data in top_services
            ],
            "by_urgency": dict(by_urgency),
            "by_month": dict(sorted(by_month.items())),
        },
        "top_incident_types": [
            {"title": t, "count": c}
            for t, c in sorted(by_title.items(), key=lambda x: x[1], reverse=True)[:20]
        ],
        "fmt": {
            "mttr_median": fmt_duration(mttr_stats["median_seconds"]),
            "mttr_mean": fmt_duration(mttr_stats["mean_seconds"]),
            "mttr_p90": fmt_duration(mttr_stats.get("p90")),
            "mttr_p99": fmt_duration(mttr_stats.get("p99")),
            "triage_median": fmt_duration(triage_stats["median_seconds"]),
            "triage_mean": fmt_duration(triage_stats["mean_seconds"]),
            "triage_p90": fmt_duration(triage_stats.get("p90")),
            "triage_p99": fmt_duration(triage_stats.get("p99")),
        },
    }


def generate_summary(metrics):
    """Generate human-readable summary report."""
    m = metrics
    f = m["fmt"]

    lines = [
        "# PagerDuty 6-Month Incident Baseline Report",
        f"**Period:** {m['period']['since'][:10]} to {m['period']['until'][:10]}",
        f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        f"**Ticket:** SPRE-5966",
        "",
        "---",
        "",
        "## Summary",
        "",
        f"- **Total incidents:** {m['total_incidents']}",
        f"- **Resolved:** {m['by_status']['resolved']}",
        f"- **Triggered (open):** {m['by_status']['triggered']}",
        f"- **Acknowledged:** {m['by_status']['acknowledged']}",
        "",
        "## MTTR (Mean Time to Resolve)",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Incidents resolved | {m['mttr']['count']} |",
        f"| Median MTTR | {f['mttr_median']} |",
        f"| Mean MTTR | {f['mttr_mean']} |",
        f"| p90 MTTR | {f['mttr_p90']} |",
        f"| p99 MTTR | {f['mttr_p99']} |",
        "",
        "## Triage Time (Time to First Acknowledge)",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Incidents with ack data | {m['triage_time']['count']} |",
        f"| Median triage time | {f['triage_median']} |",
        f"| Mean triage time | {f['triage_mean']} |",
        f"| p90 triage time | {f['triage_p90']} |",
        f"| p99 triage time | {f['triage_p99']} |",
        "",
        "## Alert Volume by Service (Top 15)",
        "",
        "| Service | Total | Resolved | Triggered | Alerts |",
        "|---------|-------|----------|-----------|--------|",
    ]

    for svc in m["alert_volume"]["by_service"][:15]:
        lines.append(
            f"| {svc['service']} | {svc['total']} | {svc.get('resolved',0)} | {svc.get('triggered',0)} | {svc['alerts']} |"
        )

    lines += [
        "",
        "## Alert Volume by Urgency",
        "",
        "| Urgency | Count |",
        "|---------|-------|",
    ]
    for urg, cnt in sorted(m["alert_volume"]["by_urgency"].items()):
        lines.append(f"| {urg} | {cnt} |")

    lines += [
        "",
        "## Incident Volume by Month",
        "",
        "| Month | Count |",
        "|-------|-------|",
    ]
    for month, cnt in sorted(m["alert_volume"]["by_month"].items()):
        lines.append(f"| {month} | {cnt} |")

    lines += [
        "",
        "## Top Incident Types",
        "",
        "| Title | Count |",
        "|-------|-------|",
    ]
    for t in m["top_incident_types"][:15]:
        lines.append(f"| {t['title']} | {t['count']} |")

    lines += [
        "",
        "---",
        "",
        "*This baseline is the foundation for measuring improvements from Agentic AI workflows.*",
    ]

    return "\n".join(lines)


def main():
    print("=" * 60)
    print("PagerDuty 6-Month Incident Baseline Extraction")
    print("=" * 60)
    print(f"Period: {SINCE[:10]} to {UNTIL[:10]}")
    print()

    print("[1/4] Fetching incidents from PagerDuty API...")
    incidents = fetch_all_incidents()
    print(f"  Total incidents fetched: {len(incidents)}")
    print()

    print("[2/4] Computing metrics (MTTR, triage time, alert volume)...")
    metrics = compute_metrics(incidents)
    print(f"  Resolved incidents (for MTTR): {metrics['mttr']['count']}")
    print(f"  Services found: {len(metrics['alert_volume']['by_service'])}")
    print()

    # Save raw incidents
    raw_path = os.path.join(OUTPUT_DIR, "pagerduty_incidents_raw.json")
    print(f"[3/4] Saving raw data to {raw_path}...")
    raw_output = {
        "metadata": {
            "period": {"since": SINCE, "until": UNTIL},
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "total_incidents": len(incidents),
        },
        "incidents": [
            {
                "incident_number": inc.get("incident_number"),
                "title": inc.get("title"),
                "status": inc.get("status"),
                "urgency": inc.get("urgency"),
                "created_at": inc.get("created_at"),
                "resolved_at": inc.get("resolved_at"),
                "last_status_change_at": inc.get("last_status_change_at"),
                "service": inc.get("service", {}).get("summary"),
                "service_id": inc.get("service", {}).get("id"),
                "alert_counts": inc.get("alert_counts"),
                "assignments": [
                    {"assignee": a.get("assignee", {}).get("summary"), "at": a.get("at")}
                    for a in inc.get("assignments", [])
                ],
                "acknowledgements": [
                    {"acknowledger": a.get("acknowledger", {}).get("summary"), "at": a.get("at")}
                    for a in inc.get("acknowledgements", [])
                ],
                "escalation_policy": inc.get("escalation_policy", {}).get("summary"),
            }
            for inc in incidents
        ],
    }
    with open(raw_path, "w") as f:
        json.dump(raw_output, f, indent=2)
    print(f"  Saved {len(incidents)} incidents")

    # Save metrics JSON
    metrics_path = os.path.join(OUTPUT_DIR, "pagerduty_baseline_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"  Metrics saved to {metrics_path}")

    # Save summary report
    summary_path = os.path.join(OUTPUT_DIR, "pagerduty_baseline_report.md")
    print(f"\n[4/4] Generating summary report...")
    summary = generate_summary(metrics)
    with open(summary_path, "w") as f:
        f.write(summary)
    print(f"  Report saved to {summary_path}")

    print()
    print("=" * 60)
    print("DONE. Files created:")
    print(f"  1. {raw_path}")
    print(f"  2. {metrics_path}")
    print(f"  3. {summary_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
