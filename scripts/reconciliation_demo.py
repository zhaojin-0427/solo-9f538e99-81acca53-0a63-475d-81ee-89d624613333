#!/usr/bin/env python3
"""End-to-end demo for the nightly manifest reconciliation API.

Submits a manifest that mixes matching rows, value mismatches (including
explicit NULL vs missing-field semantics), an analyzer-only id and then shows
the paginated diff report.  A second run demonstrates that a committed write
during reconciliation aborts publication, and that a write after publication
only marks the old report stale.

Usage::

    python scripts/reconciliation_demo.py --api http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request


def call(method: str, url: str, payload: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


def wait_terminal(api: str, report_id: str, timeout: float = 60.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        _, body = call("GET", f"{api}/reconciliations/{report_id}")
        report = body["report"]
        if report["status"] in {"published", "aborted", "failed"}:
            return report
        time.sleep(0.2)
    raise SystemExit(f"report {report_id} never finished")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    print("1) submit a manifest (matching + mismatch + analyzer-only id)")
    manifest = {
        "entries": [
            {"sample_id": 1, "temperature": 36.5, "ph": 7.2},  # exact match
            {"sample_id": 3, "temperature": None, "ph": 7.05},  # explicit NULL
            {"sample_id": 4, "temperature": 36.9},  # ph omitted: cannot mismatch
            {"sample_id": 5, "temperature": 37.1, "ph": 1.0},  # ph mismatch
            {"sample_id": 99001, "temperature": 35.9, "ph": 7.0},  # manifest only
        ]
    }
    status, body = call("POST", f"{args.api}/reconciliations", manifest)
    print(f"   HTTP {status}")
    report_id = body["report"]["report_id"]
    report = wait_terminal(args.api, report_id)
    print("   status:", report["status"], "source:", report["source"])
    print("   diff counts:", json.dumps(report["diff_counts"]))

    print("2) page through all diffs")
    cursor = None
    while True:
        url = f"{args.api}/reconciliations/{report_id}/diffs?limit=3"
        if cursor is not None:
            url += f"&after_id={cursor}"
        _, page = call("GET", url)
        for item in page["items"]:
            print(
                "   ",
                item["diff_type"],
                item["sample_id"],
                "manifest=",
                json.dumps(item["manifest"], ensure_ascii=False),
                "database=",
                json.dumps(item["database"], ensure_ascii=False),
            )
        if not page["has_more"]:
            break
        cursor = page["next_after_id"]

    print("3) filter one diff class")
    _, page = call(
        "GET",
        f"{args.api}/reconciliations/{report_id}/diffs"
        "?diff_type=value_mismatch&limit=10",
    )
    for item in page["items"]:
        print("   ", item["sample_id"], item["diff_type"])

    print("4) a later committed write only marks the report stale")
    input(
        "   press Enter after committing a write to samples"
        " (e.g. docker compose exec legacy-acquisition python -m legacy.acquisition"
        " --db /app/data/lab.db update 1 --temperature 37.0), or Ctrl-C to stop"
    )
    _, body = call("GET", f"{args.api}/reconciliations/{report_id}")
    report = body["report"]
    print(
        "   status still:",
        report["status"],
        "| stale:",
        report["stale"],
        "| current_revision:",
        report["current_revision"],
        "| published_revision:",
        report["published_revision"],
    )
    _, page = call("GET", f"{args.api}/reconciliations/{report_id}/diffs?limit=2")
    print("   old content still readable, page count:", page["count"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
