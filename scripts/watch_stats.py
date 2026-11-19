#!/usr/bin/env python3
"""Optionally inserts sample events, then polls /admin/stats and reports per event type:

  first pickup : first row left NEW            (dispatch latency)
  dispatched   : no NEW or CLAIMED rows left   (dispatcher throughput)
  drained      : every row is DONE or FAILED   (end-to-end, includes workflow run time)

    python3 scripts/watch_stats.py --generate INBOUND_PAYMENT:5000
    python3 scripts/watch_stats.py --generate INBOUND_PAYMENT:2000 PAYMENT_POSTED:2000 --timeout 300
    python3 scripts/watch_stats.py                      # just watch what's already there
"""
import argparse
import json
import time
import urllib.request


def call(base, path, method="GET"):
    req = urllib.request.Request(base + path, method=method)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost:8080")
    p.add_argument("--generate", nargs="*", default=[], metavar="TYPE:COUNT")
    p.add_argument("--due-within", type=int, default=90, help="seconds, for PAYMENT_SCHEDULED")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--quiet", action="store_true", help="print only the summary")
    a = p.parse_args()

    t0 = time.time()
    for spec in a.generate:
        t, n = spec.split(":")
        call(a.base, f"/events/generate?type={t}&count={n}&dueWithinSeconds={a.due_within}", "POST")
    types = [s.split(":")[0] for s in a.generate] or None
    inserted_s = time.time() - t0

    first_pickup, dispatched, drained = {}, {}, {}
    while True:
        s = call(a.base, "/admin/stats")
        events = s["events"]
        watch = types or sorted(events)
        elapsed = time.time() - t0
        lines = []
        for t in watch:
            c = events.get(t, {})
            total = sum(c.values())
            new, claimed, started = c.get("NEW", 0), c.get("CLAIMED", 0), c.get("STARTED", 0)
            if t not in first_pickup and new < total:
                first_pickup[t] = elapsed
            if t not in dispatched and total and new == 0 and claimed == 0:
                dispatched[t] = elapsed
            if t not in drained and total and new == claimed == started == 0:
                drained[t] = elapsed
            lines.append(f"{t:<18} new={new:>6} claimed={claimed:>5} started={started:>5} "
                         f"done={c.get('DONE', 0):>6} failed={c.get('FAILED', 0):>4}")
        if not a.quiet:
            bell = s.get("doorbell", {})
            print(f"[{elapsed:6.1f}s] mode={s['mode']} rings={bell.get('ringsReceived', 0)} triggers={bell.get('triggersSent', 0)}")
            for line in lines:
                print("    " + line)
        if all(t in drained for t in watch) or elapsed > a.timeout:
            break
        time.sleep(a.interval)

    fmt = lambda v: "   —  " if v is None else f"{v:6.1f}"
    print(f"\nmode={s['mode']}  (seconds from the insert request; inserting took {inserted_s:.1f}s)")
    print(f"  {'event type':<18} {'first pickup':>12} {'dispatched':>11} {'drained':>8}")
    for t in watch:
        print(f"  {t:<18} {fmt(first_pickup.get(t)):>12} {fmt(dispatched.get(t)):>11} {fmt(drained.get(t)):>8}")
    for line in lines:
        print("  " + line)


if __name__ == "__main__":
    main()
