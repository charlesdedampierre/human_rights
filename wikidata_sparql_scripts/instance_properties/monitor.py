#!/usr/bin/env python3
"""
Monitor extraction progress.

Usage:
    python monitor.py          # Show current status
    python monitor.py --watch  # Auto-refresh every 10s
    python monitor.py --errors # Show recent errors
"""

import json
import argparse
import time
from pathlib import Path

STATUS_FILE = "output/status.json"
ERRORS_FILE = "output/errors.json"


def load_status():
    """Load status from JSON file."""
    try:
        with open(STATUS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError:
        return {"error": "Status file corrupted"}


def load_errors():
    """Load errors from JSON file."""
    try:
        with open(ERRORS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def format_status(status):
    """Format status for display."""
    if not status:
        print("No status file found. Extraction may not have started.")
        return

    if "error" in status:
        print(f"Error: {status['error']}")
        return

    print("=" * 60)
    print(f"  WIKIDATA EXTRACTION STATUS")
    print("=" * 60)

    # Status
    st = status.get("status", "unknown")
    status_icon = "🟢" if st == "running" else "✅" if st == "completed" else "🔴"
    print(f"\n  Status: {status_icon} {st.upper()}")

    # Progress
    prog = status.get("progress", {})
    completed = prog.get("completed_items", 0)
    total = prog.get("total_items", 0)
    percent = prog.get("percent_complete", 0)

    print(f"\n  Progress:")
    print(f"    Items:   {completed:,} / {total:,} ({percent:.1f}%)")
    print(f"    Batches: {prog.get('completed_batches', 0)} / {prog.get('total_batches', 0)}")
    print(f"    Failed:  {prog.get('failed_batches', 0)}")

    # Progress bar
    bar_width = 40
    filled = int(bar_width * percent / 100) if total > 0 else 0
    bar = "█" * filled + "░" * (bar_width - filled)
    print(f"\n    [{bar}] {percent:.1f}%")

    # Performance
    perf = status.get("performance", {})
    print(f"\n  Performance:")
    print(f"    Elapsed:     {perf.get('elapsed_human', perf.get('total_time_human', 'N/A'))}")
    print(f"    Rate:        {perf.get('items_per_second', 0):.2f} items/s")
    print(f"    Hourly:      {perf.get('items_per_hour', 0):,.0f} items/h")

    # ETA
    eta = status.get("eta")
    if eta and st == "running":
        print(f"\n  ETA: {eta}")

    # System
    sys_stats = status.get("system", {})
    if sys_stats:
        print(f"\n  System:")
        if "memory_percent" in sys_stats:
            print(f"    Memory:  {sys_stats.get('memory_used_gb', 0):.1f} GB ({sys_stats.get('memory_percent', 0):.1f}%)")
        if "disk_free_gb" in sys_stats:
            print(f"    Disk:    {sys_stats.get('disk_free_gb', 0):.1f} GB free ({sys_stats.get('disk_percent_used', 0):.1f}% used)")

    # Timestamps
    print(f"\n  Started:  {status.get('started_at', 'N/A')}")
    print(f"  Updated:  {status.get('last_updated', status.get('completed_at', 'N/A'))}")

    # Errors
    error_count = status.get("error_count", 0)
    if error_count > 0:
        print(f"\n  ⚠️  {error_count} errors recorded (run with --errors to see)")

    print("\n" + "=" * 60)


def show_errors():
    """Show recent errors."""
    errors = load_errors()
    if not errors:
        print("No errors file found.")
        return

    print("=" * 60)
    print(f"  EXTRACTION ERRORS ({errors.get('total_errors', 0)} total)")
    print("=" * 60)

    for err in errors.get("errors", [])[-10:]:  # Show last 10
        print(f"\n  Batch {err.get('batch_num', '?')} @ {err.get('timestamp', 'unknown')}")
        print(f"    Error: {err.get('error', 'unknown')}")
        if err.get("batch_ids"):
            print(f"    IDs: {', '.join(err['batch_ids'][:3])}...")


def main():
    parser = argparse.ArgumentParser(description="Monitor extraction progress")
    parser.add_argument("--watch", "-w", action="store_true", help="Auto-refresh every 10s")
    parser.add_argument("--errors", "-e", action="store_true", help="Show recent errors")
    args = parser.parse_args()

    if args.errors:
        show_errors()
        return

    if args.watch:
        try:
            while True:
                print("\033[2J\033[H")  # Clear screen
                format_status(load_status())
                print("\n  Press Ctrl+C to exit")
                time.sleep(10)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        format_status(load_status())


if __name__ == "__main__":
    main()
