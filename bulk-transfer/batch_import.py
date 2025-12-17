#!/usr/bin/env python3
import subprocess
import json
import math
import time
import sys
from typing import List


def run_cli(cmd: List[str]):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {' '.join(cmd)}:\n{result.stderr}", file=sys.stderr)
        return None
    return result.stdout

def run_cli_async(cmd: List[str]):
    # Launch command asynchronously, return Popen object
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

def list_blobs(storage_account: str, container: str, sas_token: str = None, subscription: str = None) -> List[str]:
    cmd = ["az", "storage", "blob", "list", "--account-name", storage_account, "--container-name", container, "--output", "json"]
    if sas_token:
        cmd += ["--sas-token", sas_token]
    elif subscription:
        cmd += ["--subscription", subscription]
    output = run_cli(cmd)
    blobs = json.loads(output)
    return [blob["name"] for blob in blobs]


def trigger_import_pipeline_async(resource_group: str, acr_name: str, pipeline_name: str, blob: str, run_name: str):
    cmd = [
        "az", "acr", "pipeline-run", "create",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--pipeline", pipeline_name,
        "--name", run_name,
        "--pipeline-type", "import",
        "--storage-blob", blob,
        "--output", "json"
    ]
    print(f"Triggering import pipeline run: {run_name} for blob: {blob}")
    return run_cli_async(cmd)
def get_existing_pipeline_runs(resource_group: str, acr_name: str, prefix: str) -> dict:
    # Returns a dict of {run_name: provisioningState}
    cmd = [
        "az", "acr", "pipeline-run", "list",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--output", "json"
    ]
    output = run_cli(cmd)
    runs = json.loads(output) if output else []
    result = {}
    for run in runs:
        name = run.get("name", "")
        state = run.get("provisioningState", "")
        if name.startswith(prefix):
            result[name] = state
    return result

def get_pipeline_run_status(resource_group: str, acr_name: str, run_name: str) -> str:
    cmd = [
        "az", "acr", "pipeline-run", "show",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--name", run_name,
        "--output", "json"
    ]
    output = run_cli(cmd)
    if not output:
        return "Unknown"
    run = json.loads(output)
    return run.get("provisioningState", "Unknown")

# Attach subscription as a static attribute for use in trigger_import_pipeline

def main():

    import argparse
    import threading
    parser = argparse.ArgumentParser(description="Batch ACR import pipeline runner.")
    parser.add_argument("--resource-group", required=True, help="Resource group of the ACR registry")
    parser.add_argument("--acr-name", required=True, help="Target ACR name")
    parser.add_argument("--pipeline-name", required=True, help="Import pipeline name (letters/numbers only, e.g. importpipeline)")
    parser.add_argument("--storage-account", required=True, help="Storage account name for blob container")
    parser.add_argument("--container", required=True, help="Blob container name")
    parser.add_argument("--sas-token", help="SAS token for storage account (optional)")
    parser.add_argument("--subscription", help="Azure subscription ID or name (used only if no SAS token is provided)")
    parser.add_argument("--prefix", default="import-batch", help="Prefix for pipeline run names")
    parser.add_argument("--dry-run", action="store_true", help="Only print batches, do not trigger pipelines")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Maximum number of concurrent import pipeline runs")

    args = parser.parse_args()

    blobs = list_blobs(args.storage_account, args.container, args.sas_token, args.subscription)
    if not blobs:
        print(f"No blobs found in container {args.container}.")
        sys.exit(0)
    print(f"Found {len(blobs)} blobs in container {args.container}.")

    # Get existing pipeline runs to skip already succeeded blobs
    existing_runs = get_existing_pipeline_runs(args.resource_group, args.acr_name, args.prefix)
    succeeded_runs = {name for name, status in existing_runs.items() if status.lower() == "succeeded"}
    print(f"Found {len(succeeded_runs)} succeeded pipeline runs with prefix '{args.prefix}'.")

    # Prepare jobs: skip blobs with succeeded runs
    jobs = []
    for i, blob in enumerate(blobs, 1):
        run_name = f"{args.prefix}{i:03d}"
        if run_name in succeeded_runs:
            print(f"Skipping blob {blob} (run {run_name}) - already succeeded.")
            continue
        jobs.append((blob, run_name))

    print(f"{len(jobs)} blobs to import after skipping succeeded runs.")

    if args.dry_run:
        for blob, run_name in jobs:
            print(f"[DRY RUN] Would import blob: {blob} as run: {run_name}")
        return

    import random
    max_concurrent = max(1, args.max_concurrent)
    pending = list(jobs)
    completed = set()
    failed = set()
    poll_interval = 10

    print(f"Starting up to {max_concurrent} concurrent import jobs...")

    # Submit jobs up to max_concurrent in each loop, matching export script burst pattern
    import random
    create_processes = {}  # {run_name: Popen}
    triggered_runs = []
    while pending:
        # Refresh all pipeline runs with the prefix
        all_runs = get_existing_pipeline_runs(args.resource_group, args.acr_name, args.prefix)
        # Count running jobs (not terminal states)
        running_jobs = {name: state for name, state in all_runs.items() if state and state.lower() not in ("succeeded", "failed", "cancelled", "canceled", "timedout")}

        # Mark completed/failed jobs
        for name, state in all_runs.items():
            if not state or state.strip() == "":
                print(f"[warning] Run {name} has empty provisioningState, treating as terminal.")
                completed.add(name)
            elif state.lower() == "succeeded":
                completed.add(name)
            elif state.lower() in ("failed", "cancelled", "canceled", "timedout"):
                failed.add(name)

        # If all jobs are succeeded, break
        if not running_jobs and not pending:
            break


        # Use async submission for true concurrency
        slots_available = max_concurrent - len(running_jobs)
        jobs_to_start = min(slots_available, len(pending))
        started = 0
        i = 0
        while started < jobs_to_start and i < len(pending):
            blob, run_name = pending[i]
            existing_state = all_runs.get(run_name)
            if existing_state and existing_state.lower() not in ("failed", "cancelled", "canceled", "timedout"):
                print(f"Skipping {run_name} (provisioningState: {existing_state}) - already running or succeeded.")
                pending.pop(i)
                continue
            print(f"Submitting import pipeline run: {run_name} for blob: {blob}")
            proc = trigger_import_pipeline_async(
                args.resource_group,
                args.acr_name,
                args.pipeline_name,
                blob,
                run_name
            )
            create_processes[run_name] = proc
            triggered_runs.append(run_name)
            pending.pop(i)
            started += 1
            # Random sleep to avoid thundering herd
            time.sleep(random.uniform(0.5, 2.0))

        if len(pending) > 0:
            print(f"{len(running_jobs) + started} jobs still running. Waiting {poll_interval}s...")
            time.sleep(poll_interval)

    # Wait for all az command processes to complete (just the command, not the pipeline execution)
    if create_processes:
        print(f"\nWaiting for all {len(create_processes)} 'az' commands to complete...")
        for run_name, proc in create_processes.items():
            stdout, stderr = proc.communicate()
            if proc.returncode != 0:
                print(f"  Failed to create {run_name}: {stderr}", file=sys.stderr)
                triggered_runs.remove(run_name)
            else:
                print(f"  {run_name} creation command completed.")

    # Poll for completion of all triggered runs, robustly matching export script
    if triggered_runs:
        print(f"\nAll {len(triggered_runs)} jobs triggered. Waiting for Azure pipelines to complete...")
        poll_interval = 30
        while True:
            all_runs = get_existing_pipeline_runs(args.resource_group, args.acr_name, args.prefix)
            # Only consider runs we triggered
            our_runs = [r for r in triggered_runs if r in all_runs]
            # Defensive: handle missing or empty state
            def state_of(r):
                s = all_runs.get(r, "").lower() if all_runs.get(r) else ""
                return s
            pending_count = sum(1 for r in our_runs if state_of(r) not in ("succeeded", "failed", "cancelled", "canceled", "timedout") and state_of(r) != "")
            succeeded_count = sum(1 for r in our_runs if state_of(r) == "succeeded")
            failed_count = sum(1 for r in our_runs if state_of(r) in ("failed", "cancelled", "canceled", "timedout"))
            unknown_count = sum(1 for r in our_runs if state_of(r) == "")
            print(f"  Status: {pending_count} running, {succeeded_count} succeeded, {failed_count} failed, {unknown_count} unknown (total: {len(our_runs)}/{len(triggered_runs)})")
            if pending_count == 0 and unknown_count == 0:
                print(f"\nAll pipeline runs completed!")
                print(f"  Succeeded: {succeeded_count}")
                print(f"  Failed: {failed_count}")
                break
            else:
                print(f"  Waiting {poll_interval}s before next check...")
                time.sleep(poll_interval)

    # Final status check for all jobs
    all_runs = get_existing_pipeline_runs(args.resource_group, args.acr_name, args.prefix)
    for name, status in all_runs.items():
        if status.lower() == "succeeded":
            completed.add(name)
        elif status.lower() in ("failed", "cancelled", "canceled", "timedout"):
            failed.add(name)
    print(f"All import jobs complete. {len(completed)} succeeded, {len(failed)} failed.")

if __name__ == "__main__":
    main()
