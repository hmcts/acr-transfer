#!/usr/bin/env python3
import subprocess
import json
import math
import time
import sys
from typing import List


def get_existing_pipeline_runs(resource_group: str, acr_name: str, prefix: str) -> set:
    """
    Returns a set of run names that exist (not Failed/Canceled) and match the given prefix.
    This includes Succeeded, Running, Creating, Pending, Updating states.
    """
    cmd = [
        "az", "acr", "pipeline-run", "list",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--output", "json"
    ]
    try:
        output = run_cli(cmd)
        runs = json.loads(output)
        existing = set()
        for run in runs:
            name = run.get("name", "")
            status = run.get("provisioningState")
            # Skip only if explicitly Failed or Canceled
            if name.startswith(prefix) and status not in ["Failed", "Canceled"]:
                existing.add(name)
        return existing
    except Exception as e:
        print(f"Warning: Could not fetch existing pipeline runs: {e}", file=sys.stderr)
        return set()

def run_cli(cmd: List[str]):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running {' '.join(cmd)}:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout

def list_repositories(acr_name: str) -> List[str]:
    cmd = ["az", "acr", "repository", "list", "--name", acr_name, "--output", "json"]
    output = run_cli(cmd)
    return json.loads(output)

def list_tags(acr_name: str, repository: str) -> List[str]:
    cmd = ["az", "acr", "repository", "show-tags", "--name", acr_name, "--repository", repository, "--output", "json"]
    output = run_cli(cmd)
    return json.loads(output)

def get_all_artifacts(acr_name: str) -> List[str]:
    import concurrent.futures
    artifacts = []
    repos = list_repositories(acr_name)
    print(f"Discovered {len(repos)} repositories in {acr_name}.")

    def fetch_valid_artifacts(repo):
        tags = set(list_tags(acr_name, repo))
        # Get all manifests for this repo
        cmd = [
            "az", "acr", "repository", "show-manifests",
            "--name", acr_name,
            "--repository", repo,
            "--output", "json"
        ]
        try:
            output = run_cli(cmd)
            manifests = json.loads(output)
        except Exception as exc:
            print(f"  {repo}: failed to fetch manifests: {exc}", file=sys.stderr)
            return []
        # Build set of valid tags from manifests
        valid_tags = set()
        for manifest in manifests:
            valid_tags.update(manifest.get("tags", []) or [])
        # Only include tags that are present in valid_tags
        valid_artifacts = []
        for tag in tags:
            if tag in valid_tags:
                valid_artifacts.append((repo, tag))
            else:
                print(f"  {repo}:{tag} skipped (no valid manifest)", file=sys.stderr)
        print(f"  {repo}: {len(valid_artifacts)}/{len(tags)} valid tags")
        return valid_artifacts

    all_artifacts = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_repo = {executor.submit(fetch_valid_artifacts, repo): repo for repo in repos}
        for idx, future in enumerate(concurrent.futures.as_completed(future_to_repo), 1):
            repo = future_to_repo[future]
            try:
                repo_artifacts = future.result()
                all_artifacts.extend(repo_artifacts)
            except Exception as exc:
                print(f"  {repo}: generated an exception: {exc}", file=sys.stderr)
            if idx % 25 == 0 or idx == len(repos):
                print(f"Processed {idx}/{len(repos)} repositories...")
    return [f"{repo}:{tag}" for repo, tag in all_artifacts]

def split_batches(items: List[str], batch_size: int) -> List[List[str]]:
    return [items[i:i+batch_size] for i in range(0, len(items), batch_size)]

def count_running_pipeline_runs(resource_group: str, acr_name: str) -> int:
    """
    Count pipeline runs that are currently in a non-terminal state.
    """
    cmd = [
        "az", "acr", "pipeline-run", "list",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--output", "json"
    ]
    try:
        output = run_cli(cmd)
        runs = json.loads(output)
        # Count runs that are not in terminal state (Succeeded, Failed, Canceled)
        running = sum(1 for run in runs if run.get("provisioningState") in ["Creating", "Running", "Pending", "Updating"])
        return running
    except Exception as e:
        print(f"Warning: Could not count running pipeline runs: {e}", file=sys.stderr)
        return 0

def wait_for_available_slot(resource_group: str, acr_name: str, max_concurrent: int = 10, poll_interval: int = 30):
    """
    Wait until there's an available slot for a new pipeline run.
    """
    while True:
        running = count_running_pipeline_runs(resource_group, acr_name)
        if running < max_concurrent:
            return
        print(f"  {running}/{max_concurrent} pipeline runs active. Waiting {poll_interval}s...")
        time.sleep(poll_interval)

def trigger_export_pipeline_async(resource_group: str, acr_name: str, pipeline_name: str, artifacts: List[str], run_name: str):
    """
    Trigger export pipeline asynchronously without blocking.
    Returns a subprocess.Popen object that can be checked later.
    """
    cmd = [
        "az", "acr", "pipeline-run", "create",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--pipeline", pipeline_name,
        "--name", run_name,
        "--pipeline-type", "export",
        "--storage-blob", run_name,
        "--output", "json",
        "--artifacts"
    ] + artifacts
    print(f"Triggering pipeline run: {run_name} with {len(artifacts)} artifacts...")
    # Start process without waiting for it to complete
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    return process

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch ACR export pipeline runner.")
    parser.add_argument("--resource-group", required=True, help="Resource group of the ACR registry")
    parser.add_argument("--acr-name", required=True, help="Source ACR name")
    parser.add_argument("--pipeline-name", required=True, help="Export pipeline name (letters/numbers only, e.g. exportpipeline)")
    parser.add_argument("--batch-size", type=int, default=50, help="Artifacts per batch (default: 50)")
    parser.add_argument("--prefix", default="export-batch", help="Prefix for pipeline run names")
    parser.add_argument("--dry-run", action="store_true", help="Only print batches, do not trigger pipelines")
    parser.add_argument("--ignore-tags", type=str, default=None, help="Path to a JSON file with a list of {repository, tag} objects to ignore.")
    parser.add_argument("--max-concurrent", type=int, default=10, help="Maximum number of concurrent pipeline runs (default: 10)")

    args = parser.parse_args()

    ignore_tags = set()
    ignore_repos = set()
    if args.ignore_tags:
        try:
            with open(args.ignore_tags, "r", encoding="utf-8") as f:
                ignore_list = json.load(f)
            for entry in ignore_list:
                repo = entry.get("repository")
                tag = entry.get("tag", None)
                # If only repo is specified, ignore all tags in that repo
                if repo and (tag is None or tag == ""):
                    ignore_repos.add(repo)
                elif repo and tag:
                    ignore_tags.add((repo, tag))
        except Exception as e:
            print(f"Warning: Could not load ignore-tags file: {e}", file=sys.stderr)

    all_artifacts = get_all_artifacts(args.acr_name)
    print(f"Found {len(all_artifacts)} artifacts.")
    # Filter out ignored tags and repos
    if ignore_tags or ignore_repos:
        before = len(all_artifacts)
        def not_ignored(artifact):
            repo, tag = artifact.split(":", 1)
            if repo in ignore_repos:
                return False
            if (repo, tag) in ignore_tags:
                return False
            return True
        all_artifacts = [a for a in all_artifacts if not_ignored(a)]
        print(f"Filtered out {before - len(all_artifacts)} artifacts using ignore-tags and ignore-repos.")
    all_artifacts.sort()  # Always sort for repeatable batches
    batches = split_batches(all_artifacts, args.batch_size)
    print(f"Splitting into {len(batches)} batches of up to {args.batch_size}.")

    # Fetch existing pipeline runs once at the start (skip Succeeded and Running)
    existing_runs = get_existing_pipeline_runs(args.resource_group, args.acr_name, args.prefix)
    if existing_runs:
        print(f"Found {len(existing_runs)} existing pipeline runs with prefix '{args.prefix}' (will skip these).")

    any_batch_failed = False
    triggered_runs = []  # Track pipeline run names we've triggered
    create_processes = {}  # Track az command processes: {run_name: process}

    for i, batch in enumerate(batches, 1):
        run_name = f"{args.prefix}{i:03d}"
        if run_name in existing_runs:
            print(f"Batch {i}: {len(batch)} artifacts. Run name: {run_name}")
            print(f"  Skipping batch {i} ({run_name}): already exists (Succeeded/Running/Pending).")
            continue
        print(f"Batch {i}: {len(batch)} artifacts. Run name: {run_name}")
        if args.dry_run:
            print(batch)
        else:
            # Wait for an available slot before triggering
            wait_for_available_slot(args.resource_group, args.acr_name, args.max_concurrent)
            process = trigger_export_pipeline_async(
                args.resource_group,
                args.acr_name,
                args.pipeline_name,
                batch,
                run_name
            )
            create_processes[run_name] = process
            triggered_runs.append(run_name)
        # Small delay to avoid overwhelming the API
        time.sleep(2)

    # Wait for all az command processes to complete (just the command, not the pipeline execution)
    if create_processes:
        print(f"\nWaiting for all {len(create_processes)} 'az' commands to complete...")
        for run_name, process in create_processes.items():
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                print(f"  Failed to create {run_name}: {stderr}", file=sys.stderr)
                any_batch_failed = True
                triggered_runs.remove(run_name)
            else:
                print(f"  {run_name} creation command completed.")

    # Wait for all triggered pipeline runs to complete in Azure
    if triggered_runs and not args.dry_run:
        print(f"\nAll {len(triggered_runs)} batches triggered. Waiting for Azure pipelines to complete...")
        print(f"Triggered runs: {', '.join(triggered_runs)}")
        poll_interval = 30
        while True:
            # Check status of all our triggered runs
            cmd = [
                "az", "acr", "pipeline-run", "list",
                "--resource-group", args.resource_group,
                "--registry", args.acr_name,
                "--output", "json"
            ]
            try:
                output = run_cli(cmd)
                all_runs = json.loads(output)

                # Filter to only our triggered runs
                our_runs = [r for r in all_runs if r.get("name") in triggered_runs]

                # Count by status
                pending = sum(1 for r in our_runs if r.get("provisioningState") in ["Running", "Creating", "Pending", "Updating"])
                succeeded = sum(1 for r in our_runs if r.get("provisioningState") == "Succeeded")
                failed = sum(1 for r in our_runs if r.get("provisioningState") == "Failed")
                canceled = sum(1 for r in our_runs if r.get("provisioningState") == "Canceled")

                print(f"  Status: {pending} running, {succeeded} succeeded, {failed} failed, {canceled} canceled (total: {len(our_runs)}/{len(triggered_runs)})")

                if pending == 0:
                    # All done
                    print(f"\nAll pipeline runs completed!")
                    print(f"  Succeeded: {succeeded}")
                    print(f"  Failed: {failed}")
                    print(f"  Canceled: {canceled}")
                    if failed > 0 or canceled > 0:
                        any_batch_failed = True
                    break
                else:
                    print(f"  Waiting {poll_interval}s before next check...")
                    time.sleep(poll_interval)
            except Exception as e:
                print(f"  Error checking pipeline status: {e}", file=sys.stderr)
                time.sleep(poll_interval)

    if any_batch_failed:
        print("\nSome batches failed. See errors above.", file=sys.stderr)
        sys.exit(1)
    else:
        print(f"\nAll batches completed successfully!")

if __name__ == "__main__":
    main()
