#!/usr/bin/env python3
import subprocess
import json
import math
import time
import sys
from typing import List


def get_succeeded_pipeline_runs(resource_group: str, acr_name: str, prefix: str) -> set:
    """
    Returns a set of run names that have status 'Succeeded' and match the given prefix.
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
        # Debug output removed
        succeeded = set()
        for run in runs:
            name = run.get("name", "")
            if name.startswith(prefix) and run.get("provisioningState") == "Succeeded":
                succeeded.add(name)
        return succeeded
    except Exception as e:
        print(f"Warning: Could not fetch succeeded pipeline runs: {e}", file=sys.stderr)
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

def trigger_export_pipeline(resource_group: str, acr_name: str, pipeline_name: str, artifacts: List[str], run_name: str):
    # Pass artifacts as a space-separated list, not JSON
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
    output = run_cli(cmd)
    print(f"Pipeline run {run_name} started.")
    return json.loads(output)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch ACR export pipeline runner.")
    parser.add_argument("--resource-group", required=True, help="Resource group of the ACR registry")
    parser.add_argument("--acr-name", required=True, help="Source ACR name")
    parser.add_argument("--pipeline-name", required=True, help="Export pipeline name (letters/numbers only, e.g. exportpipeline)")
    # --storage-uri argument removed (unused)
    parser.add_argument("--batch-size", type=int, default=50, help="Artifacts per batch (default: 50)")
    parser.add_argument("--prefix", default="export-batch", help="Prefix for pipeline run names")
    parser.add_argument("--dry-run", action="store_true", help="Only print batches, do not trigger pipelines")

    # --assign-identity and --options arguments removed (not valid for pipeline-run)
    args = parser.parse_args()

    all_artifacts = get_all_artifacts(args.acr_name)
    print(f"Found {len(all_artifacts)} artifacts.")
    all_artifacts.sort()  # Always sort for repeatable batches
    batches = split_batches(all_artifacts, args.batch_size)
    print(f"Splitting into {len(batches)} batches of up to {args.batch_size}.")

    # Fetch succeeded pipeline runs once at the start
    succeeded_runs = get_succeeded_pipeline_runs(args.resource_group, args.acr_name, args.prefix)
    if succeeded_runs:
        print(f"Found {len(succeeded_runs)} succeeded pipeline runs with prefix '{args.prefix}'.")

    any_batch_failed = False
    for i, batch in enumerate(batches, 1):
        run_name = f"{args.prefix}{i:03d}"
        if run_name in succeeded_runs:
            print(f"Batch {i}: {len(batch)} artifacts. Run name: {run_name}")
            print(f"  Skipping batch {i} ({run_name}): already succeeded.")
            continue
        print(f"Batch {i}: {len(batch)} artifacts. Run name: {run_name}")
        if args.dry_run:
            print(batch)
        else:
            try:
                trigger_export_pipeline(
                    args.resource_group,
                    args.acr_name,
                    args.pipeline_name,
                    batch,
                    run_name
                )
            except Exception as e:
                print(f"  Error in batch {i} ({run_name}): {e}", file=sys.stderr)
                any_batch_failed = True
        # Optional: sleep to avoid hitting concurrency limits
        time.sleep(1)
    if any_batch_failed:
        print("Some batches failed. See errors above.", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
