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
    artifacts = []
    repos = list_repositories(acr_name)
    for repo in repos:
        tags = list_tags(acr_name, repo)
        for tag in tags:
            artifacts.append(f"{repo}:{tag}")
    return artifacts

def split_batches(items: List[str], batch_size: int) -> List[List[str]]:
    return [items[i:i+batch_size] for i in range(0, len(items), batch_size)]

def trigger_export_pipeline(resource_group: str, acr_name: str, pipeline_name: str, artifacts: List[str], storage_uri: str, run_name: str):
    artifacts_json = json.dumps(artifacts)
    cmd = [
        "az", "acr", "pipeline-run", "create",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--pipeline", pipeline_name,
        "--name", run_name,
        "--pipeline-type", "export",
        "--storage-blob", run_name,
        "--artifacts", artifacts_json,
        "--output", "json"
    ]
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
    parser.add_argument("--storage-uri", required=True, help="Destination blob storage URI")
    parser.add_argument("--batch-size", type=int, default=50, help="Artifacts per batch (default: 50)")
    parser.add_argument("--prefix", default="export-batch", help="Prefix for pipeline run names")
    parser.add_argument("--dry-run", action="store_true", help="Only print batches, do not trigger pipelines")
    args = parser.parse_args()

    all_artifacts = get_all_artifacts(args.acr_name)
    print(f"Found {len(all_artifacts)} artifacts.")
    batches = split_batches(all_artifacts, args.batch_size)
    print(f"Splitting into {len(batches)} batches of up to {args.batch_size}.")

    for i, batch in enumerate(batches, 1):
        run_name = f"{args.prefix}-{i:03d}"
        print(f"Batch {i}: {len(batch)} artifacts. Run name: {run_name}")
        if args.dry_run:
            print(batch)
        else:
            trigger_export_pipeline(
                args.resource_group,
                args.acr_name,
                args.pipeline_name,
                batch,
                args.storage_uri,
                run_name
            )
        # Optional: sleep to avoid hitting concurrency limits
        time.sleep(1)

if __name__ == "__main__":
    main()
