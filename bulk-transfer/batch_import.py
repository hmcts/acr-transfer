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

def list_blobs(storage_account: str, container: str, sas_token: str = None) -> List[str]:
    cmd = ["az", "storage", "blob", "list", "--account-name", storage_account, "--container-name", container, "--output", "json"]
    if sas_token:
        cmd += ["--sas-token", sas_token]
    output = run_cli(cmd)
    blobs = json.loads(output)
    return [blob["name"] for blob in blobs]

def split_batches(items: List[str], batch_size: int) -> List[List[str]]:
    return [items[i:i+batch_size] for i in range(0, len(items), batch_size)]

def trigger_import_pipeline(resource_group: str, acr_name: str, pipeline_name: str, blobs: List[str], storage_uri: str, run_name: str):
    # The import pipeline expects a list of blob names (relative to the container)
    blobs_json = json.dumps(blobs)
    cmd = [
        "az", "acr", "pipeline-run", "create",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--pipeline", pipeline_name,
        "--name", run_name,
        "--pipeline-type", "import",
        "--storage-blob", run_name,
        "--artifacts", blobs_json,
        "--output", "json"
    ]
    print(f"Triggering import pipeline run: {run_name} with {len(blobs)} blobs...")
    output = run_cli(cmd)
    print(f"Import pipeline run {run_name} started.")
    return json.loads(output)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Batch ACR import pipeline runner.")
    parser.add_argument("--resource-group", required=True, help="Resource group of the ACR registry")
    parser.add_argument("--acr-name", required=True, help="Target ACR name")
    parser.add_argument("--pipeline-name", required=True, help="Import pipeline name (letters/numbers only, e.g. importpipeline)")
    parser.add_argument("--storage-account", required=True, help="Storage account name")
    parser.add_argument("--container", required=True, help="Blob container name")
    parser.add_argument("--storage-uri", required=True, help="Blob container SAS URI")
    parser.add_argument("--sas-token", help="SAS token for storage account (optional)")
    parser.add_argument("--subscription", help="Azure subscription ID or name")
    parser.add_argument("--batch-size", type=int, default=50, help="Blobs per batch (default: 50)")
    parser.add_argument("--prefix", default="import-batch", help="Prefix for pipeline run names")
    parser.add_argument("--dry-run", action="store_true", help="Only print batches, do not trigger pipelines")
    args = parser.parse_args()

    blobs = list_blobs(args.storage_account, args.container, args.sas_token)
    print(f"Found {len(blobs)} blobs in container {args.container}.")
    batches = split_batches(blobs, args.batch_size)
    print(f"Splitting into {len(batches)} batches of up to {args.batch_size}.")

    for i, batch in enumerate(batches, 1):
        run_name = f"{args.prefix}-{i:03d}"
        print(f"Batch {i}: {len(batch)} blobs. Run name: {run_name}")
        if args.dry_run:
            print(batch)
        else:
            trigger_import_pipeline(
                args.resource_group,
                args.acr_name,
                args.pipeline_name,
                batch,
                args.storage_uri,
                run_name
            )
        time.sleep(1)

if __name__ == "__main__":
    main()
