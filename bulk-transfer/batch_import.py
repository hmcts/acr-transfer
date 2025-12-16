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

def list_blobs(storage_account: str, container: str, sas_token: str = None, subscription: str = None) -> List[str]:
    cmd = ["az", "storage", "blob", "list", "--account-name", storage_account, "--container-name", container, "--output", "json"]
    if sas_token:
        cmd += ["--sas-token", sas_token]
    elif subscription:
        cmd += ["--subscription", subscription]
    output = run_cli(cmd)
    blobs = json.loads(output)
    return [blob["name"] for blob in blobs]


def trigger_import_pipeline(resource_group: str, acr_name: str, pipeline_name: str, blobs: List[str], run_name: str):
    # The import pipeline expects a single blob name (relative to the container)
    blob_name = blobs[0]
    cmd = [
        "az", "acr", "pipeline-run", "create",
        "--resource-group", resource_group,
        "--registry", acr_name,
        "--pipeline", pipeline_name,
        "--name", run_name,
        "--pipeline-type", "import",
        "--storage-blob", blob_name,
        "--output", "json"
    ]
    print(f"Triggering import pipeline run: {run_name} for blob: {blob_name}")
    output = run_cli(cmd)
    print(f"Import pipeline run {run_name} started.")
    return json.loads(output)

# Attach subscription as a static attribute for use in trigger_import_pipeline
trigger_import_pipeline.subscription = None

def main():
    import argparse
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

    args = parser.parse_args()

    blobs = list_blobs(args.storage_account, args.container, args.sas_token, args.subscription)
    if not blobs:
        print(f"No blobs found in container {args.container}.")
        sys.exit(0)
    print(f"Found {len(blobs)} blobs in container {args.container}.")

    for i, blob in enumerate(blobs, 1):
        run_name = f"{args.prefix}{i:03d}"
        print(f"Blob {i}: {blob}. Run name: {run_name}")
        if args.dry_run:
            print(f"[DRY RUN] Would import blob: {blob}")
        else:
            trigger_import_pipeline(
                args.resource_group,
                args.acr_name,
                args.pipeline_name,
                [blob],
                run_name
            )
        time.sleep(1)

if __name__ == "__main__":
    main()
