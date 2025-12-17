# Bulk ACR Transfer

This folder contains scripts and an Azure DevOps pipeline for performing large-scale, batched migrations of container images and Helm charts between Azure Container Registries (ACR) using ACR Transfer Pipelines.

## Overview
- **batch_export.py**: Automates batching and triggering of ACR export pipeline runs, exporting up to 50 artifacts per run to Azure Blob Storage.
- **batch_import.py**: Automates batching and triggering of ACR import pipeline runs, importing up to 50 blobs per run from Azure Blob Storage into the target ACR.
- **azure-pipeline.yaml**: Azure DevOps pipeline that orchestrates the creation of pipelines and the execution of batch export/import scripts using a service connection for authentication.

## Prerequisites
- Python 3.7+
- Azure CLI installed (with `acrtransfer` extension)
- Access to source and target ACRs, Key Vaults, and Blob Storage
- Azure DevOps service connection with permissions to all resources
- Export and import pipelines must be created once per registry (the pipeline handles this in `create` mode)

## How It Works
1. **Export**
   - Lists all repositories and tags in the source ACR
   - Batches artifacts into groups of 50
   - Triggers an export pipeline run for each batch (fire-and-forget)
   - Each artifact is exported as a blob (tarball) to the specified storage container
2. **Import**
   - Lists all blobs in the storage container
   - Batches blobs into groups of 50
   - Triggers an import pipeline run for each batch
   - Each blob is imported into the target ACR

## Usage
### Azure DevOps Pipeline
- Edit `azure-pipeline.yaml` to set your resource names and service connection.
- Run the pipeline with parameters:
  - `operation`: `export` or `import`
  - `mode`: `create` (to create the pipeline) or `migrate` (to run batch transfer)
- The pipeline will:
  - Create the export/import pipeline if needed
  - Run the batch export/import script inside an `AzureCLI@2` task for authentication

### Scripts (Standalone)
You can also run the scripts locally for testing:

#### Export
```sh
python3 batch_export.py \
  --acr-name <SOURCE_ACR> \
  --pipeline-name <EXPORT_PIPELINE_NAME> \
  --batch-size 50 \
  --prefix export-batch \
  [--ignore-tags ignore-tags.json]
```

#### Import
```sh
python3 batch_import.py \
  --acr-name <TARGET_ACR> \
  --pipeline-name <IMPORT_PIPELINE_NAME> \
  --storage-account <STORAGE_ACCOUNT> \
  --container <CONTAINER_NAME> \
  --storage-uri <BLOB_CONTAINER_SAS_URI> \
  --batch-size 50 \
  --prefix import-batch
```

Add `--dry-run` to preview batches without triggering pipelines.

## Authentication
- When run in Azure DevOps, authentication is handled by the service connection via `AzureCLI@2`.
- When run locally, ensure you are logged in to Azure CLI with access to all required resources.

## Monitoring
- Pipeline runs are submitted asynchronously (fire-and-forget).
- Monitor progress in the Azure Portal under your ACR resource (look for **Tasks**, **Task runs**, or use the **Activity log**).
- You can also use the Azure CLI:
  ```sh
  az acr pipeline-run list --registry <ACR_NAME> --output table
  ```

### Live Progress Table (Recommended)
To watch export/import progress in your terminal, use:

```sh
watch -n 2 'az acr pipeline-run list --resource-group <RESOURCE_GROUP> --registry <ACR_NAME> --output json 2>/dev/null | jq -r '\''["NAME","PROGRESS","STATUS"], (.[] | [.name, .response.progress.percentage, .response.status]) | @tsv'\'' | column -t'
```

This will show a live-updating table of pipeline run progress, with warnings suppressed.
## Ignoring Specific Tags

You can skip specific tags from all batches by providing an `ignore-tags.json` file:

```json
[
  {"repository": "repo1", "tag": "badtag1"},
  {"repository": "repo2", "tag": "badtag2"}
]
```

Then run:

```sh
python3 batch_export.py --acr-name <SOURCE_ACR> --pipeline-name <EXPORT_PIPELINE_NAME> --ignore-tags ignore-tags.json
```

## Notes
- Each artifact or blob is processed as a separate pipeline run batch (max 50 per run).
- Ensure you do not exceed ACR pipeline concurrency limits (default is 10 parallel runs).
- Clean up blobs in storage after migration if needed.

---
For questions or improvements, please contact the platform team.
