# Azure Container Registry Transfer Utility

## Overview

`acr_transfer.py` is a Python utility to copy repositories, container images, and Helm charts between Azure Container Registries (ACR). It supports filtering, dry-run, force overwrite, and advanced ignore patterns (including regex).

### Key Features

- **Digest-Based Verification**: Compares manifest digests (SHA256 hashes) to ensure artifacts are identical, not just tag names
- **Re-tag Detection**: Automatically detects when tags have been re-pointed to different images
- **Smart Migration**: Only transfers tags that are missing or have different underlying images
- **Cross-Subscription Support**: Migrate between ACRs in different Azure subscriptions
- **Parallel Imports**: Speed up transfers with concurrent import operations
- **Flexible Filtering**: Filter by repository name patterns, letters, or regex

### How Artifact Comparison Works

The utility performs **digest-based comparison** rather than simple tag name matching:

1. **Fetches Manifest Digests**: For each repository, retrieves the SHA256 digest for every tag in both source and target registries
2. **Compares Digests**: A tag is migrated if:
   - The tag doesn't exist in the target registry, OR
   - The tag exists but points to a different manifest digest (re-tagged image)
3. **Skips Identical Artifacts**: Tags with matching digests are skipped (already synchronized)

**Example Scenarios:**
- Source has `myapp:1.0.0` → `sha256:abc123`, Target has `myapp:1.0.0` → `sha256:abc123` ✅ **Skipped** (identical)
- Source has `myapp:1.0.0` → `sha256:abc123`, Target has `myapp:1.0.0` → `sha256:def456` ⚠️ **Migrated** (re-tagged)
- Source has `myapp:2.0.0` → `sha256:xyz789`, Target doesn't have `myapp:2.0.0` ⬆️ **Migrated** (new tag)

This ensures you're migrating the **actual image content**, not just tag labels.

## Prerequisites

- Python 3.8+
- Azure CLI (`az`) installed and authenticated
- Permissions to read/write to both source and target ACRs

## Usage

```sh
python3 scripts/acr_transfer.py \
  --source-registry-name <SOURCE_ACR_NAME> \
  --target-registry-name <TARGET_ACR_NAME> \
  [--repository <REPO_NAME>] \
  [--letters <FILTER>] \
  [--ignore-pattern <PATTERN>] \
  [--ignore-config <PATH_TO_IGNORE_CONFIG>] \
  [--max-repositories <N>] \
  [--delay-seconds <SECONDS>] \
  [--dry-run] \
  [--force]
```

### Arguments

- `--source-registry-name` (required): Name of the source Azure Container Registry.
- `--target-registry-name` (required): Name of the target Azure Container Registry.
- `--repository`: Migrate a single repository (overrides letter filter).
- `--letters`: Comma-separated list of letters or ranges (e.g. `a-c,e,g`) to filter repositories by name.
- `--ignore-pattern`: Glob-style pattern(s) to exclude repositories (can be specified multiple times or as a comma-separated list).
- `--ignore-config`: Path to a JSON file containing ignore patterns (see below).
- `--max-repositories`: Limit the number of repositories processed in this run.
- `--delay-seconds`: Delay (in seconds) between imports to avoid overloading the service.
- `--dry-run`: Report planned actions without importing artifacts.
- `--force`: Overwrite existing tags in the target registry, even if digests match.

### Force Mode Behavior

- **Without `--force`**: Only migrates tags that are missing or have different digests (smart sync)
- **With `--force`**: Migrates ALL tags from source to target, overwriting even if digests are identical

Use `--force` when:
- You want to ensure all tags are re-imported regardless of content
- Target registry may have corrupted manifests
- You need to reset target registry to exactly match source

## Ignore Patterns

You can exclude repositories using glob patterns or regular expressions.

### Glob Patterns

- `"myRepo/*/*"`: Matches any repo under `myRepo/` with two subfolders.
- `"myRepo/*"`: Matches any repo under `myRepo/`.
- `"*repo/*"`: Matches any repo ending with `repo/`.

### Regex Patterns

Prefix with `re:`. Example:
- `"re:^myRepo/([^/]+)/\\1$"`: Matches repos like `myRepo/foo/foo` (second and third segments identical).

### ignore-config.json Example

```json
[
  "myRepo/*",
  "myRepoWithSubFolders/*/*",
  "re:^myRepo/([^/]+)/\\1$"
]
```

## Pipeline Integration

To use in a CI/CD pipeline (e.g. Azure DevOps, GitHub Actions):

1. Ensure Python and Azure CLI are installed.
2. Authenticate to Azure (`az login` or service principal).
3. Run the script with required arguments.
4. Supply `ignore-config.json` as an artifact or repository file.

### Example Pipeline Step

```yaml
- script: |
    python3 scripts/acr_transfer.py \
      --source-registry-name $(SOURCE_ACR) \
      --target-registry-name $(TARGET_ACR) \
      --letters a-c \
      --ignore-config ignore-repos.json \
      --max-repositories 10 \
      --dry-run
  displayName: 'Run ACR Transfer'
```

## Cross-Subscription Support

This utility now supports migrating artifacts between ACRs in different Azure subscriptions.

### New Required Arguments

- `--source-subscription-id`: Azure subscription ID for the source ACR.
- `--target-subscription-id`: Azure subscription ID for the target ACR.

These arguments ensure the script can authenticate and query the correct registry resource IDs for cross-subscription imports.

### Example Usage

```sh
python3 scripts/acr_transfer.py \
  --source-registry-name <SOURCE_ACR_NAME> \
  --target-registry-name <TARGET_ACR_NAME> \
  --source-subscription-id <SOURCE_SUBSCRIPTION_ID> \
  --target-subscription-id <TARGET_SUBSCRIPTION_ID> \
  [other options]
```

## Required Permissions for Cross-Subscription Migration

To successfully migrate container images between Azure Container Registries (ACR) in different subscriptions, the following Azure roles and permissions are required:

### 1. Source ACR (Export)
- **AcrPull**: Allows reading (pulling) images from the source registry.
- **Storage Blob Data Contributor**: Required if exporting to a storage account in a different subscription.

### 2. Target ACR (Import)
- **AcrPush**: Allows writing (pushing/importing) images to the target registry.

### 3. Storage Account (Intermediate Blob Storage)
- **Storage Blob Data Contributor**: Required for both export and import operations to read/write blobs.

### 4. Key Vault (if using SAS tokens/secrets)
- **Key Vault Secrets User**: Allows reading secrets (e.g., SAS tokens) from Azure Key Vault.

### 5. Managed Identity (Recommended for Automation)
If running in Azure DevOps, GitHub Actions, or other automation, use a managed identity or service principal. Assign the above roles to the identity at the appropriate scope (resource or resource group).

#### Example: Assigning Roles via Azure CLI
```sh
# Assign AcrPull to source registry
az role assignment create --assignee <IDENTITY_OBJECT_ID> --role AcrPull --scope $(az acr show --name <SOURCE_ACR> --query id -o tsv)

# Assign AcrPush to target registry
az role assignment create --assignee <IDENTITY_OBJECT_ID> --role AcrPush --scope $(az acr show --name <TARGET_ACR> --query id -o tsv)

# Assign Storage Blob Data Contributor to storage account
az role assignment create --assignee <IDENTITY_OBJECT_ID> --role "Storage Blob Data Contributor" --scope $(az storage account show --name <STORAGE_ACCOUNT> --query id -o tsv)

# Assign Key Vault Secrets User to Key Vault (if needed)
az role assignment create --assignee <IDENTITY_OBJECT_ID> --role "Key Vault Secrets User" --scope $(az keyvault show --name <KEYVAULT_NAME> --query id -o tsv)
```

> **Note:**
> - Replace `<IDENTITY_OBJECT_ID>` with the object ID of your managed identity or service principal.
> - Role assignments may take a few minutes to propagate.
> - For cross-subscription scenarios, ensure the identity exists in both subscriptions or use a multi-tenant service principal.

### Best Practices
- Use managed identities for automation to avoid storing credentials.
- Grant the minimum required permissions at the narrowest scope possible.
- Regularly review and audit role assignments.
- Use Azure Key Vault to securely store and access secrets (e.g., SAS tokens).

### Troubleshooting Permission Errors
- **Export/Import Fails with Authorization Error:**
  - Check that the identity has the correct role on both source and target ACRs.
  - Ensure Storage Blob Data Contributor is assigned for the storage account.
- **Key Vault Access Denied:**
  - Verify the identity has Key Vault Secrets User on the Key Vault.
- **Cross-Subscription Issues:**
  - Confirm the identity is assigned in both subscriptions and has the necessary roles.
  - For Azure DevOps, ensure the service connection is configured for the correct subscription and has access to all required resources.

### Best Practices
- Always use the correct subscription IDs for source and target.
- The script will automatically fetch the resource ID for the source registry and use it for import.
- You do not need to log in to the source registry; only the target registry needs to be authenticated for import.
- If importing from multiple source registries with matching repository names, tags will be merged in the target registry. Use `--force` to overwrite tags if needed.
- Review repository and tag naming to avoid accidental overwrites.

### Security Note
- The script does not log registry resource IDs to avoid exposing sensitive information.

## Notes

- Patterns in `ignore-config.json` can be globs or regex (with `re:` prefix).
- Regex must be valid Python regex.
- All arguments are case-sensitive.
- Use `--dry-run` to preview actions before actual migration.

## Troubleshooting

### Digest Comparison Issues

**"Detected re-tagged artifacts" warning:**
- This means a tag exists in both registries but points to different images
- The tag will be migrated to update the target registry
- This is normal if someone deleted and re-pushed a tag with the same name

**Tags not migrating as expected:**
- Check that digests actually differ: Use `az acr repository show-manifests --name <registry> --repository <repo>`
- Ensure you have permissions to read manifests from both registries
- Verify subscription IDs are correct for cross-subscription scenarios

### General Troubleshooting

- If a repo is not ignored as expected, check pattern syntax and ensure `--ignore-config` is supplied.
- For advanced filtering, use regex patterns.

## License

MIT
