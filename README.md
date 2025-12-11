# Azure Container Registry Transfer Utility

## Overview

`acr_transfer.py` is a Python utility to copy repositories, container images, and Helm charts between Azure Container Registries (ACR). It supports filtering, dry-run, force overwrite, and advanced ignore patterns (including regex).

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
- `--force`: Overwrite existing tags in the target registry.

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

- If a repo is not ignored as expected, check pattern syntax and ensure `--ignore-config` is supplied.
- For advanced filtering, use regex patterns.

## License

MIT
