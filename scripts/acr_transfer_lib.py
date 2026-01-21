"""Library for ACR transfer logic."""

from __future__ import annotations
import argparse
import fnmatch
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence
import re

class AzCliError(Exception):
    """Raised when an Azure CLI command fails."""
    def __init__(self, command: Sequence[str], returncode: int, stdout: str, stderr: str) -> None:
        message = (
            f"Command {' '.join(command)} failed with exit code {returncode}.\n"
            f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
        super().__init__(message)
        self.command = list(command)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

def _log(message: str, color: str = "") -> None:
    timestamp = time.strftime("%H:%M:%S")
    color_codes = {
        "bold": "\033[1m",
        "cyan": "\033[36m",
        "green": "\033[32m",
        "yellow": "\033[33m",
        "magenta": "\033[35m",
        "dim": "\033[2m",
        "reset": "\033[0m",
    }
    prefix = color_codes.get(color, "")
    suffix = color_codes["reset"] if color else ""
    print(f"[{timestamp}] {prefix}{message}{suffix}")

def _run_az(command: Sequence[str], *, expect_json: bool = False) -> str | list | dict:
    process = subprocess.run([
        "az",
        *command,
    ], capture_output=True, text=True)
    if process.returncode != 0:
        raise AzCliError(command, process.returncode, process.stdout, process.stderr)
    if expect_json:
        output = process.stdout.strip()
        if not output:
            return []
        return json.loads(output)
    return process.stdout.strip()

@dataclass
class TransferContext:
    source_name: str
    target_name: str
    source_login: str  # This will be the resource ID
    dry_run: bool
    force: bool
    force_on_retry: bool = False
    delay: float = 0.0
    source_subscription_id: str = ""
    target_subscription_id: str = ""

def _resolve_login_server(registry_name: str) -> str:
        login_server = _run_az([
            "acr",
            "show",
            "--name",
            registry_name,
            "--query",
            "loginServer",
            "--output",
            "tsv",
        ])
        resource_id = _run_az([
            "acr",
            "show",
            "--name",
            registry_name,
            "--query",
            "id",
            "--output",
            "tsv",
        ])
        return login_server, resource_id

def _parse_letters_filter(filter_expression: Optional[str]) -> Callable[[str], bool]:
    if not filter_expression:
        return lambda repo: True
    ranges: List[tuple[str, str]] = []
    singles: List[str] = []
    for token in filter_expression.split(","):
        candidate = token.strip().lower()
        if not candidate:
            continue
        if "-" in candidate:
            start, end = (part.strip() for part in candidate.split("-", 1))
            if len(start) != 1 or len(end) != 1 or not start.isalpha() or not end.isalpha():
                raise ValueError(f"Invalid range token '{token}'. Use the form a-c.")
            if start > end:
                raise ValueError(f"Invalid range token '{token}'. Range start must precede end.")
            ranges.append((start, end))
        else:
            if len(candidate) != 1 or not candidate.isalpha():
                raise ValueError(f"Invalid letter token '{token}'.")
            singles.append(candidate)
    def predicate(repository: str) -> bool:
        if not repository:
            return False
        first_alpha = None
        for char in repository.lower():
            if char.isalpha():
                first_alpha = char
                break
        if first_alpha is None:
            return False
        if first_alpha in singles:
            return True
        return any(start <= first_alpha <= end for start, end in ranges)
    return predicate

def _normalize_ignore_patterns(raw_patterns: Optional[Sequence[str]]) -> List[str]:
    if not raw_patterns:
        return []
    patterns: List[str] = []
    for entry in raw_patterns:
        if entry is None:
            continue
        if not isinstance(entry, str):
            raise ValueError("Ignore pattern values must be strings.")
        cleaned = entry.strip()
        if not cleaned:
            continue
        for token in cleaned.split(","):
            candidate = token.strip()
            if candidate:
                patterns.append(candidate)
    return patterns

def _compile_ignore_filter(patterns: Sequence[str]) -> Callable[[str], bool]:
    if not patterns:
        return lambda _: False
    regexes = []
    globs = []
    for pattern in patterns:
        if pattern.startswith("re:"):
            try:
                regexes.append(re.compile(pattern[3:]))
            except Exception:
                continue
        else:
            globs.append(pattern)
    def predicate(repository: str) -> bool:
        for regex in regexes:
            if regex.match(repository):
                return True
        for pattern in globs:
            if fnmatch.fnmatchcase(repository, pattern):
                return True
        return False
    return predicate

def _load_ignore_patterns_from_file(path: Optional[str]) -> List[str]:
    if not path:
        return []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError as error:
        raise ValueError(f"Ignore config file '{path}' not found.") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"Ignore config file '{path}' contains invalid JSON: {error}.") from error
    candidates: Sequence[str]
    if isinstance(payload, dict):
        patterns_value = payload.get("patterns")
        if patterns_value is None:
            raise ValueError(
                "Ignore config file must provide a 'patterns' property when using an object payload."
            )
        if isinstance(patterns_value, (str, bytes)):
            raise ValueError("Ignore config 'patterns' must be a sequence of strings, not a scalar value.")
        candidates = patterns_value
    elif isinstance(payload, list):
        candidates = payload
    else:
        raise ValueError(
            "Ignore config file must contain either a list of pattern strings or an object with a 'patterns' list."
        )
    return _normalize_ignore_patterns(candidates)

def _list_repositories(source_registry: str, subscription: str = "") -> List[str]:
    args = [
        "acr",
        "repository",
        "list",
        "--name",
        source_registry,
        "--output",
        "json",
    ]
    if subscription:
        args.extend(["--subscription", subscription])
    repos = _run_az(args, expect_json=True)
    return list(repos)

def _list_tags(registry: str, repository: str, subscription: str = "") -> List[str]:
    args = [
        "acr",
        "repository",
        "show-tags",
        "--name",
        registry,
        "--repository",
        repository,
        "--output",
        "json",
    ]
    if subscription:
        args.extend(["--subscription", subscription])
    tags = _run_az(args, expect_json=True)
    return sorted(list(tags))

def _list_manifests_with_digests(registry: str, repository: str, subscription: str = "") -> dict:
    """
    Get all manifests with their digests for a repository.
    
    Returns a dict mapping tag names to their manifest digests.
    Example: {"1.0.0": "sha256:abc123...", "latest": "sha256:abc123..."}
    """
    args = [
        "acr",
        "repository",
        "show-manifests",
        "--name",
        registry,
        "--repository",
        repository,
        "--output",
        "json",
    ]
    if subscription:
        args.extend(["--subscription", subscription])
    
    try:
        manifests = _run_az(args, expect_json=True)
    except AzCliError as error:
        # If repository doesn't exist, return empty dict
        stderr_lower = error.stderr.lower()
        if "repositorynotfound" in stderr_lower or "not found" in stderr_lower:
            return {}
        raise
    
    # Build mapping of tag -> digest
    tag_digest_map = {}
    if isinstance(manifests, list):
        for manifest in manifests:
            if not isinstance(manifest, dict):
                continue
            digest = manifest.get("digest", "")
            tags = manifest.get("tags")
            if digest and tags and isinstance(tags, list):
                for tag in tags:
                    tag_digest_map[tag] = digest
    
    return tag_digest_map

def _tag_has_manifest(registry: str, repository: str, tag: str) -> bool:
    """Return True if the tag has a valid manifest, False otherwise."""
    try:
        # Try to fetch the manifest digest for the tag
        manifests = _run_az([
            "acr",
            "repository",
            "show-manifests",
            "--name",
            registry,
            "--repository",
            repository,
            "--query",
            f"[?tags[?@=='{tag}']].digest",
            "--output",
            "tsv",
        ])
        if isinstance(manifests, list):
            return bool(manifests)
        return bool(str(manifests).strip())
    except AzCliError:
        return False
def _import_artifact(context: TransferContext, repository: str, tag: str) -> None:
    # Set context to target subscription before import
    _run_az(["account", "set", "--subscription", context.target_subscription_id])
    source_ref = f"{repository}:{tag}"
    resource_id = context.source_login[1] if isinstance(context.source_login, tuple) else context.source_login
    args = [
        "acr",
        "import",
        "--name",
        context.target_name,
        "--source",
        source_ref,
        "--image",
        f"{repository}:{tag}",
        "--registry",
        resource_id,
    ]
    if context.force:
        args.append("--force")
    try:
        _run_az(args)
    except AzCliError as error:
        _log(f"[debug] Import failed for {repository}:{tag}. context.force={context.force}, context.force_on_retry={context.force_on_retry}", "magenta")
        _log(f"[debug] Error text: {error.stderr or ''}\n{error.stdout or ''}", "magenta")
        # Only retry if force_on_retry is enabled and not already using --force
        if context.force_on_retry and not context.force:
            _log(f"[debug] Considering force-on-retry for {repository}:{tag}", "magenta")
            err = f"{error.stderr or ''}\n{error.stdout or ''}".lower()
            # Match common conflict/phantom tag errors
            conflict_patterns = [
                "409",
                "already exists",
                "tag already exists",
                "manifest unknown",
                "manifest does not exist",
                "code: conflict",
                "error: (conflict)",
            ]
            if any(pat in err for pat in conflict_patterns):
                _log(f"[force-on-retry] Retrying {repository}:{tag} with --force due to conflict or phantom tag error.\nError was: {error}", "yellow")
                # Ensure --force is not duplicated
                if "--force" not in args:
                    args.append("--force")
                try:
                    _run_az(args)
                except AzCliError as retry_error:
                    _log(f"[force-on-retry] Retry with --force failed for {repository}:{tag}: {retry_error}", "red")
                    raise retry_error
            else:
                _log(f"[force-on-retry] Not retrying {repository}:{tag}: error did not match conflict/phantom tag patterns.", "magenta")
                raise
        else:
            _log(f"[debug] Not retrying {repository}:{tag}: force_on_retry={context.force_on_retry}, force={context.force}", "magenta")
            raise

def perform_transfer(
    context: TransferContext,
    repositories: Sequence[str],
    *,
    max_repositories: int,
    parallel_imports: int = 1,
) -> None:
    import concurrent.futures
    repo_count = 0
    processed_repos = 0
    acted_repos = 0
    skipped_repos = 0
    planned_imports = 0
    total_success = 0
    total_failures: List[str] = []
    # dry_run_report removed
    for repository in repositories:
        if max_repositories and acted_repos >= max_repositories:
            _log("Reached repository processing limit. Stopping early as requested.")
            break
        repo_count += 1
        processed_repos += 1
        _log(
            f"Processing repository '{repository}' ({repo_count}/{len(repositories)})", "cyan"
        )
        
        # Get source manifests with digests
        try:
            source_manifests = _list_manifests_with_digests(
                context.source_name, repository, context.source_subscription_id
            )
        except AzCliError as error:
            _log(f"Failed to list manifests for '{repository}': {error}")
            total_failures.append(f"{repository}: manifest listing failed")
            continue
        
        if not source_manifests:
            _log(f"No tags found for '{repository}'. Skipping.")
            continue
        
        # Get target manifests with digests
        try:
            target_manifests = _list_manifests_with_digests(
                context.target_name, repository, context.target_subscription_id
            )
        except AzCliError as error:
            _log(f"Failed to inspect target registry for '{repository}': {error}")
            total_failures.append(f"{repository}: target inspection failed")
            continue
        
        # Determine tags to process based on digest comparison
        if context.force:
            # Force mode: migrate all tags regardless of digest
            tags_to_process = list(source_manifests.keys())
        else:
            # Compare digests: migrate if tag missing OR digest differs
            tags_to_process = []
            retagged_tags = []
            
            for tag, source_digest in source_manifests.items():
                if tag not in target_manifests:
                    # Tag doesn't exist in target
                    tags_to_process.append(tag)
                elif target_manifests[tag] != source_digest:
                    # Tag exists but points to different digest (re-tagged)
                    tags_to_process.append(tag)
                    retagged_tags.append(tag)
            
            # Report re-tagged artifacts
            if retagged_tags:
                display = ", ".join(retagged_tags[:3])
                suffix = "" if len(retagged_tags) <= 3 else ", ..."
                _log(
                    f"Detected {len(retagged_tags)} re-tagged artifact(s) for '{repository}': {display}{suffix}",
                    "yellow"
                )
        
        # Sort tags for deterministic order
        tags_to_process = sorted(tags_to_process)
        if not tags_to_process:
            skipped_repos += 1
            _log(f"No tags to import for '{repository}'. Skipping repository.")
            continue
        acted_repos += 1
        if not context.force:
            # Report skipped tags (tags with matching digests)
            all_source_tags = set(source_manifests.keys())
            skipped_tags = sorted(all_source_tags - set(tags_to_process))
            if skipped_tags:
                display = ", ".join(skipped_tags[:3])
                suffix = "" if len(skipped_tags) <= 3 else ", ..."
                _log(
                    f"Skipping {len(skipped_tags)} tag(s) with matching digest(s) for '{repository}': {display}{suffix}"
                )
        # Prepare import jobs
        def import_job(tag):
            operation_label = f"{repository}:{tag}"
            if context.dry_run:
                _log(f"DRY-RUN would import {operation_label}")
                return (repository, tag, "dry-run", None)
            _log(f"Importing {operation_label}")
            try:
                _import_artifact(context, repository, tag)
                _log(f"Successfully imported {operation_label}")
                return (repository, tag, "success", None)
            except AzCliError as error:
                _log(f"Failed to import {operation_label}: {error}")
                return (repository, tag, "failure", str(error))
        # Parallel or sequential import
        if context.dry_run or parallel_imports <= 1:
            for index, tag in enumerate(tags_to_process, start=1):
                planned_imports += 1
                result = import_job(tag)
                if not context.dry_run:
                    if result[2] == "success":
                        total_success += 1
                    elif result[2] == "failure":
                        total_failures.append(f"{repository}:{tag}")
                if not context.dry_run and context.delay:
                    time.sleep(context.delay)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_imports) as executor:
                future_to_tag = {executor.submit(import_job, tag): tag for tag in tags_to_process}
                for index, future in enumerate(concurrent.futures.as_completed(future_to_tag), 1):
                    tag = future_to_tag[future]
                    planned_imports += 1
                    result = future.result()
                    if not context.dry_run:
                        if result[2] == "success":
                            total_success += 1
                        elif result[2] == "failure":
                            total_failures.append(f"{repository}:{tag}")
                    if not context.dry_run and context.delay:
                        time.sleep(context.delay)
    _log("")
    _log(f"Transfer complete.", "green")
    _log(f"Repositories scanned: {processed_repos}", "green")
    _log(f"Repositories requiring action: {acted_repos}", "green")
    if skipped_repos:
        _log(f"Repositories already synchronized or skipped: {skipped_repos}", "yellow")
    if context.dry_run:
        _log(f"Planned imports: {planned_imports}", "magenta")
    else:
        _log(f"Successful imports: {total_success}", "green")
    if total_failures:
        _log("Failed imports:")
        for failure in total_failures:
            _log(f"  - {failure}", "red")
        sys.exit(1)
