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
    delay: float
    target_subscription_id: str

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

def _list_repositories(source_registry: str) -> List[str]:
    repos = _run_az([
        "acr",
        "repository",
        "list",
        "--name",
        source_registry,
        "--output",
        "json",
    ], expect_json=True)
    return list(repos)

def _list_tags(registry: str, repository: str) -> List[str]:
    tags = _run_az([
        "acr",
        "repository",
        "show-tags",
        "--name",
        registry,
        "--repository",
        repository,
        "--output",
        "json",
    ], expect_json=True)
    return sorted(list(tags))

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
    _run_az(args)

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
        try:
            tags = _list_tags(context.source_name, repository)
        except AzCliError as error:
            _log(f"Failed to list tags for '{repository}': {error}")
            total_failures.append(f"{repository}: tag listing failed")
            continue
        if not tags:
            _log(f"No tags found for '{repository}'. Skipping.")
            continue
        try:
            target_tags = _list_tags(context.target_name, repository)
        except AzCliError as error:
            stderr_lower = error.stderr.lower()
            if "repositorynotfound" in stderr_lower or "not found" in stderr_lower:
                target_tags = []
            else:
                _log(f"Failed to inspect target registry for '{repository}': {error}")
                total_failures.append(f"{repository}: target inspection failed")
                continue
        target_tag_set = set(target_tags)
        if context.force:
            tags_to_process = list(tags)
        else:
            tags_to_process = [tag for tag in tags if tag not in target_tag_set]
        # Sort tags for deterministic order
        tags_to_process = sorted(tags_to_process)
        if not tags_to_process:
            skipped_repos += 1
            _log(f"No tags to import for '{repository}'. Skipping repository.")
            continue
        acted_repos += 1
        if not context.force:
            skipped_tags = [tag for tag in tags if tag not in tags_to_process]
            if skipped_tags:
                display = ", ".join(skipped_tags[:3])
                suffix = "" if len(skipped_tags) <= 3 else ", ..."
                _log(
                    f"Skipping {len(skipped_tags)} existing tag(s) for '{repository}': {display}{suffix}"
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
