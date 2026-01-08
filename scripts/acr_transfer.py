#!/usr/bin/env python3
"""Utility to copy repositories, container images, and Helm charts between Azure Container Registries."""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from acr_transfer_lib import (
    AzCliError,
    TransferContext,
    _log,
    _run_az,
    _resolve_login_server,
    _parse_letters_filter,
    _normalize_ignore_patterns,
    _compile_ignore_filter,
    _load_ignore_patterns_from_file,
    _list_repositories,
    _list_tags,
    _import_artifact,
    perform_transfer,
)

import argparse
from typing import Optional, Sequence, List

def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transfer artifacts between Azure Container Registries.")
    parser.add_argument("--source-registry-name", required=True, help="Name of the source Azure Container Registry.")
    parser.add_argument("--target-registry-name", required=True, help="Name of the target Azure Container Registry.")
    parser.add_argument("--source-subscription-id", required=True, help="Azure subscription ID for the source ACR.")
    parser.add_argument("--target-subscription-id", required=True, help="Azure subscription ID for the target ACR.")
    parser.add_argument("--repository", help="Single repository name to transfer. Overrides letter filters.")
    parser.add_argument(
        "--letters",
        help="Comma separated list of letters or ranges (for example: a-c,e,g) used to filter repositories by name.",
    )
    parser.add_argument(
        "--ignore-pattern",
        action="append",
        default=None,
        help=(
            "Glob-style pattern(s) of repository names to exclude. "
            "Specify multiple times or as a comma-separated list."
        ),
    )
    parser.add_argument(
        "--ignore-config",
        help=(
            "Path to a JSON file containing ignore patterns. "
            "The file may be a list of strings or an object with a 'patterns' list."
        ),
    )
    parser.add_argument(
        "--max-repositories",
        type=int,
        default=0,
        help="Optional limit for the number of repositories to process in this run.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Optional delay in seconds between individual imports to avoid overloading the service.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report planned actions without importing artifacts.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing tags in the target registry when duplicates are encountered.",
    )
    parser.add_argument(
        "--force-on-retry",
        action="store_true",
        help="On import failure due to tag conflict, retry with --force for that tag only.",
    )
    parser.add_argument(
        "--parallel-imports",
        type=int,
        default=2,
        help="Number of parallel imports to run (default: 1, i.e., sequential). Use with caution.",
    )
    return parser.parse_args(argv)

def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    try:
        letter_filter = _parse_letters_filter(args.letters)
    except ValueError as error:
        _log(str(error))
        sys.exit(2)

    try:
        cli_ignore_patterns = _normalize_ignore_patterns(args.ignore_pattern)
    except ValueError as error:
        _log(str(error))
        sys.exit(2)

    try:
        config_ignore_patterns = _load_ignore_patterns_from_file(args.ignore_config)
    except ValueError as error:
        _log(str(error))
        sys.exit(2)

    ignore_patterns = cli_ignore_patterns + config_ignore_patterns
    ignore_predicate = _compile_ignore_filter(ignore_patterns)

    _log("Resolving registry endpoints...", "bold")
    # Set context to source subscription before getting source registry info
    try:
        _run_az(["account", "set", "--subscription", args.source_subscription_id])
        source_login_server, source_resource_id = _resolve_login_server(args.source_registry_name)
    except AzCliError as error:
        _log(f"Unable to resolve source registry endpoint: {error}")
        sys.exit(1)
    # Set context to target subscription before getting target registry info
    try:
        _run_az(["account", "set", "--subscription", args.target_subscription_id])
        target_login_server, target_resource_id = _resolve_login_server(args.target_registry_name)
    except AzCliError as error:
        _log(f"Unable to resolve target registry endpoint: {error}")
        sys.exit(1)

    _log(f"Source registry login server: {source_login_server}", "cyan")
    _log(f"Target registry login server: {target_login_server}", "cyan")

    repositories: List[str]
    scheduled_repos: List[str]
    context = TransferContext(
        source_name=args.source_registry_name,
        target_name=args.target_registry_name,
        source_login=(source_login_server, source_resource_id),
        dry_run=args.dry_run,
        force=args.force,
        force_on_retry=getattr(args, "force_on_retry", False),
        delay=args.delay_seconds,
        target_subscription_id=args.target_subscription_id,
    )

    if args.repository:
        repositories = [args.repository]
        _log("=== Repository selection summary ===")
        _log(f"Single repository specified: {args.repository}")
        # Debug output removed
        try:
            tags = _list_tags(args.source_registry_name, args.repository)
        except AzCliError as error:
            _log(f"[ERROR] Failed to list tags for '{args.repository}' in source: {error}", "red")
            tags = []
        try:
            target_tags = _list_tags(args.target_registry_name, args.repository)
        except AzCliError as error:
            stderr_lower = str(error.stderr).lower()
            if "repositorynotfound" in stderr_lower or "not found" in stderr_lower:
                target_tags = []
            else:
                _log(f"[ERROR] Failed to list tags for '{args.repository}' in target: {error}", "red")
                target_tags = []
        target_tag_set = set(target_tags)
        if args.force:
            tags_to_process = list(tags)
        else:
            tags_to_process = [tag for tag in tags if tag not in target_tag_set]
        scheduled_repos = [args.repository] if tags_to_process else []
    else:
        import concurrent.futures
        try:
            all_repositories = _list_repositories(args.source_registry_name)
        except AzCliError as error:
            _log(f"Failed to list repositories: {error}")
            sys.exit(1)
        eligible: List[str] = []
        ignored_repositories: List[str] = []
        for repo in all_repositories:
            if not letter_filter(repo):
                continue
            if ignore_predicate(repo):
                ignored_repositories.append(repo)
                continue
            eligible.append(repo)
        repositories = sorted(eligible)
        scheduled_repos = []
        skipped_no_tags = []
        skipped_all_tags_present = []

        def fetch_tags_for_repo(repo):
            try:
                tags = _list_tags(args.source_registry_name, repo)
            except AzCliError:
                tags = []
            try:
                target_tags = _list_tags(args.target_registry_name, repo)
            except AzCliError as error:
                stderr_lower = str(error.stderr).lower()
                if "repositorynotfound" in stderr_lower or "not found" in stderr_lower:
                    target_tags = []
                else:
                    target_tags = []
            return repo, tags, target_tags

        _log(f"Fetching tags for {len(repositories)} repositories in parallel...", "bold")
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            future_to_repo = {executor.submit(fetch_tags_for_repo, repo): repo for repo in repositories}
            for idx, future in enumerate(concurrent.futures.as_completed(future_to_repo), 1):
                repo = future_to_repo[future]
                try:
                    repo, tags, target_tags = future.result()
                except Exception as exc:
                    _log(f"[ERROR] Exception fetching tags for {repo}: {exc}", "red")
                    tags, target_tags = [], []
                results.append((repo, tags, target_tags))
                if idx % 25 == 0 or idx == len(repositories):
                    _log(f"Processed {idx}/{len(repositories)} repositories...")

        for repo, tags, target_tags in results:
            target_tag_set = set(target_tags)
            if not tags:
                skipped_no_tags.append(repo)
                continue
            if args.force:
                tags_to_process = list(tags)
            else:
                tags_to_process = [tag for tag in tags if tag not in target_tag_set]
            if tags_to_process:
                scheduled_repos.append(repo)
            else:
                skipped_all_tags_present.append(repo)
        if args.max_repositories:
            scheduled_repos = scheduled_repos[:args.max_repositories]
        _log(f"Identified {len(repositories)} repositories to process from {len(all_repositories)} total available.", "bold")
        _log("====================================", "cyan")
        _log("=== Repository selection summary ===", "bold")
        _log("====================================", "cyan")
        if ignore_patterns:
            _log(f"Ignore pattern(s) in effect: {', '.join(ignore_patterns)}", "dim")
            if args.ignore_config:
                _log(f"Ignore patterns loaded from config file: {args.ignore_config}", "dim")
        elif args.ignore_config:
            _log(f"No ignore patterns found in config file: {args.ignore_config}", "dim")
        if ignored_repositories:
            preview_window = min(len(ignored_repositories), 10)
            preview_items = sorted(ignored_repositories)[:preview_window]
            formatted_preview = "\n  - ".join(preview_items)
            _log(f"Ignored {len(ignored_repositories)} repository(ies) matching patterns:\n  - {formatted_preview}", "dim")
        if skipped_no_tags:
            preview_window = min(len(skipped_no_tags), 10)
            preview_items = sorted(skipped_no_tags)[:preview_window]
            formatted_preview = "\n  - ".join(preview_items)
            _log(f"Skipped {len(skipped_no_tags)} repository(ies) (no tags found in source):\n  - {formatted_preview}", "yellow")
        if skipped_all_tags_present:
            preview_window = min(len(skipped_all_tags_present), 10)
            preview_items = sorted(skipped_all_tags_present)[:preview_window]
            formatted_preview = "\n  - ".join(preview_items)
            _log(f"Skipped {len(skipped_all_tags_present)} repository(ies) (all tags already present in target):\n  - {formatted_preview}", "magenta")
        if scheduled_repos:
            formatted_list = "\n  - ".join(scheduled_repos)
            _log(f"Repositories scheduled for this run (limit {args.max_repositories}):\n  - {formatted_list}", "green")
            remaining = len(repositories) - len(scheduled_repos) - len(skipped_no_tags) - len(skipped_all_tags_present)
            if remaining > 0:
                _log(f"{remaining} additional repositories remain matching the filter.", "cyan")
        else:
            _log("No repositories require migration based on current criteria.", "yellow")
        _log("")
    if not scheduled_repos:
        _log("No repositories matched the provided criteria. Nothing to do.")
        return
    _log("===============================", "cyan")
    _log("=== Beginning transfer loop ===", "bold")
    _log("===============================", "cyan")
    perform_transfer(
        context,
        scheduled_repos,
        max_repositories=args.max_repositories,
        parallel_imports=args.parallel_imports,
    )

if __name__ == "__main__":
    main()
