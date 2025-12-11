import pytest
from unittest.mock import patch, MagicMock
import sys
import types
import json

# Import the script as a module


# Standard import for acr_transfer_lib
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))
import acr_transfer_lib as acr_transfer

# Helper to patch _run_az for repo/tag listing
class AzMock:
    def __init__(self, repo_tags=None):
        self.repo_tags = repo_tags or {}
        self.calls = []
    def __call__(self, command, expect_json=False):
        self.calls.append(command)
        if "repository list" in " ".join(command):
            return list(self.repo_tags.keys())
        elif "show-tags" in " ".join(command):
            repo = command[command.index("--repository") + 1]
            return self.repo_tags.get(repo, [])
        elif "show" in command:
            return "mock.azurecr.io"
        elif "import" in command:
            return "imported"
        return []

# Test _parse_letters_filter
@pytest.mark.parametrize("expr,repo,expected", [
    (None, "abc", True),
    ("a-c", "apple", True),
    ("a-c", "zebra", False),
    ("a,b", "banana", True),
    ("a-c,e", "elephant", True),
    ("a-c,e", "dog", False),
])
def test_parse_letters_filter(expr, repo, expected):
    pred = acr_transfer._parse_letters_filter(expr)
    assert pred(repo) == expected

# Test _normalize_ignore_patterns
@pytest.mark.parametrize("raw,expected", [
    (None, []),
    (["foo"], ["foo"]),
    (["foo,bar"], ["foo", "bar"]),
    (["  baz  "], ["baz"]),
    (["foo", "bar,baz"], ["foo", "bar", "baz"]),
])
def test_normalize_ignore_patterns(raw, expected):
    assert acr_transfer._normalize_ignore_patterns(raw) == expected

# Test _compile_ignore_filter
@pytest.mark.parametrize("patterns,repo,expected", [
    ([], "repo", False),
    (["foo*"], "foobar", True),
    (["bar*"], "baz", False),
])
def test_compile_ignore_filter(patterns, repo, expected):
    pred = acr_transfer._compile_ignore_filter(patterns)
    assert pred(repo) == expected

# Test _load_ignore_patterns_from_file
import tempfile

def test_load_ignore_patterns_from_file_list():
    with tempfile.NamedTemporaryFile("w+", delete=False) as f:
        json.dump(["foo", "bar"], f)
        f.flush()
        patterns = acr_transfer._load_ignore_patterns_from_file(f.name)
    assert patterns == ["foo", "bar"]


def test_load_ignore_patterns_from_file_dict():
    with tempfile.NamedTemporaryFile("w+", delete=False) as f:
        json.dump({"patterns": ["foo", "bar"]}, f)
        f.flush()
        patterns = acr_transfer._load_ignore_patterns_from_file(f.name)
    assert patterns == ["foo", "bar"]


def test_load_ignore_patterns_from_file_invalid():
    with tempfile.NamedTemporaryFile("w+", delete=False) as f:
        f.write("not json")
        f.flush()
        with pytest.raises(ValueError):
            acr_transfer._load_ignore_patterns_from_file(f.name)

# Test perform_transfer dry-run and skip logic

def test_perform_transfer_dry_run_and_skip(monkeypatch):
    repo_tags = {
        "repo1": ["v1", "v2"],
        "repo2": ["v1"],
        "repo3": ["v1"],
    }
    target_tags = {
        "repo1": ["v1", "v2"],  # already migrated
        "repo2": [],
        "repo3": ["v1"],  # already migrated
    }
    azmock = AzMock(repo_tags)
    def run_az_side_effect(command, expect_json=False):
        if "show-tags" in " ".join(command):
            repo = command[command.index("--repository") + 1]
            if command[command.index("--name") + 1] == "target":
                return target_tags.get(repo, [])
            else:
                return repo_tags.get(repo, [])
        return azmock(command, expect_json)
    monkeypatch.setattr(acr_transfer, "_run_az", run_az_side_effect)
    context = acr_transfer.TransferContext(
        source_name="source",
        target_name="target",
        source_login="mock.azurecr.io",
        dry_run=True,
        force=False,
        delay=0.0,
        target_subscription_id="dummy-sub-id",
    )
    # Only repo2 should be scheduled for migration
    acr_transfer.perform_transfer(context, ["repo1", "repo2", "repo3"], max_repositories=2)

# Test perform_transfer force mode

def test_perform_transfer_force(monkeypatch):
    repo_tags = {
        "repo1": ["v1", "v2"],
    }
    target_tags = {
        "repo1": ["v1"],
    }
    azmock = AzMock(repo_tags)
    def run_az_side_effect(command, expect_json=False):
        if "show-tags" in " ".join(command):
            repo = command[command.index("--repository") + 1]
            if command[command.index("--name") + 1] == "target":
                return target_tags.get(repo, [])
            else:
                return repo_tags.get(repo, [])
        return azmock(command, expect_json)
    monkeypatch.setattr(acr_transfer, "_run_az", run_az_side_effect)
    context = acr_transfer.TransferContext(
        source_name="source",
        target_name="target",
        source_login="mock.azurecr.io",
        dry_run=True,
        force=True,
        delay=0.0,
        target_subscription_id="dummy-sub-id",
    )
    acr_transfer.perform_transfer(context, ["repo1"], max_repositories=1)

# Test AzCliError

def test_azclierror_str():
    err = acr_transfer.AzCliError(["az", "acr", "show"], 1, "stdout", "stderr")
    assert "exit code 1" in str(err)
    assert "STDOUT" in str(err)
    assert "STDERR" in str(err)
