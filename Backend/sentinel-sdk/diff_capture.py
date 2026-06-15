"""
sentinel/diff_capture.py

Stage-1 git diff capture for the Sentinel SDK.

Detects local code changes (versus origin/main) and filters them down to
LLM-relevant files only.  The result is shipped once per sentinel.init()
call via the existing AsyncShipper to POST /ingest/diff.

All public functions are silent-failure: any exception is logged at DEBUG
level and swallowed so diff capture never blocks or crashes the caller's
pipeline.
"""

from __future__ import annotations

import logging
import re
import subprocess
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File-filter patterns  (Stage 1 — cheap, local, pre-LLM)
# ---------------------------------------------------------------------------

LLM_RELEVANT_EXTENSIONS: frozenset[str] = frozenset(
    {".py", ".txt", ".yaml", ".yml", ".json", ".toml"}
)

PROMPT_DIR_PATTERNS: list[str] = [
    "**/prompts/**",
    "**/agents/**",
    "**/tools/**",
    "**/retrieval/**",
    "**/schemas/**",
    "**/rubrics/**",
    "**/config/**",
]

ALWAYS_SKIP_PATTERNS: list[str] = [
    "**/tests/**",
    "**/test_*.py",
    "**/*_test.py",
    "**/*.md",
    "**/frontend/**",
    "requirements.txt",
    "requirements*.txt",
    "docker-compose*",
    "**/.env*",
    "**/migrations/**",
    "**/*.css",
    "**/*.html",
    "**/*.lock",
]

# ---------------------------------------------------------------------------
# Glob helpers
# ---------------------------------------------------------------------------


def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Compile a glob pattern (with ** support) to a fullmatch-safe regex."""
    p = pattern.replace("\\", "/")
    tokens: list[str] = []
    i = 0
    while i < len(p):
        if p[i : i + 2] == "**":
            i += 2
            if i < len(p) and p[i] == "/":
                # **/ → zero or more path components (e.g. **/prompts/**)
                tokens.append("(?:.+/)?")
                i += 1
            else:
                tokens.append(".*")
        elif p[i] == "*":
            tokens.append("[^/]*")
            i += 1
        elif p[i] == "?":
            tokens.append("[^/]")
            i += 1
        else:
            tokens.append(re.escape(p[i]))
            i += 1
    return re.compile("".join(tokens))


def _matches_any(filepath: str, patterns: list[str]) -> bool:
    """Return True if filepath matches any glob pattern in the list."""
    normalized = filepath.replace("\\", "/")
    basename = normalized.rsplit("/", 1)[-1]
    for pattern in patterns:
        rx = _glob_to_regex(pattern)
        if rx.fullmatch(normalized):
            return True
        # For patterns with no path separator, also try matching just the filename
        if "/" not in pattern and rx.fullmatch(basename):
            return True
    return False


# ---------------------------------------------------------------------------
# Git operations
# ---------------------------------------------------------------------------


def _find_git_root() -> Optional[str]:
    """Return the absolute path of the git repo root, or None if not in a repo."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() if result.returncode == 0 else None
    except Exception as exc:
        log.debug("git root lookup failed: %s", exc)
        return None


def _capture_git_diff(root: str) -> tuple[Optional[str], str]:
    """
    Return (diff_text, base_branch_ref).

    Tries `git diff HEAD origin/main` first; falls back to
    `git diff HEAD` (uncommitted changes) if origin/main is unavailable.
    Returns (None, ref) when the diff is empty.
    """
    result = subprocess.run(
        ["git", "diff", "HEAD", "origin/main"],
        capture_output=True, text=True, cwd=root, timeout=10,
    )
    if result.returncode == 0:
        text = result.stdout
        return (text if text.strip() else None), "origin/main"

    result = subprocess.run(
        ["git", "diff", "HEAD"],
        capture_output=True, text=True, cwd=root, timeout=10,
    )
    if result.returncode == 0:
        text = result.stdout
        return (text if text.strip() else None), "HEAD"

    return None, "unknown"


def _get_git_metadata(root: str) -> dict[str, str]:
    """Return {'branch': ..., 'commit_sha': ...} for the current HEAD."""
    branch_result = subprocess.run(
        ["git", "branch", "--show-current"],
        capture_output=True, text=True, cwd=root, timeout=5,
    )
    branch = branch_result.stdout.strip() if branch_result.returncode == 0 else "unknown"

    sha_result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True, text=True, cwd=root, timeout=5,
    )
    commit_sha = sha_result.stdout.strip()[:8] if sha_result.returncode == 0 else "unknown"

    return {"branch": branch, "commit_sha": commit_sha}


def _filter_diff(raw_diff: str) -> str:
    """
    Stage-1 filter: keep only diff sections for LLM-relevant files.

    Two-pass check per file:
      1. ALWAYS_SKIP_PATTERNS — immediately excluded (tests, docs, frontend).
      2. PROMPT_DIR_PATTERNS OR LLM_RELEVANT_EXTENSIONS — must satisfy at least one.
    """
    if not raw_diff:
        return ""

    sections = re.split(r"(?=^diff --git )", raw_diff, flags=re.MULTILINE)
    kept: list[str] = []

    for section in sections:
        if not section.startswith("diff --git"):
            continue

        match = re.match(r"^diff --git a/(.*) b/(.*)$", section, re.MULTILINE)
        if not match:
            continue

        filepath = match.group(2)

        if _matches_any(filepath, ALWAYS_SKIP_PATTERNS):
            continue

        ext = ("." + filepath.rsplit(".", 1)[-1]) if "." in filepath else ""
        in_relevant_dir = _matches_any(filepath, PROMPT_DIR_PATTERNS)
        has_relevant_ext = ext in LLM_RELEVANT_EXTENSIONS

        if in_relevant_dir or has_relevant_ext:
            kept.append(section)

    return "".join(kept)


def _capture_original_files(root: str, filtered_diff: str, base_branch: str) -> dict[str, str]:
    """
    Fetch pre-change file content for every file in the filtered diff.

    Uses `git show <ref>:<path>`. Files that do not exist at the ref
    (new files, or ref unavailable) are silently skipped.
    """
    if not filtered_diff:
        return {}

    filepaths = re.findall(r"^diff --git a/(.*) b/", filtered_diff, re.MULTILINE)
    ref = base_branch if base_branch not in ("HEAD", "unknown") else "HEAD"

    originals: dict[str, str] = {}
    for filepath in filepaths:
        try:
            result = subprocess.run(
                ["git", "show", f"{ref}:{filepath}"],
                capture_output=True, text=True, cwd=root, timeout=5,
            )
            if result.returncode == 0 and result.stdout:
                originals[filepath] = result.stdout
        except Exception as exc:
            log.debug("could not fetch original for %s: %s", filepath, exc)

    return originals


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def capture_diff_payload(
    customer_id: str,
    pipeline_name: str,
    developer: str,
    experiment_id: str,
):
    """
    Capture the current git diff and return a DiffPayload, or None.

    Returns None when the working directory is not inside a git repo or
    when any unrecoverable error occurs.  Never raises.
    """
    from schema import DiffPayload  # late import avoids circular deps at module load

    try:
        root = _find_git_root()
        if root is None:
            return None

        raw_diff, base_branch = _capture_git_diff(root)
        filtered_diff = _filter_diff(raw_diff) if raw_diff else ""
        metadata = _get_git_metadata(root)
        original_files = _capture_original_files(root, filtered_diff, base_branch)

        return DiffPayload(
            experiment_id=experiment_id,
            pipeline_name=pipeline_name,
            developer=developer,
            branch=metadata["branch"],
            base_branch=base_branch,
            commit_sha=metadata["commit_sha"],
            filtered_diff=filtered_diff,
            original_files=original_files,
            customer_id=customer_id,
        )
    except Exception as exc:
        log.debug("capture_diff_payload failed silently: %s", exc, exc_info=True)
        return None
