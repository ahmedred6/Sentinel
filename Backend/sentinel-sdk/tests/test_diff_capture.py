"""
Tests for sentinel-sdk/diff_capture.py

Covers DIFF-01 acceptance criteria:
  - Stage-1 filter keeps LLM-relevant files and drops always-irrelevant ones
  - Git subprocess calls are fully mocked — tests run without a real remote
  - capture_diff_payload returns None on git errors, never raises
  - DiffPayload is correctly structured with all required fields
"""

from unittest.mock import MagicMock, patch

import pytest

from diff_capture import (
    ALWAYS_SKIP_PATTERNS,
    LLM_RELEVANT_EXTENSIONS,
    PROMPT_DIR_PATTERNS,
    _capture_git_diff,
    _capture_original_files,
    _filter_diff,
    _find_git_root,
    _get_git_metadata,
    _matches_any,
    capture_diff_payload,
)
from schema import DiffPayload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_DIFF = """\
diff --git a/prompts/rfp_rubric.txt b/prompts/rfp_rubric.txt
index abc123..def456 100644
--- a/prompts/rfp_rubric.txt
+++ b/prompts/rfp_rubric.txt
@@ -1,3 +1,4 @@
 Score 1-5 on relevance.
-Old criterion.
+New criterion added here.
 Weight: 0.4
+Weight: 0.6
diff --git a/tests/test_evaluator.py b/tests/test_evaluator.py
index 111111..222222 100644
--- a/tests/test_evaluator.py
+++ b/tests/test_evaluator.py
@@ -1 +1,2 @@
 # existing test
+# new test
diff --git a/agents/score_assignor.py b/agents/score_assignor.py
index 333333..444444 100644
--- a/agents/score_assignor.py
+++ b/agents/score_assignor.py
@@ -10 +10,3 @@
-old_logic()
+new_logic()
+extra_logic()
"""


def _make_proc(returncode: int = 0, stdout: str = "") -> MagicMock:
    mock = MagicMock()
    mock.returncode = returncode
    mock.stdout = stdout
    return mock


# ---------------------------------------------------------------------------
# _matches_any  (glob pattern matching)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("filepath,patterns,expected", [
    # LLM-relevant: prompt dir patterns
    ("prompts/rfp_rubric.txt",         ["**/prompts/**"],     True),
    ("agents/score_assignor.py",       ["**/agents/**"],      True),
    ("a/b/c/prompts/nested.txt",       ["**/prompts/**"],     True),
    ("tools/retriever.py",             ["**/tools/**"],       True),
    # LLM-relevant: extension match
    ("main.py",                        ["**/*.py"],           True),
    ("config.yaml",                    ["**/*.yaml"],         True),
    # Always-skip: directory patterns
    ("tests/test_foo.py",              ["**/tests/**"],       True),
    ("frontend/App.js",                ["**/frontend/**"],    True),
    ("migrations/0001_initial.sql",    ["**/migrations/**"],  True),
    # Always-skip: filename patterns (basename fallback)
    ("requirements.txt",               ["requirements.txt"],  True),
    ("backend/requirements.txt",       ["requirements.txt"],  True),
    ("docker-compose.yml",             ["docker-compose*"],   True),
    ("infra/docker-compose.dev.yml",   ["docker-compose*"],   True),
    (".env.local",                     ["**/.env*"],          True),
    ("backend/.env",                   ["**/.env*"],          True),
    # Non-matches
    ("prompts/rfp.txt",                ["**/tests/**"],       False),
    ("agents/foo.py",                  ["**/frontend/**"],    False),
    ("main.py",                        ["**/prompts/**"],     False),
])
def test_matches_any(filepath, patterns, expected):
    assert _matches_any(filepath, patterns) == expected


# ---------------------------------------------------------------------------
# _filter_diff
# ---------------------------------------------------------------------------

def test_filter_diff_empty_string_returns_empty():
    assert _filter_diff("") == ""


def test_filter_diff_keeps_prompt_files():
    result = _filter_diff(SAMPLE_DIFF)
    assert "prompts/rfp_rubric.txt" in result


def test_filter_diff_keeps_agent_files():
    result = _filter_diff(SAMPLE_DIFF)
    assert "agents/score_assignor.py" in result


def test_filter_diff_drops_test_files():
    result = _filter_diff(SAMPLE_DIFF)
    assert "tests/test_evaluator.py" not in result


def test_filter_diff_result_is_valid_git_diff_format():
    result = _filter_diff(SAMPLE_DIFF)
    assert result.startswith("diff --git")
    for line in result.splitlines():
        if line.startswith("diff --git"):
            assert " a/" in line and " b/" in line


def test_filter_diff_all_irrelevant_returns_empty():
    diff_only_skip = """\
diff --git a/README.md b/README.md
index aaa..bbb 100644
--- a/README.md
+++ b/README.md
@@ -1 +1 @@
-old
+new
diff --git a/tests/test_foo.py b/tests/test_foo.py
index ccc..ddd 100644
--- a/tests/test_foo.py
+++ b/tests/test_foo.py
@@ -1 +1 @@
-x
+y
"""
    assert _filter_diff(diff_only_skip) == ""


# ---------------------------------------------------------------------------
# _find_git_root
# ---------------------------------------------------------------------------

def test_find_git_root_returns_path_on_success():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=0, stdout="/home/user/project\n")
        result = _find_git_root()
    assert result == "/home/user/project"


def test_find_git_root_returns_none_outside_repo():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=128, stdout="")
        result = _find_git_root()
    assert result is None


def test_find_git_root_returns_none_on_exception():
    with patch("diff_capture.subprocess.run", side_effect=FileNotFoundError("git not found")):
        result = _find_git_root()
    assert result is None


# ---------------------------------------------------------------------------
# _capture_git_diff
# ---------------------------------------------------------------------------

def test_capture_git_diff_prefers_origin_main():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=0, stdout="diff --git a/x b/x\n")
        text, base = _capture_git_diff("/repo")
    assert base == "origin/main"
    assert text is not None


def test_capture_git_diff_falls_back_to_head():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.side_effect = [
            _make_proc(returncode=128, stdout=""),   # origin/main fails
            _make_proc(returncode=0, stdout="diff --git a/x b/x\n"),  # HEAD succeeds
        ]
        text, base = _capture_git_diff("/repo")
    assert base == "HEAD"
    assert text is not None


def test_capture_git_diff_empty_output_returns_none_text():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=0, stdout="   \n")
        text, base = _capture_git_diff("/repo")
    assert text is None


# ---------------------------------------------------------------------------
# _get_git_metadata
# ---------------------------------------------------------------------------

def test_get_git_metadata_returns_branch_and_sha():
    def _side_effect(cmd, **kwargs):
        if "--show-current" in cmd:
            return _make_proc(returncode=0, stdout="feature/my-branch\n")
        if "rev-parse" in cmd:
            return _make_proc(returncode=0, stdout="abcdef1234567890\n")
        return _make_proc(returncode=1)

    with patch("diff_capture.subprocess.run", side_effect=_side_effect):
        meta = _get_git_metadata("/repo")

    assert meta["branch"] == "feature/my-branch"
    assert meta["commit_sha"] == "abcdef12"  # first 8 chars


def test_get_git_metadata_unknown_on_git_error():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=1, stdout="")
        meta = _get_git_metadata("/repo")
    assert meta["branch"] == "unknown"
    assert meta["commit_sha"] == "unknown"


# ---------------------------------------------------------------------------
# _capture_original_files
# ---------------------------------------------------------------------------

def test_capture_original_files_empty_diff_returns_empty():
    result = _capture_original_files("/repo", "", "origin/main")
    assert result == {}


def test_capture_original_files_skips_files_not_in_git():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=128, stdout="")
        result = _capture_original_files("/repo", SAMPLE_DIFF, "origin/main")
    assert result == {}


def test_capture_original_files_captures_all_found_files():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=0, stdout="original content\n")
        result = _capture_original_files("/repo", SAMPLE_DIFF, "origin/main")
    # SAMPLE_DIFF has 3 file sections
    assert len(result) == 3
    assert all(v == "original content\n" for v in result.values())


# ---------------------------------------------------------------------------
# capture_diff_payload  (end-to-end integration)
# ---------------------------------------------------------------------------

def _mock_all_git(cmd, **kwargs):
    """Simulate a healthy git repo with SAMPLE_DIFF as the current diff."""
    joined = " ".join(str(c) for c in cmd)
    if "rev-parse --show-toplevel" in joined:
        return _make_proc(returncode=0, stdout="/repo\n")
    if "diff HEAD origin/main" in joined:
        return _make_proc(returncode=0, stdout=SAMPLE_DIFF)
    if "--show-current" in joined:
        return _make_proc(returncode=0, stdout="feature/fix\n")
    if "rev-parse HEAD" in joined:
        return _make_proc(returncode=0, stdout="abc12345\n")
    if "show" in joined:
        return _make_proc(returncode=0, stdout="original\n")
    return _make_proc(returncode=1)


def test_capture_diff_payload_returns_diff_payload():
    with patch("diff_capture.subprocess.run", side_effect=_mock_all_git):
        payload = capture_diff_payload(
            customer_id="cust_001",
            pipeline_name="rfp-eval",
            developer="ahmed",
            experiment_id="exp_abc123",
        )

    assert isinstance(payload, DiffPayload)
    assert payload.customer_id == "cust_001"
    assert payload.pipeline_name == "rfp-eval"
    assert payload.developer == "ahmed"
    assert payload.experiment_id == "exp_abc123"
    assert payload.branch == "feature/fix"
    assert payload.base_branch == "origin/main"
    assert payload.commit_sha == "abc12345"


def test_capture_diff_payload_filtered_diff_excludes_tests():
    with patch("diff_capture.subprocess.run", side_effect=_mock_all_git):
        payload = capture_diff_payload(
            customer_id="cust_001",
            pipeline_name="rfp-eval",
            developer="ahmed",
            experiment_id="exp_abc123",
        )

    assert "prompts" in payload.filtered_diff
    assert "agents" in payload.filtered_diff
    assert "test_evaluator" not in payload.filtered_diff


def test_capture_diff_payload_returns_none_when_not_in_repo():
    with patch("diff_capture.subprocess.run") as mock_run:
        mock_run.return_value = _make_proc(returncode=128, stdout="")
        payload = capture_diff_payload(
            customer_id="cust_001",
            pipeline_name="rfp-eval",
            developer="ahmed",
            experiment_id="exp_123",
        )
    assert payload is None


def test_capture_diff_payload_returns_none_on_unexpected_exception():
    with patch("diff_capture.subprocess.run", side_effect=RuntimeError("unexpected")):
        payload = capture_diff_payload(
            customer_id="cust_001",
            pipeline_name="rfp-eval",
            developer="ahmed",
            experiment_id="exp_123",
        )
    assert payload is None


def test_capture_diff_payload_empty_diff_returns_payload_with_empty_filtered():
    def _mock_no_diff(cmd, **kwargs):
        joined = " ".join(str(c) for c in cmd)
        if "rev-parse --show-toplevel" in joined:
            return _make_proc(returncode=0, stdout="/repo\n")
        if "diff HEAD origin/main" in joined:
            return _make_proc(returncode=0, stdout="")  # no changes
        if "--show-current" in joined:
            return _make_proc(returncode=0, stdout="main\n")
        if "rev-parse HEAD" in joined:
            return _make_proc(returncode=0, stdout="abc12345\n")
        return _make_proc(returncode=1)

    with patch("diff_capture.subprocess.run", side_effect=_mock_no_diff):
        payload = capture_diff_payload(
            customer_id="cust_001",
            pipeline_name="rfp-eval",
            developer="ahmed",
            experiment_id="exp_123",
        )

    assert payload is not None
    assert payload.filtered_diff == ""
    assert payload.original_files == {}
