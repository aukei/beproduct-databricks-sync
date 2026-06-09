"""
Upload Notebooks to Databricks Workspace
==========================================

Uploads local Python notebook files (.py) to Databricks workspace.
Automatically discovers notebooks in specified directories and uploads
them with proper path structure.

Prerequisites
-------------
    pip install databricks-sdk

Environment variables (add to .env):
    DATABRICKS_HOST      = https://adb-XXXXXXXX.azuredatabricks.net
    DATABRICKS_PAT       = dapi...

Usage
-----
    # Upload all notebooks
    python scripts/upload_notebooks.py

    # Upload specific directory
    python scripts/upload_notebooks.py --dir beproduct
    
    # Dry run (preview only)
    python scripts/upload_notebooks.py --dry-run
"""

from __future__ import annotations

import base64
import os
import sys
from pathlib import Path
from typing import Optional

# ── Ensure project root is importable ────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv

load_dotenv(_ROOT / ".env")


# ─────────────────────────────────────────────────────────────────────────────
# ANSI colour helpers
# ─────────────────────────────────────────────────────────────────────────────

def _c(code: str, text: str) -> str:
    """Wrap text in an ANSI escape code."""
    if os.environ.get("NO_COLOR") or not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


OK = lambda t: _c("32", t)  # green
WARN = lambda t: _c("33", t)  # yellow
ERR = lambda t: _c("31", t)  # red
INFO = lambda t: _c("36", t)  # cyan
BOLD = lambda t: _c("1", t)  # bold


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

# Notebook directories to upload: (local_dir, workspace_path)
NOTEBOOK_DIRS = [
    ("beproduct", "/Workspace/Repos/beproduct-sync/beproduct"),
    ("dtc/notebooks", "/Workspace/Repos/beproduct-sync/DTC/notebooks"),
    ("dtc/python/connectors", "/Workspace/Repos/beproduct-sync/DTC/python/connectors"),
]


def _load_config() -> dict[str, str]:
    """Load and validate required env vars."""
    required = {
        "DATABRICKS_HOST": "Databricks workspace URL",
        "DATABRICKS_PAT": "Personal Access Token",
    }
    cfg: dict[str, str] = {}
    missing: list[str] = []

    for key, label in required.items():
        val = os.getenv(key, "").strip()
        if not val or val.startswith("your_"):
            missing.append(f"  {key:<30} # {label}")
        else:
            cfg[key] = val

    if missing:
        print(ERR("✗ Missing Databricks configuration in .env:\n"))
        for m in missing:
            print(ERR(m))
        print(f"\nAdd the above to {_ROOT / '.env'} then retry.")
        print("See .env.example for reference.")
        sys.exit(1)

    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# UPLOAD LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def upload_notebooks_to_databricks(
    local_dir: str,
    workspace_path: str,
    file_extensions: list[str] = [".py", ".sql"],
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Upload notebooks from local directory to Databricks workspace.
    
    Args:
        local_dir: Local directory containing notebooks
        workspace_path: Target path in Databricks workspace
        file_extensions: File extensions to upload (default: .py, .sql)
        dry_run: If True, only preview uploads without executing
        
    Returns:
        (uploaded_count, failed_count)
    """
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.workspace import ImportFormat, Language
    except ImportError:
        print(ERR("✗ 'databricks-sdk' is not installed."))
        print("  Run:  pip install databricks-sdk")
        sys.exit(1)

    if not Path(local_dir).exists():
        print(ERR(f"✗ Local directory not found: {local_dir}"))
        return 0, 0

    # Find all notebook files
    notebooks: list[tuple[Path, str]] = []
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            if any(file.endswith(ext) for ext in file_extensions):
                local_path = Path(root) / file
                
                # Calculate relative path
                relative_path = local_path.relative_to(local_dir)
                
                # Build workspace path (remove file extension)
                remote_path = workspace_path + "/" + str(relative_path.with_suffix("")).replace("\\", "/")
                
                notebooks.append((local_path, remote_path))

    if not notebooks:
        print(WARN(f"  ⚠  No notebook files found in {local_dir}"))
        return 0, 0

    print(INFO(f"\n  Found {len(notebooks)} notebook(s) in {local_dir}/"))

    if dry_run:
        print(BOLD("\n  📋 Dry Run - Preview:"))
        for local_path, remote_path in notebooks:
            print(f"    {local_path} → {remote_path}")
        return len(notebooks), 0

    # Upload notebooks
    w = WorkspaceClient()
    uploaded = 0
    failed = 0

    for local_path, remote_path in notebooks:
        try:
            with open(local_path, "rb") as f:
                content_bytes = f.read()

            # Encode content as base64 string (required by SDK)
            content = base64.b64encode(content_bytes).decode("utf-8")

            # Determine language from extension
            ext = local_path.suffix.lower()
            if ext == ".py":
                language = Language.PYTHON
            elif ext == ".sql":
                language = Language.SQL
            else:
                language = Language.PYTHON  # default

            w.workspace.import_(
                path=remote_path,
                format=ImportFormat.SOURCE,
                language=language,
                content=content,
                overwrite=True,
            )

            print(OK(f"  ✅ {local_path.name} → {remote_path}"))
            uploaded += 1

        except Exception as exc:
            print(ERR(f"  ✗ Failed: {local_path.name} - {str(exc)[:80]}"))
            failed += 1

    return uploaded, failed


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Upload notebooks to Databricks workspace"
    )
    parser.add_argument(
        "--dir",
        help="Upload only this directory (e.g., 'beproduct'). Default: all configured directories",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview uploads without executing",
    )
    args = parser.parse_args()

    # ── 1. Config ────────────────────────────────────────────────────────────
    cfg = _load_config()

    print()
    print(BOLD("═" * 60))
    print(BOLD("  📤 Upload Notebooks to Databricks Workspace"))
    print(BOLD("═" * 60))
    print(INFO(f"  Workspace: {cfg['DATABRICKS_HOST']}"))

    if args.dry_run:
        print(WARN("  🔍 DRY RUN MODE - No uploads will be executed"))

    # ── 2. Filter directories if --dir specified ─────────────────────────────
    dirs_to_upload = NOTEBOOK_DIRS
    if args.dir:
        dirs_to_upload = [
            (local, remote)
            for local, remote in NOTEBOOK_DIRS
            if local.startswith(args.dir)
        ]
        if not dirs_to_upload:
            print(ERR(f"\n✗ No configured directory matches: {args.dir}"))
            print(f"  Available directories: {', '.join(d[0] for d in NOTEBOOK_DIRS)}")
            sys.exit(1)

    # ── 3. Upload all directories ────────────────────────────────────────────
    total_uploaded = 0
    total_failed = 0

    for local_dir, workspace_path in dirs_to_upload:
        print()
        print(BOLD(f"── {local_dir}/ ──"))
        print(INFO(f"  Target: {workspace_path}"))

        uploaded, failed = upload_notebooks_to_databricks(
            local_dir=local_dir,
            workspace_path=workspace_path,
            dry_run=args.dry_run,
        )

        total_uploaded += uploaded
        total_failed += failed

    # ── 4. Summary ───────────────────────────────────────────────────────────
    print()
    print(BOLD("─" * 60))

    if args.dry_run:
        print(INFO(f"  📋 Would upload {total_uploaded} notebook(s)"))
        print(INFO("  Run without --dry-run to execute uploads"))
    else:
        if total_failed == 0:
            print(OK(f"  ✅ Successfully uploaded {total_uploaded} notebook(s)"))
        else:
            print(WARN(f"  ⚠  Uploaded: {total_uploaded}, Failed: {total_failed}"))

    print()


if __name__ == "__main__":
    main()
