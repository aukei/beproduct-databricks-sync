"""
Phase 3 BeProduct -> DTC image upload core (pure Python, no Spark / no HTTP).

Style Image is a binary cell that cannot ride the JSON sheetData PATCH used by
Phase 1; it has its own multipart endpoint
(POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex=..&columnname=..),
which operates on an EXISTING row. So image sync is a separate step that runs
AFTER Phase 1 (beproduct_to_dtc_push), once rows exist and have a rowIndex.

This module decides WHICH rows need an image uploaded; all HTTP (download from
the BeProduct CDN, upload to DTC) and Spark IO live in the notebook
(beproduct/beproduct_to_dtc_images.py) and connectors.dtc.DTCConnector.

Decision rule (per requirement):
  For each live DTC sheet row, upload an image when BOTH hold:
    * the row's "Style Image" cell is NOT populated, AND
    * the matching BeProduct staging row has a valid front_image_url.
  Match is on the in-request key (LF Style#, Color / Wash), same as Phase 1.

Rows that already have an image are skipped silently (idempotent re-run). Rows
that are blank but whose BeProduct source has no usable URL are recorded as
informational skips. Image sync stays strictly BeProduct -> DTC (one direction);
it never clears or reads an image back into BeProduct.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .phase1 import norm, MATCH_KEY_COLS, STYLE_IMAGE_COL, _match_key

__all__ = [
    "is_image_populated",
    "is_valid_image_url",
    "ImageUploadOp",
    "ImageSkip",
    "ImagePlan",
    "compute_image_uploads",
]


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

def is_image_populated(dtc_row: Dict[str, Any]) -> bool:
    """
    True if the DTC row's "Style Image" cell already holds a value.

    Empty / null-sentinel cells (norm() -> None) count as NOT populated. Note
    that a column with no value across every row does not even appear in the
    sheetData row object, so .get() returns None there too -> not populated.
    """
    return norm(dtc_row.get(STYLE_IMAGE_COL)) is not None


def is_valid_image_url(url: Any) -> bool:
    """
    True if `url` is a usable absolute http(s) image URL.

    BeProduct stores the front image as headerData.frontImage.origin, a CDN URL.
    We only attempt a download for an http/https URL; blanks and null sentinels
    ("N/A", "none", ...) are rejected via norm().
    """
    v = norm(url)
    if v is None:
        return False
    return v.lower().startswith(("http://", "https://"))


# ---------------------------------------------------------------------------
# Plan dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ImageUploadOp:
    """A resolved image upload for one row."""
    match_key: Tuple[Optional[str], Optional[str]]
    row_index: int
    image_url: str
    row_id: Optional[str] = None  # informational; the image API keys off rowIndex


@dataclass
class ImageSkip:
    """A row considered but not uploaded, with a reason."""
    reason: str
    match_key: Tuple[Optional[str], Optional[str]]
    detail: str = ""


@dataclass
class ImagePlan:
    uploads: List[ImageUploadOp] = field(default_factory=list)
    skips: List[ImageSkip] = field(default_factory=list)

    def summary(self) -> Dict[str, int]:
        return {"uploads": len(self.uploads), "skips": len(self.skips)}


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def compute_image_uploads(
    dtc_rows: List[Dict[str, Any]],
    bp_rows: List[Dict[str, Any]],
) -> ImagePlan:
    """
    Compute the image-upload plan for one request.

    Args:
        dtc_rows: current (freshly reloaded) DTC rows from the WIP_ITS_USE view;
                  each must carry 'rowIndex', the match-key columns and the
                  "Style Image" cell (absent => treated as blank).
        bp_rows:  BeProduct staging rows targeting THIS request; keys are staging
                  column names, including 'lf_style_number', 'color' and
                  'front_image_url'.

    Returns:
        ImagePlan. An upload is emitted only for a DTC row that is blank-image
        AND has a matching BeProduct row with a valid front_image_url AND a
        rowIndex. Already-imaged rows are skipped silently (no skip record);
        blank rows whose source lacks a URL, or that have no rowIndex, are
        recorded as skips for visibility.
    """
    lf_col, color_col = MATCH_KEY_COLS

    # Index BeProduct rows by the in-request key (first row wins on dup).
    bp_index: Dict[Tuple[Optional[str], Optional[str]], Dict[str, Any]] = {}
    for bp in bp_rows:
        key = (norm(bp.get("lf_style_number")), norm(bp.get("color")))
        if key == (None, None):
            continue
        bp_index.setdefault(key, bp)

    plan = ImagePlan()
    seen: set = set()

    for r in dtc_rows:
        key = _match_key(r, lf_col, color_col)
        if key == (None, None):
            continue  # blank/placeholder DTC row
        if key in seen:
            continue  # only act once per key (sheet dupes ignored here)
        seen.add(key)

        if is_image_populated(r):
            continue  # already has an image -> nothing to do (idempotent)

        bp = bp_index.get(key)
        if bp is None:
            continue  # DTC row with no BeProduct source row -> leave it alone

        url = bp.get("front_image_url")
        if not is_valid_image_url(url):
            plan.skips.append(ImageSkip(
                "no_source_image", key, f"front_image_url={url!r}"))
            continue

        row_index = r.get("rowIndex")
        if row_index is None:
            plan.skips.append(ImageSkip(
                "missing_row_index", key, "DTC row has no rowIndex"))
            continue

        plan.uploads.append(ImageUploadOp(
            match_key=key,
            row_index=int(row_index),
            image_url=norm(url),
            row_id=r.get("rowId"),
        ))

    return plan
