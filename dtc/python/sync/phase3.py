"""
Phase 3 BeProduct -> DTC image upload core (pure Python, no Spark / no HTTP).

Style Image is a binary cell that cannot ride the JSON sheetData PATCH used by
Phase 1; it has its own multipart endpoint
(POST /v1/sheets/{sheetId}/views/{viewId}/images?rowindex=..&columnname=..),
which operates on an EXISTING row. So image sync is a separate step that runs
AFTER Phase 1 (p1p7_beproduct_to_dtc_push), once rows exist and have a rowIndex.

This module decides WHICH rows need an image uploaded; all HTTP (download from
the BeProduct CDN, upload to DTC) and Spark IO live in the notebook
(beproduct/p3_beproduct_to_dtc_images.py) and connectors.dtc.DTCConnector.

Decision rule (per requirement):
  For each live DTC sheet row, upload an image when BOTH hold:
    * the row's "Style Image" cell is NOT populated, AND
    * the matching BeProduct staging row has a valid front_image_url.
  Match is on the in-request key (BP Style#, Color / Wash), same as Phase 1.
  Phase 6: key column changed from "LF Style#" to "BP Style#"; staging column
  changed from lf_style_number to bp_style_number.

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
    "DTC_NATIVE_IMAGE_TYPES",
    "CONVERTIBLE_IMAGE_TYPES",
    "PDF_RENDERABLE_TYPES",
    "ImageEncoding",
    "classify_image_type",
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
# Image-type policy (Phase 3 webp/etc. handling)
# ---------------------------------------------------------------------------
# DTC's sheet image endpoint accepts jpg/png directly (validated 2026-06-17:
# 41 jpg/png uploads OK) but REJECTS webp with HTTP 400. So before upload we
# classify the downloaded image and transcode anything that isn't natively
# accepted but is a raster format we can decode; vector/unknown types are
# skipped (cannot be rasterised here).

# Types the DTC images endpoint accepts as-is.
DTC_NATIVE_IMAGE_TYPES = {"image/jpeg", "image/png"}

# Raster types we can transcode to PNG (Pillow-decodable) before upload.
CONVERTIBLE_IMAGE_TYPES = {
    "image/webp", "image/gif", "image/bmp", "image/x-ms-bmp",
    "image/tiff", "image/x-tiff",
}

# Vector / explicitly-unsupported types: cannot rasterise in this pipeline.
_VECTOR_TYPES = {"image/svg+xml"}

# PDF-backed types that CAN be rasterised (added 2026-09-30). An Adobe
# Illustrator `.ai` saved with "Create PDF Compatible File" -- Illustrator's
# default -- IS a PDF (starts with `%PDF`; live: KTB-00033's `hbpants.ai` is
# `%PDF-1.6`), so its first page renders to PNG with pypdfium2 (Apache/BSD).
# A PostScript-only `.ai` (`%!PS`, very old Illustrator or saved without PDF
# compatibility) would need Ghostscript and stays unsupported.
# Content-Types BeProduct's CDN may send for these; it sent
# application/octet-stream for the live `.ai`, hence the extension + magic.
PDF_RENDERABLE_TYPES = {
    "application/pdf", "application/illustrator", "application/postscript",
    "application/vnd.adobe.illustrator",
}
_PDF_RENDERABLE_EXTS = {"ai", "pdf"}
PDF_MAGIC = b"%PDF"
POSTSCRIPT_MAGIC = b"%!PS"

_EXT_TO_TYPE = {
    "jpg": "image/jpeg", "jpeg": "image/jpeg", "jpe": "image/jpeg",
    "png": "image/png",
    "webp": "image/webp", "gif": "image/gif", "bmp": "image/bmp",
    "tif": "image/tiff", "tiff": "image/tiff", "svg": "image/svg+xml",
}


@dataclass
class ImageEncoding:
    """How to handle a downloaded image before upload."""
    action: str          # 'upload' (as-is) | 'convert' (-> PNG) | 'rasterize' (PDF page 1 -> PNG) | 'skip'
    content_type: Optional[str]  # the type to SEND (None when skip)
    reason: str = ""     # populated for skip / convert (informational)


def _ext_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    path = str(url).split("?", 1)[0].split("#", 1)[0]
    if "." not in path.rsplit("/", 1)[-1]:
        return None
    return path.rsplit(".", 1)[-1].strip().lower() or None


def _classify_pdf_like(head: Optional[bytes], what: str) -> ImageEncoding:
    """A PDF-backed file (by type or extension): the first bytes decide."""
    if head is not None and head.lstrip()[:4] == POSTSCRIPT_MAGIC:
        return ImageEncoding("skip", None, f"unsupported_postscript_ai:{what}")
    if head is not None and PDF_MAGIC not in head[:1024]:
        return ImageEncoding("skip", None, f"unsupported_image_type:{what}")
    return ImageEncoding("rasterize", "image/png", f"rasterize {what} page 1 -> png")


def classify_image_type(content_type: Any, url: Optional[str] = None,
                        head: Optional[bytes] = None) -> ImageEncoding:
    """
    Decide whether a downloaded image can be uploaded as-is, must be transcoded
    to PNG, rasterised from a PDF, or should be skipped.

    Resolution order: trust the HTTP Content-Type first; if it is missing or
    generic (e.g. application/octet-stream), fall back to the URL/file extension,
    and finally to the file's own first bytes (`head`).

    `head` (optional, the first bytes of the download) decides PDF-backed files:
    `%PDF` -> rasterize, `%!PS` -> skip (needs Ghostscript), anything else ->
    skip. Without `head` a PDF-like type/extension is optimistically rasterised
    and the renderer has the last word.

    Returns an ImageEncoding:
      * action='upload'    -> send as-is with content_type (jpg/png)
      * action='convert'   -> transcode to PNG before sending (content_type=image/png)
      * action='rasterize' -> render PDF page 1 to PNG (.ai / .pdf; content_type=image/png)
      * action='skip'      -> unsupported (vector/unknown); content_type=None
    """
    ct = (str(content_type or "").split(";")[0].strip().lower()) or None
    if ct in PDF_RENDERABLE_TYPES:
        return _classify_pdf_like(head, ct)
    if ct in DTC_NATIVE_IMAGE_TYPES:
        return ImageEncoding("upload", ct)
    if ct in CONVERTIBLE_IMAGE_TYPES:
        return ImageEncoding("convert", "image/png", f"transcode {ct} -> png")
    if ct in _VECTOR_TYPES:
        return ImageEncoding("skip", None, f"unsupported_vector_image:{ct}")

    # Content-Type unhelpful (None / octet-stream / text) -> use the extension.
    ext_type = _EXT_TO_TYPE.get(_ext_from_url(url))
    if ext_type in DTC_NATIVE_IMAGE_TYPES:
        return ImageEncoding("upload", ext_type)
    if ext_type in CONVERTIBLE_IMAGE_TYPES:
        return ImageEncoding("convert", "image/png", f"transcode {ext_type} -> png")
    if ext_type in _VECTOR_TYPES:
        return ImageEncoding("skip", None, f"unsupported_vector_image:{ext_type}")
    ext = _ext_from_url(url)
    if ext in _PDF_RENDERABLE_EXTS:
        return _classify_pdf_like(head, ext)

    # Nothing else identifies it: a PDF header still does.
    if head is not None and head.startswith(PDF_MAGIC):
        return ImageEncoding("rasterize", "image/png", "rasterize pdf (by header) page 1 -> png")

    return ImageEncoding("skip", None, f"unsupported_image_type:{ct or ext or 'unknown'}")


# Longest side of a rasterised PDF/.ai page, in pixels. The Style Image cell
# is a thumbnail; 2000 px keeps drawings legible at ~0.1 s / ~300 KB per file
# (live: KTB-00033's 612x792 pt page -> 1546x2000 PNG, 273 KB, 0.12 s).
RASTER_MAX_PX = 2000


def render_pdf_first_page_png(data: bytes, max_px: int = RASTER_MAX_PX) -> Tuple[bytes, str]:
    """
    Render page 1 of a PDF (incl. a PDF-compatible Illustrator `.ai`) to PNG.

    Returns (png_bytes, note). The note records the page count, because a
    multi-artboard `.ai` renders only its FIRST artboard -- usually the front
    view, but a designer could order them differently.

    Uses pypdfium2 (Google PDFium; Apache-2.0/BSD), imported lazily so the
    rest of this module stays importable without it. Raises ImportError when
    it is not installed and ValueError for a file PDFium cannot open, so the
    caller can log the row as skipped instead of failing the run.
    """
    import io
    import pypdfium2 as pdfium  # lazy: only the rasterize path needs it

    try:
        pdf = pdfium.PdfDocument(data)
    except Exception as e:  # pdfium.PdfiumError; broad on purpose, logged by caller
        raise ValueError(f"pdf_open_failed:{str(e)[:80]}") from e
    try:
        n_pages = len(pdf)
        if n_pages < 1:
            raise ValueError("pdf_has_no_pages")
        page = pdf[0]
        w, h = page.get_size()
        # Scale to max_px on the long side, but never above 4x (tiny pages)
        # and never below a legible floor.
        scale = max(0.5, min(max_px / max(w, h, 1.0), 4.0))
        img = page.render(scale=scale).to_pil().convert("RGB")  # white background
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
    finally:
        pdf.close()
    note = f"rasterized page 1 of {n_pages} -> {img.size[0]}x{img.size[1]} png"
    return buf.getvalue(), note


# ---------------------------------------------------------------------------
# Plan dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ImageUploadOp:
    """A resolved image upload for one row."""
    match_key: Tuple[Optional[str], Optional[str]]
    # PRIMARY LOCATOR since 2026-09-23. The image endpoint accepts either, and
    # rowId is the safe one: a stale rowIndex does NOT error, it returns 201 and
    # CREATES a row (live-verified). rowIndex also shifts whenever anyone
    # inserts, deletes or re-orders; rowId never does.
    row_id: str
    image_url: str
    # Retained for logging/traceability ONLY -- never used to address the cell.
    row_index: Optional[int] = None
    source: str = "beproduct_extract"  # "beproduct_extract" | "sibling_copy" -- see compute_image_uploads
    # sibling_copy only: this row's OWN BeProduct front_image_url, tried when
    # the DTC-hosted sibling URL cannot be downloaded. Since 2026-09-28 DTC's
    # Application Gateway returns 403 for EVERY GET of /api/v1/images/*, with
    # or without x-api-key (34/34 live), so without this the sibling path
    # fails forever and the row never gets an image.
    fallback_url: Optional[str] = None


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
                  each must carry 'rowIndex', the match-key columns ("BP Style#",
                  "Color / Wash") and the "Style Image" cell (absent => blank).
        bp_rows:  BeProduct staging rows targeting THIS request; keys are staging
                  column names, including 'bp_style_number', 'color' and
                  'front_image_url'. (Phase 6: was lf_style_number)

    Returns:
        ImagePlan. An upload is emitted for a DTC row that is blank-image AND
        either (a) another row in this SAME request sharing the same BP
        Style# already has a populated Style Image (copied from that
        sibling's own DTC-hosted image URL — no BeProduct download at all),
        or (b) has a matching BeProduct row with a valid front_image_url AND
        a rowIndex (the original full-extraction path). Already-imaged rows
        are skipped silently (no skip record); blank rows whose source lacks
        a URL, or that have no rowIndex, are recorded as skips for
        visibility.

    Sibling-copy (added 2026-09-03, owner spec): a style's front image is a
    HEADER-level BeProduct attribute (one per style, not per colorway), so
    every colorway row of the same BP Style# within one request (fixed
    Brand+Season) is expected to carry the SAME image. Before falling back
    to a full BeProduct CDN download+transcode+upload for a blank row, check
    whether ANY other row for the same BP Style# in this request already has
    a real Style Image — if so, reuse that row's OWN already-uploaded
    DTC-hosted image URL as the upload source instead. This avoids: a
    redundant BeProduct CDN download (some of those URLs are known to 403 —
    see AGENTS.md), redundant transcoding, and any chance of the two
    colorways' images silently diverging. The upload MECHANICS are
    unchanged (still download-the-URL + classify/transcode + multipart
    POST) — only WHICH url is downloaded differs, so no notebook-side
    changes were needed; see `ImageUploadOp.source`.
    """
    lf_col, color_col = MATCH_KEY_COLS

    # Index BeProduct rows by the in-request key (first row wins on dup).
    # Phase 6: key column changed from lf_style_number to bp_style_number.
    bp_index: Dict[Tuple[Optional[str], Optional[str]], Dict[str, Any]] = {}
    for bp in bp_rows:
        key = (norm(bp.get("bp_style_number")), norm(bp.get("color")))
        if key == (None, None):
            continue
        bp_index.setdefault(key, bp)

    # Index EXISTING (already-imaged) DTC rows by BP Style# alone (style-level,
    # not colorway-level) so a blank sibling can copy from any already-imaged
    # colorway of the same style in this request. First real image wins.
    sibling_image_by_style: Dict[str, str] = {}
    for r in dtc_rows:
        style_no = norm(r.get(lf_col))
        if style_no is None or style_no in sibling_image_by_style:
            continue
        if is_image_populated(r):
            existing_url = norm(r.get(STYLE_IMAGE_COL))
            if is_valid_image_url(existing_url):
                sibling_image_by_style[style_no] = existing_url

    plan = ImagePlan()

    for r in dtc_rows:
        key = _match_key(r, lf_col, color_col)
        if key == (None, None):
            continue  # blank/placeholder DTC row

        # NOTE: deliberately NO "only act once per (style, color) key" dedup
        # here (removed 2026-09-04, live-discovered bug) -- Phase 10 can
        # create MULTIPLE physical WIP rows sharing the exact same (BP
        # Style#, Color / Wash) key (one "Main Fabric" row + one duplicate
        # per "Fabric" segment, see sync/bom.py), each with its OWN
        # Style Image cell/rowIndex. The old dedup treated the SECOND such
        # row as "already handled" the moment it saw the FIRST row's key --
        # even when the first row had a real image and the second was
        # genuinely blank -- silently skipping it with no upload AND no
        # skip record at all (invisible in every summary/log). Every
        # physical row is now evaluated independently by its own
        # rowId/rowIndex, which is what the "already imaged" / sibling-copy
        # / rowIndex checks below already operate on anyway.
        if is_image_populated(r):
            continue  # already has an image -> nothing to do (idempotent)

        # Gate on rowId: it is what the upload now addresses. A row without one
        # cannot be targeted safely at all -- uploading by a rowIndex we cannot
        # verify risks CREATING a row rather than filling one.
        row_id = r.get("rowId")
        if not row_id:
            plan.skips.append(ImageSkip(
                "missing_row_id", key, "DTC row has no rowId"))
            continue
        row_index = r.get("rowIndex")

        bp = bp_index.get(key)
        bp_url = norm(bp.get("front_image_url")) if bp is not None else None

        style_no = norm(r.get(lf_col))
        sibling_url = sibling_image_by_style.get(style_no) if style_no else None
        if sibling_url is not None:
            plan.uploads.append(ImageUploadOp(
                match_key=key,
                row_id=row_id,
                image_url=sibling_url,
                row_index=(int(row_index) if row_index is not None else None),
                source="sibling_copy",
                fallback_url=bp_url if is_valid_image_url(bp_url) else None,
            ))
            continue

        if bp is None:
            continue  # DTC row with no BeProduct source row -> leave it alone

        url = bp.get("front_image_url")
        if not is_valid_image_url(url):
            plan.skips.append(ImageSkip(
                "no_source_image", key, f"front_image_url={url!r}"))
            continue

        plan.uploads.append(ImageUploadOp(
            match_key=key,
            row_id=row_id,
            image_url=norm(url),
            row_index=(int(row_index) if row_index is not None else None),
            source="beproduct_extract",
        ))

    return plan
