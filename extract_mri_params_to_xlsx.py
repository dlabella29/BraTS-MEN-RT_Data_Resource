# -*- coding: utf-8 -*-
"""
Multithreaded extraction of MRI acquisition parameters from DICOMs into Excel (.xlsx),
robust to arbitrary case-folder names and variable nesting depths.

- Accepts "PatientID_****\\DICOM\\EXP00000\\*.dcm" style folders
- Accepts single-level random case IDs: "SUNY\\<random_case_id>\\*.dcm"
- Identifies case folders by (a) PatientID_* pattern OR (b) having >= MIN_DICOMS_PER_CASE DICOMs in subtree

Requires:
    pip install pydicom openpyxl
"""

import os
import re
import sys
import traceback
from collections import defaultdict
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------
# CONFIG (EDIT ME)
# -----------------
MASTER_DIR   = r"D:\Research\BraTS-MEN-RT\DICOMS\SITE"
OUTPUT_XLSX  = r"D:\Research\BraTS-MEN-RT\SITE_imaging_parameters_extracted_MT.xlsx"
CASE_PREFIX  = "PatientID_"     # used if such folders exist; otherwise ignored
MAX_CASES    = None             # None for all, or integer for quick tests
MAX_WORKERS  = 24

# A folder is considered a "case" if it contains at least this many DICOM files in its subtree
MIN_DICOMS_PER_CASE = 20

# Safety: do not scan more than this many files per folder when checking if it's a DICOM-bearing case
CASE_SCAN_FILE_BUDGET = 1000

# -----------------
# Imports (deps)
# -----------------
try:
    import pydicom
    from pydicom.errors import InvalidDicomError
except Exception:
    print("ERROR: pydicom is required. Install with: pip install pydicom")
    raise

try:
    from openpyxl import Workbook
except Exception:
    print("ERROR: openpyxl is required. Install with: pip install openpyxl")
    raise

# --------------------------------
# Constants / SOP Class filtering
# --------------------------------
MR_IMAGE_SOP_UIDS = {
    "1.2.840.10008.5.1.4.1.1.4",    # MR Image Storage
    "1.2.840.10008.5.1.4.1.1.4.1",  # Enhanced MR Image Storage
}
EXCLUDE_SOP_UIDS = {
    "1.2.840.10008.5.1.4.1.1.481.3",  # RT Structure Set
    "1.2.840.10008.5.1.4.1.1.7",      # Secondary Capture
}

# --------------------------------
# Utility helpers
# --------------------------------
def is_dicom_file(path: str) -> bool:
    """Fast-ish check for DICOM, then safe pydicom header read fallback."""
    try:
        with open(path, "rb") as f:
            head = f.read(132)
        if len(head) >= 132 and head[128:132] == b"DICM":
            return True
    except Exception:
        pass
    try:
        pydicom.dcmread(path, stop_before_pixels=True, force=True)
        return True
    except Exception:
        return False

def read_header_minimal(path: str) -> Optional[pydicom.dataset.Dataset]:
    try:
        return pydicom.dcmread(path, stop_before_pixels=True, force=True)
    except (InvalidDicomError, FileNotFoundError, PermissionError):
        return None
    except Exception:
        return None

def _subtree_has_dicoms(dir_path: str, min_needed: int, budget: int) -> bool:
    """Return True if at least min_needed DICOMs exist under dir_path, scanning up to 'budget' files."""
    count = 0
    scanned = 0
    for r, _, files in os.walk(dir_path):
        for fn in files:
            fp = os.path.join(r, fn)
            scanned += 1
            if scanned > budget:
                return count >= min_needed
            # quick filters
            if not os.path.isfile(fp):
                continue
            if is_dicom_file(fp):
                count += 1
                if count >= min_needed:
                    return True
    return False

def find_case_roots_flexible(master_dir: str, prefix: Optional[str]) -> List[str]:
    """
    Flexible case discovery:
      1) If any 'prefix*' folders exist anywhere, use the SHALLOWEST such folder per case_id.
      2) Additionally, pick any first-level subdirectories of master_dir that contain >= MIN_DICOMS_PER_CASE
         DICOMs in their subtree (to catch random-named case folders like SUNY\B5yKtB2...).
      3) Deduplicate: if a selected folder is nested under another selected case folder, keep the SHALLOWER path.
    """
    selected: Dict[str, str] = {}

    # (1) Prefer classic PatientID_* if present anywhere
    if prefix:
        pattern = re.compile(rf"^{re.escape(prefix)}[^\\/]+$", re.IGNORECASE)
        candidates: Dict[str, str] = {}
        for root, dirs, _ in os.walk(master_dir):
            for d in dirs:
                if pattern.match(d):
                    case_id = d
                    full = os.path.join(root, d)
                    if case_id not in candidates or len(full.split(os.sep)) < len(candidates[case_id].split(os.sep)):
                        candidates[case_id] = full
        for cid, path in candidates.items():
            selected[cid] = path

    # (2) Also accept first-level subdirectories that clearly contain DICOMs (random case-name folders)
    for d in sorted(os.listdir(master_dir)):
        full = os.path.join(master_dir, d)
        if not os.path.isdir(full):
            continue
        cid = os.path.basename(full)
        # If already selected via prefix, skip
        if cid in selected:
            continue
        if _subtree_has_dicoms(full, MIN_DICOMS_PER_CASE, CASE_SCAN_FILE_BUDGET):
            selected[cid] = full

    # (3) Deduplicate nested case folders (keep shallower)
    # sort by depth, shallowest first
    items = sorted(selected.items(), key=lambda kv: len(kv[1].split(os.sep)))
    final: Dict[str, str] = {}
    kept_paths: List[str] = []
    for cid, path in items:
        # if this path is under any already-kept path, skip (we keep the shallow parent)
        parent_of_existing = False
        for kept in kept_paths:
            if os.path.commonpath([kept, path]) == kept:
                parent_of_existing = True
                break
        if not parent_of_existing:
            final[cid] = path
            kept_paths.append(path)

    # Return case roots sorted by case_id for determinism
    return [final[k] for k in sorted(final.keys())]

def choose_representative_file(case_root: str) -> Optional[str]:
    """
    Pick one DICOM from the largest MR series under case_root.
    Fallback: largest series (any modality), then any DICOM encountered.
    """
    series_counts: Dict[str, int] = defaultdict(int)
    series_example: Dict[str, str] = {}
    mr_series_counts: Dict[str, int] = defaultdict(int)
    mr_series_example: Dict[str, str] = {}
    any_dicom_example: Optional[str] = None

    for r, _, files in os.walk(case_root):
        for fn in files:
            fp = os.path.join(r, fn)
            if not os.path.isfile(fp):
                continue
            if not is_dicom_file(fp):
                continue
            ds = read_header_minimal(fp)
            if ds is None:
                continue

            sop = str(getattr(ds, "SOPClassUID", ""))
            if sop in EXCLUDE_SOP_UIDS:
                continue

            series_uid = str(getattr(ds, "SeriesInstanceUID", ""))
            modality = str(getattr(ds, "Modality", "")).upper()

            if any_dicom_example is None:
                any_dicom_example = fp

            if series_uid:
                series_counts[series_uid] += 1
                series_example.setdefault(series_uid, fp)
                if (sop in MR_IMAGE_SOP_UIDS) or (modality == "MR"):
                    mr_series_counts[series_uid] += 1
                    mr_series_example.setdefault(series_uid, fp)

    if mr_series_counts:
        best_uid = max(mr_series_counts, key=mr_series_counts.get)
        return mr_series_example[best_uid]
    if series_counts:
        best_uid = max(series_counts, key=series_counts.get)
        return series_example[best_uid]
    return any_dicom_example

def _get_first_numeric(value, default="NA") -> Any:
    if value is None:
        return default
    try:
        v = value[0] if isinstance(value, (list, tuple)) and value else value
        s = str(v).strip()
        if s == "":
            return default
        try:
            if "." in s or "e" in s.lower():
                return float(s)
            return int(s)
        except Exception:
            return s
    except Exception:
        return default

def _get_pixel_spacing(ds) -> Tuple[Any, Any]:
    ps = getattr(ds, "PixelSpacing", None)
    if ps and len(ps) >= 2:
        return (_get_first_numeric(ps[0]), _get_first_numeric(ps[1]))
    try:
        sfg = ds.get("SharedFunctionalGroupsSequence")
        if sfg and len(sfg) > 0:
            pms = sfg[0].get("PixelMeasuresSequence")
            if pms and len(pms) > 0:
                ps2 = pms[0].get("PixelSpacing")
                if ps2 and len(ps2) >= 2:
                    return (_get_first_numeric(ps2[0]), _get_first_numeric(ps2[1]))
    except Exception:
        pass
    return ("NA", "NA")

def _get_slice_thickness(ds) -> Any:
    val = getattr(ds, "SliceThickness", None)
    if val is not None:
        return _get_first_numeric(val)
    try:
        sfg = ds.get("SharedFunctionalGroupsSequence")
        if sfg and len(sfg) > 0:
            pms = sfg[0].get("PixelMeasuresSequence")
            if pms and len(pms) > 0:
                st = pms[0].get("SliceThickness")
                if st is not None:
                    return _get_first_numeric(st)
    except Exception:
        pass
    return "NA"

def _get_mr_timing(ds) -> Dict[str, Any]:
    out = {
        "TR_ms": _get_first_numeric(getattr(ds, "RepetitionTime", None)),
        "TE_ms": _get_first_numeric(getattr(ds, "EchoTime", None)),
        "TI_ms": _get_first_numeric(getattr(ds, "InversionTime", None)),
    }
    if any(v == "NA" for v in out.values()):
        try:
            sfg = ds.get("SharedFunctionalGroupsSequence")
            if sfg and len(sfg) > 0:
                mrs = sfg[0].get("MRTimingAndRelatedParametersSequence")
                if mrs and len(mrs) > 0:
                    item = mrs[0]
                    if out["TR_ms"] == "NA":
                        out["TR_ms"] = _get_first_numeric(item.get("RepetitionTime"))
                    if out["TE_ms"] == "NA":
                        out["TE_ms"] = _get_first_numeric(item.get("EchoTime"))
                    if out["TI_ms"] == "NA":
                        out["TI_ms"] = _get_first_numeric(item.get("InversionTime"))
        except Exception:
            pass
    return out

def _format_acquisition_matrix(ds) -> str:
    try:
        am = ds.get((0x0018, 0x1310))
        if am is not None:
            nums = []
            for v in am.value:
                try:
                    nums.append(int(str(v)))
                except Exception:
                    pass
            if len(nums) == 4:
                rows = max(nums[0], nums[2])
                cols = max(nums[1], nums[3])
                return f"{rows}x{cols}"
            elif len(nums) == 2:
                return f"{nums[0]}x{nums[1]}"
    except Exception:
        pass
    rows = getattr(ds, "Rows", None)
    cols = getattr(ds, "Columns", None)
    if rows and cols:
        try:
            return f"{int(rows)}x{int(cols)}"
        except Exception:
            return f"{rows}x{cols}"
    return "NA"

def extract_metadata(ds) -> Dict[str, Any]:
    vendor  = getattr(ds, "Manufacturer", None) or "NA"
    model   = getattr(ds, "ManufacturerModelName", None) or "NA"
    sdesc   = getattr(ds, "SeriesDescription", None) or "NA"

    seq_name = getattr(ds, "SequenceName", None) or getattr(ds, "PulseSequenceName", None) or "NA"
    scan_seq = getattr(ds, "ScanningSequence", None) or "NA"
    seq_var  = getattr(ds, "SequenceVariant", None) or "NA"

    field_T = _get_first_numeric(getattr(ds, "MagneticFieldStrength", None))
    timing  = _get_mr_timing(ds)
    flip    = _get_first_numeric(getattr(ds, "FlipAngle", None))
    ps_r, ps_c = _get_pixel_spacing(ds)
    st      = _get_slice_thickness(ds)
    acqmat  = _format_acquisition_matrix(ds)
    r_rows  = _get_first_numeric(getattr(ds, "Rows", None))
    r_cols  = _get_first_numeric(getattr(ds, "Columns", None))
    pbw     = _get_first_numeric(getattr(ds, "PixelBandwidth", None))
    coil    = getattr(ds, "ReceiveCoilName", None) or getattr(ds, "CoilString", None) or "NA"

    return {
        "vendor": str(vendor),
        "model": str(model),
        "field_strength_T": field_T,
        "series_description": str(sdesc),
        "sequence_name": str(seq_name),
        "scanning_sequence": str(scan_seq),
        "sequence_variant": str(seq_var),
        "TR_ms": timing["TR_ms"],
        "TE_ms": timing["TE_ms"],
        "TI_ms": timing["TI_ms"],
        "flip_angle_deg": flip,
        "pixel_spacing_mm_row": ps_r,
        "pixel_spacing_mm_col": ps_c,
        "slice_thickness_mm": st,
        "acquisition_matrix": acqmat,
        "recon_matrix_rows": r_rows,
        "recon_matrix_cols": r_cols,
        "pixel_bandwidth_Hz": pbw,
        "coil_name": str(coil),
    }

# -------------------------
# Excel writer (single IO)
# -------------------------
def write_rows_to_excel(rows: List[Dict[str, Any]], out_xlsx: str) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Parameters"

    columns = [
        "case_id",
        "vendor",
        "model",
        "field_strength_T",
        "series_description",
        "sequence_name",
        "scanning_sequence",
        "sequence_variant",
        "TR_ms",
        "TE_ms",
        "TI_ms",
        "flip_angle_deg",
        "pixel_spacing_mm_row",
        "pixel_spacing_mm_col",
        "slice_thickness_mm",
        "acquisition_matrix",
        "recon_matrix_rows",
        "recon_matrix_cols",
        "pixel_bandwidth_Hz",
        "coil_name",
    ]

    ws.append(columns)
    for row in rows:
        ws.append([row.get(col, "NA") for col in columns])

    # Basic width tuning
    for i, col in enumerate(columns, start=1):
        w = min(40, max(14, len(col) + 2))
        ws.column_dimensions[chr(64 + i)].width = w

    wb.save(out_xlsx)

# -------------------------
# Per-case worker (thread)
# -------------------------
def process_case(case_root: str) -> Optional[Dict[str, Any]]:
    """
    Worker that selects a representative DICOM for a case and extracts metadata.
    Returns a dict (row) or None on failure.
    """
    case_id = os.path.basename(case_root)
    try:
        rep = choose_representative_file(case_root)
        if rep is None:
            print(f"  [WARN] {case_id}: no DICOM found")
            return None
        ds = read_header_minimal(rep)
        if ds is None:
            print(f"  [WARN] {case_id}: could not read header")
            return None
        md = extract_metadata(ds)
        row = {"case_id": case_id}
        row.update(md)
        return row
    except Exception:
        print(f"  [ERROR] {case_id}: unexpected exception")
        traceback.print_exc(limit=1)
        return None

# -------------------------
# Main
# -------------------------
def main() -> None:
    if not os.path.isdir(MASTER_DIR):
        print(f"ERROR: MASTER_DIR not found:\n  {MASTER_DIR}")
        sys.exit(1)

    print(f"Scanning for cases under:\n  {MASTER_DIR}\n")
    case_roots = find_case_roots_flexible(MASTER_DIR, CASE_PREFIX)
    if not case_roots:
        print("No candidate case folders with DICOM content were found.")
        sys.exit(1)

    if MAX_CASES is not None:
        case_roots = case_roots[:MAX_CASES]

    print(f"Found {len(case_roots)} case folder(s). Launching {MAX_WORKERS} threads...\n")

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fut_to_case = {ex.submit(process_case, cr): cr for cr in case_roots}
        done_cnt = 0
        for fut in as_completed(fut_to_case):
            cr = fut_to_case[fut]
            case_id = os.path.basename(cr)
            try:
                row = fut.result()
                if row is not None:
                    results.append(row)
            except Exception:
                print(f"[ERROR] case {case_id} crashed")
            finally:
                done_cnt += 1
                if done_cnt % 10 == 0 or done_cnt == len(case_roots):
                    print(f"  Progress: {done_cnt}/{len(case_roots)} cases")

    results.sort(key=lambda r: r.get("case_id", ""))

    write_rows_to_excel(results, OUTPUT_XLSX)
    print(f"\nDone. Wrote {len(results)} row(s) to:\n  {OUTPUT_XLSX}")

if __name__ == "__main__":
    main()
