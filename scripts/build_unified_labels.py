#!/usr/bin/env python3
"""Build unified label CSVs for BirdNET-family models.

For every model we produce one CSV with a fixed schema:
    class_idx, scientific_name, taxo_code, assets_id,
    <language columns...>, missing_langs

One row per model class, in model output order. Blank cells mean the
translation is absent in the source; ``missing_langs`` lists the codes that
were expected for that model but not found.

BSG v4.4 is additionally cross-filled from BirdNET V2.4 by scientific-name
match, so its 265 rows pick up all 38 language translations when V2.4 has
them. Source-provided names (English, Finnish) always win over cross-fills.
"""
from __future__ import annotations

import csv
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
V24_DIR = REPO / "BirdNET_models" / "labels_Birdnet_V2.4"
EUNA_CSV = REPO / "BirdNET_models" / "BirdNET+_V3.0-preview2_EUNA_1K_Labels.csv"
BSG_CSV = REPO / "BSG_v4_4" / "BSG_birds_Finland_v4_4_classes.csv"

V24_OUT = V24_DIR / "labels_unified.csv"
EUNA_OUT = REPO / "BirdNET_models" / "labels_BirdNET+_V3.0_EUNA_1K_unified.csv"
BSG_OUT = REPO / "BSG_v4_4" / "labels_BSG_v4_4_unified.csv"

# Canonical language column order for every output file.
LANG_COLS = [
    "en", "en_uk", "af", "ar", "bg", "ca", "cs", "da", "de", "el", "es",
    "fi", "fr", "he", "hr", "hu", "in", "is", "it", "ja", "ko", "lt", "ml",
    "nl", "no", "pl", "pt_BR", "pt_PT", "ro", "ru", "sk", "sl", "sr", "sv",
    "th", "tr", "uk", "zh",
]

BASE_COLS = ["class_idx", "scientific_name", "taxo_code", "assets_id"]
HEADER = BASE_COLS + LANG_COLS + ["missing_langs"]


def _read_lines(path: Path) -> list[str]:
    """Read a one-value-per-line file and return values in row order."""
    with path.open(encoding="utf-8") as fh:
        return [line.rstrip("\n") for line in fh if line.rstrip("\n")]


def build_v24() -> tuple[list[dict], dict[str, dict]]:
    """Parse BirdNET V2.4 labels. Returns (rows, sci_index)."""
    per_lang: dict[str, list[str]] = {}
    for code in LANG_COLS:
        path = V24_DIR / f"labels_{code}.txt"
        if not path.exists():
            continue
        per_lang[code] = _read_lines(path)

    taxo = _read_lines(V24_DIR / "taxo_code.txt")
    assets = _read_lines(V24_DIR / "assets.txt")

    n = len(per_lang["en"])
    for code, values in per_lang.items():
        assert len(values) == n, f"V2.4 {code}: {len(values)} != {n}"
    assert len(taxo) == n and len(assets) == n

    rows: list[dict] = []
    sci_index: dict[str, dict] = {}
    for i in range(n):
        sci_common = per_lang["en"][i]
        sci, _, _common = sci_common.partition("_")
        row = {
            "class_idx": i,
            "scientific_name": sci,
            "taxo_code": taxo[i],
            "assets_id": assets[i],
        }
        for code in LANG_COLS:
            if code in per_lang:
                value = per_lang[code][i]
                _sci_c, _, common = value.partition("_")
                row[code] = common
            else:
                row[code] = ""
        rows.append(row)
        # Keep first occurrence; V2.4 has unique sci names in practice.
        sci_index.setdefault(sci, row)
    return rows, sci_index


def build_euna() -> list[dict]:
    rows: list[dict] = []
    with EUNA_CSV.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for i, src in enumerate(reader):
            sci = (src.get("sci_name") or "").strip()
            com = (src.get("com_name") or "").strip()
            # Source duplicates sci into com_name when EN is unknown.
            en = "" if com == sci else com
            row = {
                "class_idx": i,
                "scientific_name": sci,
                "taxo_code": (src.get("id") or "").strip(),
                "assets_id": "",
            }
            for code in LANG_COLS:
                row[code] = ""
            row["en"] = en
            rows.append(row)
    return rows


def build_bsg(v24_sci_index: dict[str, dict]) -> list[dict]:
    raw: list[tuple[int, dict]] = []
    with BSG_CSV.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for src in reader:
            try:
                cls = int((src.get("class") or "").strip())
            except ValueError:
                continue
            sci = (src.get("scientific_name") or "").strip()
            en = (src.get("common_name") or "").strip()
            fi = (src.get("suomenkielinen_nimi") or "").strip()
            row = {
                "class_idx": cls,
                "scientific_name": sci,
                "taxo_code": (src.get("species_code") or "").strip(),
                "assets_id": "",
            }
            for code in LANG_COLS:
                row[code] = ""
            row["en"] = en
            row["fi"] = fi
            # Cross-fill from V2.4 by scientific name.
            match = v24_sci_index.get(sci)
            if match is not None:
                for code in LANG_COLS:
                    if not row[code] and match.get(code):
                        row[code] = match[code]
            raw.append((cls, row))
    raw.sort(key=lambda t: t[0])
    # Reindex class_idx to be contiguous 0..N-1 in case of gaps.
    rows = []
    for new_idx, (_old, row) in enumerate(raw):
        row["class_idx"] = new_idx
        rows.append(row)
    return rows


def finalize(rows: list[dict], expected_langs: set[str]) -> None:
    """Compute ``missing_langs`` for each row over the expected set."""
    for row in rows:
        missing = [c for c in LANG_COLS if c in expected_langs and not row[c]]
        row["missing_langs"] = ",".join(missing)


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in HEADER})


def main() -> None:
    v24_rows, v24_sci_index = build_v24()
    finalize(v24_rows, set(LANG_COLS))
    write_csv(V24_OUT, v24_rows)
    print(f"wrote {V24_OUT} ({len(v24_rows)} rows)")

    euna_rows = build_euna()
    finalize(euna_rows, {"en"})
    write_csv(EUNA_OUT, euna_rows)
    print(f"wrote {EUNA_OUT} ({len(euna_rows)} rows)")

    bsg_rows = build_bsg(v24_sci_index)
    # After cross-fill we expect every V2.4 language when a match existed;
    # for unmatched species (e.g. "No bird") only en+fi were ever expected.
    for row in bsg_rows:
        if row["scientific_name"] in v24_sci_index:
            expected = set(LANG_COLS)
        else:
            expected = {"en", "fi"}
        missing = [c for c in LANG_COLS if c in expected and not row[c]]
        row["missing_langs"] = ",".join(missing)
    write_csv(BSG_OUT, bsg_rows)
    filled = sum(1 for r in bsg_rows if r["scientific_name"] in v24_sci_index)
    print(f"wrote {BSG_OUT} ({len(bsg_rows)} rows, {filled} cross-filled from V2.4)")


if __name__ == "__main__":
    main()
