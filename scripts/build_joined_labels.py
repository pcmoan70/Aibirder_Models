#!/usr/bin/env python3
"""Full outer join of the three label sources on scientific name.

Produces ``labels_unified.csv`` at the repo root. Columns from each source
are prefixed with the source tag (``v24_``, ``euna_``, ``bsg_``) so that
duplicate names (e.g. ``class``) never collide. A ``sources`` column lists
which files contributed to each row.
"""
from __future__ import annotations

import csv
from pathlib import Path

from build_unified_labels import (
    BSG_CSV,
    EUNA_CSV,
    REPO,
    V24_OUT,
    build_v24,
    finalize,
    LANG_COLS,
)

OUT = REPO / "labels_unified.csv"
AVILIST_CSV = REPO / "AviList2025NNKFkomplett.csv"

# Column orders from each source (join key is excluded; prefixes are added
# later). V2.4 comes from the unified file we produced earlier.
V24_FIELDS = [
    "class_idx", "taxo_code", "assets_id", *LANG_COLS, "missing_langs",
]
EUNA_FIELDS = ["id", "com_name", "gbif", "class", "order"]
BSG_FIELDS = [
    "species_code", "common_name", "luomus_name", "suomenkielinen_nimi",
    "class",
]
AVILIST_FIELDS = [
    "Sequence",
    "Order",
    "ordensrelasjon::orden_norsknavn",
    "Family",
    "familierelasjon::familie_norsknavn",
    "norskAviListv1",
    "English_name_AviList",
]


def _prefix(fields: list[str], tag: str) -> list[str]:
    return [f"{tag}_{f}" for f in fields]


def load_v24() -> dict[str, dict]:
    """Return {sci_name: {v24_*: value}} from the V2.4 unified CSV."""
    if not V24_OUT.exists():
        # Fall back to rebuilding in memory if the file isn't on disk yet.
        rows, _idx = build_v24()
        finalize(rows, set(LANG_COLS))
        source = rows
    else:
        with V24_OUT.open(encoding="utf-8", newline="") as fh:
            source = list(csv.DictReader(fh))

    out: dict[str, dict] = {}
    for row in source:
        sci = (row.get("scientific_name") or "").strip()
        if not sci:
            continue
        out[sci] = {f"v24_{f}": row.get(f, "") for f in V24_FIELDS}
    return out


def load_euna() -> dict[str, dict]:
    out: dict[str, dict] = {}
    with EUNA_CSV.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        for src in reader:
            sci = (src.get("sci_name") or "").strip()
            if not sci:
                continue
            out[sci] = {f"euna_{f}": (src.get(f) or "").strip() for f in EUNA_FIELDS}
    return out


def load_avilist() -> dict[str, dict]:
    out: dict[str, dict] = {}
    with AVILIST_CSV.open(encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for src in reader:
            sci = (src.get("Scientific_name") or "").strip()
            if not sci:
                continue
            out[sci] = {
                f"avilist_{f}": (src.get(f) or "").strip()
                for f in AVILIST_FIELDS
            }
    return out


def load_bsg() -> dict[str, dict]:
    out: dict[str, dict] = {}
    with BSG_CSV.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for src in reader:
            sci = (src.get("scientific_name") or "").strip()
            if not sci:
                continue
            out[sci] = {f"bsg_{f}": (src.get(f) or "").strip() for f in BSG_FIELDS}
    return out


def main() -> None:
    v24 = load_v24()
    euna = load_euna()
    bsg = load_bsg()
    avilist = load_avilist()

    header = (
        ["scientific_name"]
        + _prefix(V24_FIELDS, "v24")
        + _prefix(EUNA_FIELDS, "euna")
        + _prefix(BSG_FIELDS, "bsg")
        + _prefix(AVILIST_FIELDS, "avilist")
        + ["sources"]
    )

    all_sci = sorted(set(v24) | set(euna) | set(bsg) | set(avilist))

    with OUT.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        for sci in all_sci:
            row: dict[str, str] = {"scientific_name": sci}
            sources = []
            if sci in v24:
                row.update(v24[sci])
                sources.append("v24")
            if sci in euna:
                row.update(euna[sci])
                sources.append("euna")
            if sci in bsg:
                row.update(bsg[sci])
                sources.append("bsg")
            if sci in avilist:
                row.update(avilist[sci])
                sources.append("avilist")
            row["sources"] = ",".join(sources)
            writer.writerow({k: row.get(k, "") for k in header})

    print(f"wrote {OUT}")
    print(f"  total unique scientific names: {len(all_sci)}")
    print(f"  v24 rows matched:     {len(set(v24) & set(all_sci))}")
    print(f"  euna rows matched:    {len(set(euna) & set(all_sci))}")
    print(f"  bsg rows matched:     {len(set(bsg) & set(all_sci))}")
    print(f"  avilist rows matched: {len(set(avilist) & set(all_sci))}")
    print()
    print("  v24 ∩ avilist:        ", len(set(v24) & set(avilist)))
    print("  euna ∩ avilist:       ", len(set(euna) & set(avilist)))
    print("  bsg ∩ avilist:        ", len(set(bsg) & set(avilist)))
    print("  avilist-only:         ", len(set(avilist) - set(v24) - set(euna) - set(bsg)))


if __name__ == "__main__":
    main()
