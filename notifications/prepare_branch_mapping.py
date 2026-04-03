import argparse
from pathlib import Path

import pandas as pd

try:
    from .branch_mapping_store import load_branch_maps_from_qdrant
except ImportError:
    from notifications.branch_mapping_store import load_branch_maps_from_qdrant


def _to_norm(v) -> str:
    return str(v).strip().lower()


def merge_branch_into_catalog(catalog_path: str, branch_path: str, output_path: str) -> dict:
    catalog = pd.read_excel(catalog_path)

    use_qdrant = str(branch_path or "").strip().lower() in {"", "__qdrant__", "qdrant"}

    if use_qdrant:
        branch_map_id, branch_map_code, source_points = load_branch_maps_from_qdrant()
    else:
        branch = pd.read_excel(branch_path)
        branch = branch.copy()

        branch_map_id = {
            _to_norm(row["Content_id"]): str(row["Branch"]).strip()
            for _, row in branch.iterrows()
            if str(row.get("Content_id", "")).strip() and str(row.get("Branch", "")).strip()
        }
        branch_map_code = {
            _to_norm(row["code"]): str(row["Branch"]).strip()
            for _, row in branch.iterrows()
            if str(row.get("code", "")).strip() and str(row.get("Branch", "")).strip()
        }
        source_points = len(branch_map_id) + len(branch_map_code)

    catalog = catalog.copy()

    if "creator_region" not in catalog.columns:
        catalog["creator_region"] = ""
    else:
        catalog["creator_region"] = catalog["creator_region"].fillna("").astype(str)

    mapped = 0
    missing = 0

    for idx, row in catalog.iterrows():
        video_id = _to_norm(row.get("video_id", ""))
        code = _to_norm(row.get("code", ""))
        branch_name = branch_map_id.get(video_id) or branch_map_code.get(code)
        if branch_name:
            catalog.at[idx, "creator_region"] = str(branch_name)
            mapped += 1
        else:
            missing += 1

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    catalog.to_excel(output_path, index=False)

    return {
        "rows": len(catalog),
        "mapped": mapped,
        "missing": missing,
        "mapping_source": "qdrant" if use_qdrant else branch_path,
        "mapping_points": source_points,
        "output": output_path,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Map Branch values into creator_region")
    parser.add_argument("--branch", default="__qdrant__")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    stats = merge_branch_into_catalog(args.catalog, args.branch, args.output)
    print(
        f"Mapped creator_region | rows={stats['rows']} | mapped={stats['mapped']} | "
        f"missing={stats['missing']} | output={stats['output']}"
    )


if __name__ == "__main__":
    main()
