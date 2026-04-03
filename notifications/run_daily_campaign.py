import argparse
import json
from pathlib import Path

import pandas as pd

from notifications.models import NotificationRequest
from notifications.service import NotificationService


def parse_args():
    parser = argparse.ArgumentParser(description="Generate daily campaign notifications in bulk")
    parser.add_argument("--input", required=True, help="Input CSV/XLSX with user rows")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    parser.add_argument("--campaign-day", type=int, choices=[1, 2, 3, 4, 7, 12], required=True)
    parser.add_argument("--default-language", default="en")
    return parser.parse_args()


def read_table(path: str) -> pd.DataFrame:
    p = Path(path)
    if p.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def main():
    args = parse_args()
    df = read_table(args.input).fillna("")

    required_cols = ["user_id", "user_name", "region"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    service = NotificationService()

    reqs = []
    for _, row in df.iterrows():
        reqs.append(
            NotificationRequest(
                user_id=int(row["user_id"]),
                user_name=str(row["user_name"]),
                region=str(row["region"]),
                language=str(row.get("language", args.default_language) or args.default_language),
                campaign_day=args.campaign_day,
                video_id=str(row.get("video_id", "") or "") or None,
                video_title=str(row.get("video_title", "") or "") or None,
                creator_name=str(row.get("creator_name", "") or "") or None,
                creator_region=str(row.get("creator_region", "") or "") or None,
                creator_team=str(row.get("creator_team", "") or "") or None,
                outcome_hint=str(row.get("outcome_hint", "") or "") or None,
            )
        )

    out = service.build_notifications_batch(reqs)

    payload = {
        "total": out.total,
        "results": [item.model_dump() for item in out.results],
    }

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Generated {out.total} notifications -> {args.output}")


if __name__ == "__main__":
    main()
