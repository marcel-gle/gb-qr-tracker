from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import CampaignConfig, ScoreConfig
from .pipeline import CampaignPipeline

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Campaign list processing pipeline")
    p.add_argument("campaign_dir", type=Path)
    p.add_argument("base_name", help="File prefix for staged CSVs")
    p.add_argument(
        "step",
        choices=[
            "merge",
            "dedupe-domain",
            "score",
            "imprint",
            "dedupe-address",
            "final",
            "status",
        ],
    )
    p.add_argument("--incoming", nargs="*", type=Path, help="CSV files for merge step")
    p.add_argument("--append-new", action="store_true")
    p.add_argument("--scoring-prompt", default="handwerk_analysis")
    p.add_argument("--backend", choices=["local", "openai"], default="local")
    p.add_argument("--pass-threshold", type=float, default=None)
    p.add_argument("--only-new", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    score_cfg = ScoreConfig()
    if args.pass_threshold is not None:
        score_cfg.pass_threshold = args.pass_threshold
    config = CampaignConfig(
        campaign_dir=args.campaign_dir,
        base_name=args.base_name,
        backend=args.backend,
        scoring_prompt_name=args.scoring_prompt,
        score_config=score_cfg,
    )
    pipe = CampaignPipeline(config)
    pipe.ensure_campaign_dirs()

    if args.step == "merge":
        files = args.incoming or pipe.list_incoming_csvs()
        print(pipe.merge_raw(files, append_only_new=args.append_new))
    elif args.step == "dedupe-domain":
        print(pipe.dedupe_domain())
    elif args.step == "score":
        print(pipe.score(only_new=args.only_new))
    elif args.step == "imprint":
        print(pipe.imprint(only_new=args.only_new))
    elif args.step == "dedupe-address":
        print(pipe.dedupe_address())
    elif args.step == "final":
        print(pipe.final_review())
    elif args.step == "status":
        print(pipe.funnel_status())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
