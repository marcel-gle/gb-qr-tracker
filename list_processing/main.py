from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .config import ListProcessingConfig
from .logging_config import configure_logging
from .pipeline import ListProcessingPipeline


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the list_processing pipeline on an input CSV file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with local ML Studio backend
  python -m list_processing.main input.csv output.csv

  # Use OpenAI backend
  python -m list_processing.main input.csv output.csv --backend openai

  # Disable domain scoring
  python -m list_processing.main input.csv output.csv --no-scoring

  # Produce lettershop-style output
  python -m list_processing.main input.csv output.csv --output-schema lettershop
        """,
    )

    parser.add_argument("input_csv", help="Path to input CSV file")
    parser.add_argument("output_csv", help="Path to output CSV file")

    parser.add_argument(
        "--backend",
        choices=["local", "openai"],
        default="local",
        help="LLM backend to use (default: local)",
    )
    parser.add_argument(
        "--local-model",
        default=None,
        help=(
            "Model name for the local backend (overrides LOCAL_MODEL env var), "
            "e.g. 'Qwen/Qwen3-4B-MLX-4bit'"
        ),
    )
    parser.add_argument(
        "--max-workers-http",
        type=int,
        default=10,
        help="Maximum number of concurrent HTTP workers (default: 10)",
    )
    parser.add_argument(
        "--max-workers-llm",
        type=int,
        default=5,
        help="Maximum number of concurrent LLM requests (default: 5)",
    )
    parser.add_argument(
        "--scoring-prompt",
        default="handwerk_analysis",
        help="Name of the scoring prompt to use (default: handwerk_analysis)",
    )
    parser.add_argument(
        "--output-schema",
        choices=["internal_enriched", "lettershop"],
        default="internal_enriched",
        help="Output schema to use (default: lettershop)",
    )
    parser.add_argument(
        "--no-enrichment",
        action="store_true",
        help="Skip the enrichment (imprint) step",
    )
    parser.add_argument(
        "--no-salutation",
        action="store_true",
        help="Skip the salutation inference step",
    )
    parser.add_argument(
        "--no-scoring",
        action="store_true",
        help="Skip the domain scoring step",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume from existing checkpoints, always start fresh",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv

    parser = build_arg_parser()
    args = parser.parse_args(argv[1:])

    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)

    if not input_path.exists():
        print(f"❌ Error: Input file does not exist: {input_path}")
        return 1

    log_level = logging.DEBUG if args.verbose else logging.INFO
    configure_logging(level=log_level)

    config = ListProcessingConfig(
        input_path=input_path,
        output_path=output_path,
        backend=args.backend,
        local_model=args.local_model,
        max_workers_http=args.max_workers_http,
        max_workers_llm=args.max_workers_llm,
        scoring_prompt_name=args.scoring_prompt,
        enable_enrichment=not args.no_enrichment,
        enable_salutation=not args.no_salutation,
        enable_scoring=not args.no_scoring,
        resume=not args.no_resume,
        output_schema=args.output_schema,
    )

    llm = ListProcessingPipeline.create_llm_from_config(config)
    pipeline = ListProcessingPipeline(config, llm)

    try:
        pipeline.run()
        return 0
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        return 1
    except Exception as exc:  # pragma: no cover - defensive
        logging.getLogger(__name__).error(
            "Fatal error in list_processing pipeline: %s", exc, exc_info=True
        )
        print(f"\n\n❌ Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

