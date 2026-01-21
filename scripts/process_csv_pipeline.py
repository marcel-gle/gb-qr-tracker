"""
Complete CSV processing pipeline that combines:
1. Data enrichment from website imprints (scrape_oceanio_ai.py)
2. Filling missing salutations (fill_anrede_with_llm.py)
3. Cleaning and normalization (clean_csv.py)

Usage:
    python scripts/process_csv_pipeline.py input.csv output.csv [options]
"""

import sys
import argparse
import tempfile
from pathlib import Path
from typing import Optional

# Import the main functions from each script
# Note: These imports assume the scripts are in the same directory
try:
    from scrape_oceanio_ai import enrich_with_gpt, transform_csv_to_new_format
    from fill_anrede_with_llm import process_csv as fill_anrede
    from clean_csv import process_csv as clean_csv, print_statistics
except ImportError:
    # Fallback: add scripts directory to path
    import os
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from scrape_oceanio_ai import enrich_with_gpt, transform_csv_to_new_format
    from fill_anrede_with_llm import process_csv as fill_anrede
    from clean_csv import process_csv as clean_csv, print_statistics


def run_pipeline(
    input_csv: str,
    output_csv: str,
    transform: bool = True,
    max_workers: Optional[int] = None,
    keep_intermediate: bool = False,
) -> int:
    """
    Run the complete CSV processing pipeline.
    
    Args:
        input_csv: Path to input CSV file
        output_csv: Path to final output CSV file
        transform: Whether to transform to standardized format (default: True)
        max_workers: Maximum number of concurrent workers for enrichment
        keep_intermediate: Keep intermediate files for debugging
    
    Returns:
        0 on success, 1 on error
    """
    input_path = Path(input_csv)
    output_path = Path(output_csv)
    
    if not input_path.exists():
        print(f"❌ Error: Input file does not exist: {input_path}")
        return 1
    
    print("=" * 80)
    print("CSV PROCESSING PIPELINE")
    print("=" * 80)
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Transform: {transform}")
    print(f"Max workers: {max_workers or 'default'}")
    print("=" * 80)
    print()
    
    # Create temporary directory for intermediate files
    temp_dir = Path(tempfile.mkdtemp(prefix="csv_pipeline_"))
    if keep_intermediate:
        temp_dir = output_path.parent / f"{output_path.stem}_intermediate"
        temp_dir.mkdir(exist_ok=True)
        print(f"📁 Intermediate files will be kept in: {temp_dir}")
    else:
        print(f"📁 Using temporary directory: {temp_dir}")
    print()
    
    try:
        # Step 1: Enrichment
        print("=" * 80)
        print("STEP 1: Data Enrichment (scrape_oceanio_ai.py)")
        print("=" * 80)
        enriched_csv = temp_dir / "01_enriched.csv"
        
        try:
            enrich_with_gpt(str(input_path), str(enriched_csv), max_workers=max_workers)
            print(f"✅ Step 1 complete: {enriched_csv}")
        except Exception as e:
            print(f"❌ Step 1 failed: {e}")
            return 1
        print()
        
        # Step 2: Transformation (if requested)
        if transform:
            print("=" * 80)
            print("STEP 2: Format Transformation")
            print("=" * 80)
            transformed_csv = temp_dir / "02_transformed.csv"
            
            try:
                transform_csv_to_new_format(str(enriched_csv), str(transformed_csv))
                print(f"✅ Step 2 complete: {transformed_csv}")
                current_csv = transformed_csv
            except Exception as e:
                print(f"❌ Step 2 failed: {e}")
                return 1
            print()
        else:
            current_csv = enriched_csv
        
        # Step 3: Fill missing salutations
        print("=" * 80)
        print("STEP 3: Fill Missing Salutations (fill_anrede_with_llm.py)")
        print("=" * 80)
        with_anrede_csv = temp_dir / "03_with_anrede.csv"
        
        try:
            fill_anrede(current_csv, with_anrede_csv)
            print(f"✅ Step 3 complete: {with_anrede_csv}")
        except Exception as e:
            print(f"❌ Step 3 failed: {e}")
            return 1
        print()
        
        # Step 4: Clean and normalize
        print("=" * 80)
        print("STEP 4: Clean and Normalize (clean_csv.py)")
        print("=" * 80)
        
        try:
            stats = clean_csv(with_anrede_csv, output_path)
            print_statistics(stats)
            print(f"✅ Step 4 complete: {output_path}")
        except Exception as e:
            print(f"❌ Step 4 failed: {e}")
            return 1
        print()
        
        # Summary
        print("=" * 80)
        print("PIPELINE COMPLETE")
        print("=" * 80)
        print(f"✅ Final output: {output_path}")
        if keep_intermediate:
            print(f"📁 Intermediate files: {temp_dir}")
        else:
            print(f"📁 Intermediate files cleaned up")
        print("=" * 80)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Pipeline interrupted by user")
        if keep_intermediate:
            print(f"📁 Intermediate files preserved in: {temp_dir}")
        return 1
    except Exception as e:
        print(f"\n\n❌ Pipeline failed with error: {e}")
        import traceback
        traceback.print_exc()
        if keep_intermediate:
            print(f"📁 Intermediate files preserved in: {temp_dir}")
        return 1
    finally:
        # Clean up temporary directory if not keeping intermediate files
        if not keep_intermediate and temp_dir.exists():
            import shutil
            try:
                shutil.rmtree(temp_dir)
            except Exception:
                pass  # Ignore cleanup errors


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Complete CSV processing pipeline: enrichment → transformation → salutation filling → cleaning",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline with transformation
  python scripts/process_csv_pipeline.py input.csv output.csv
  
  # Without transformation (keep internal format)
  python scripts/process_csv_pipeline.py input.csv output.csv --no-transform
  
  # With custom worker count and keep intermediate files
  python scripts/process_csv_pipeline.py input.csv output.csv --max-workers 20 --keep-intermediate
        """
    )
    
    parser.add_argument(
        "input_csv",
        help="Path to input CSV file (must have 'Company' and 'Domain' columns)"
    )
    parser.add_argument(
        "output_csv",
        help="Path to final output CSV file"
    )
    parser.add_argument(
        "--no-transform",
        action="store_true",
        help="Skip format transformation step (keep internal format instead of standardized format)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Maximum number of concurrent workers for enrichment step (default: 10)"
    )
    parser.add_argument(
        "--keep-intermediate",
        action="store_true",
        help="Keep intermediate CSV files for debugging (saved in output directory)"
    )
    
    args = parser.parse_args(argv[1:])
    
    return run_pipeline(
        input_csv=args.input_csv,
        output_csv=args.output_csv,
        transform=not args.no_transform,
        max_workers=args.max_workers,
        keep_intermediate=args.keep_intermediate,
    )


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

