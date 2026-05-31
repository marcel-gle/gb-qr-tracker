"""
Analyze domains from CSV to determine if they belong to specific craft/trade categories.

This script:
1. Reads a CSV file with a domain column
2. Fetches the homepage content for each domain
3. Sends it to a local ML Studio model for analysis
4. Returns a match_score (0-5) indicating how likely the website belongs to specific craft categories
5. Writes results back to the CSV

Usage:
    python scripts/analyze_handwerk_domains.py input.csv output.csv [--domain-column DOMAIN]
"""

import io
import os
import re
import sys
import csv
import json
import time
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore

import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from tqdm import tqdm

from prompt_manager import get_prompt, list_prompts, load_prompt_from_file, Prompt

# ---------------- Logging Configuration ----------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Try to load from .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    pass

# ---------------- Local ML Studio client ----------------

ML_STUDIO_BASE_URL = os.environ.get("ML_STUDIO_BASE_URL", "http://localhost:1234/v1")
LOCAL_MODEL = os.environ.get("LOCAL_MODEL", "openai/gpt-oss-20b")  # ML Studio model name

# Initialize local model client (no API key needed for local models)
local_client = OpenAI(
    base_url=ML_STUDIO_BASE_URL,
    api_key="not-needed"  # ML Studio doesn't require a real API key
)

# ---------------- HTTP / scraping helpers ----------------

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
REQUEST_TIMEOUT = 10
MAX_WORKERS_HTTP = 10  # concurrent HTTP requests
MAX_WORKERS_LLM = 5  # concurrent LLM API calls

# Enforce LLM concurrency limit
LLM_SEMAPHORE = Semaphore(MAX_WORKERS_LLM)


def fetch_url(url: str) -> Optional[requests.Response]:
    """Fetch a URL and return the response if successful."""
    try:
        logger.debug(f"Fetching URL: {url}")
        resp = requests.get(
            url,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
            logger.debug(f"Successfully fetched {url} ({len(resp.text)} bytes)")
            return resp
        else:
            logger.debug(f"Failed to fetch {url}: status={resp.status_code}, content-type={resp.headers.get('Content-Type')}")
    except requests.RequestException as e:
        logger.debug(f"Request exception for {url}: {e}")
    return None


def normalize_domain(domain: str) -> Optional[str]:
    """
    Normalize a domain (which may already include scheme/path) and
    return the first reachable base URL (https or http).
    """
    domain = domain.strip()
    if not domain:
        logger.debug("Empty domain provided")
        return None

    # If domain already includes a scheme, strip it
    if domain.startswith("http://"):
        domain = domain[len("http://") :]
    elif domain.startswith("https://"):
        domain = domain[len("https://") :]

    # Strip everything after the host (paths, query, etc.)
    domain = domain.split("/")[0].rstrip("/")
    logger.debug(f"Normalized domain: {domain}")

    # Try https first, then http
    for scheme in ("https://", "http://"):
        url = scheme + domain
        resp = fetch_url(url)
        if resp:
            logger.info(f"Found accessible URL for {domain}: {resp.url}")
            return resp.url
    
    logger.warning(f"Could not reach domain: {domain}")
    return None


def extract_homepage_text(domain: str) -> Optional[str]:
    """Extract visible text from the homepage of a domain."""
    logger.debug(f"Extracting homepage text for {domain}")
    base_url = normalize_domain(domain)
    if not base_url:
        logger.warning(f"Could not normalize domain: {domain}")
        return None

    resp = fetch_url(base_url)
    if not resp:
        logger.warning(f"Could not fetch homepage for: {domain}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()
    
    # Get text content
    text = soup.get_text(separator="\n")
    
    # Clean up whitespace
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = "\n".join(chunk for chunk in chunks if chunk)
    
    # Limit text length to avoid token limits
    max_chars = 14000
    original_len = len(text)
    if len(text) > max_chars:
        text = text[:max_chars]
        logger.debug(f"Truncated homepage text from {original_len} to {max_chars} chars for {domain}")
    
    logger.info(f"Extracted {len(text)} chars of text from {domain}")
    return text


# ---------------- LLM Analysis ----------------


def extract_json_from_response(content: str) -> Optional[Dict[str, Any]]:
    """Extract JSON object from LLM response, handling markdown code blocks."""
    content_clean = content.strip()
    
    # Remove markdown code blocks if present
    if content_clean.startswith("```json"):
        content_clean = content_clean[7:]  # Remove ```json
    elif content_clean.startswith("```"):
        content_clean = content_clean[3:]  # Remove ```
    
    if content_clean.endswith("```"):
        content_clean = content_clean[:-3]  # Remove closing ```
    
    content_clean = content_clean.strip()
    
    # Try to find JSON object in the response
    start_idx = content_clean.find('{')
    end_idx = content_clean.rfind('}')
    
    if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
        json_str = content_clean[start_idx:end_idx + 1]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None
    return None


def _sanitize_analysis_key(value: str) -> str:
    key = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip()).strip("_").lower()
    return key or "value"


def flatten_analysis_result(result: Any, prefix: str = "analysis") -> Dict[str, Any]:
    flat: Dict[str, Any] = {}

    def _walk(value: Any, path: List[str]) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                _walk(child, path + [_sanitize_analysis_key(str(key))])
            return

        flat_key = prefix if not path else f"{prefix}_{'_'.join(path)}"
        if isinstance(value, list):
            flat[flat_key] = json.dumps(value, ensure_ascii=False)
            return

        flat[flat_key] = "" if value is None else value

    if isinstance(result, dict):
        _walk(result, [])

    return flat


def apply_flat_analysis_result(row: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    row["analysis_result"] = json.dumps(result, ensure_ascii=False)

    flat = flatten_analysis_result(result)
    row.update(flat)

    score_value = result.get("match_score")
    if score_value is None:
        score_value = result.get("score")
    row["match_score"] = "" if score_value is None else str(score_value)

    return flat


def analyze_domain_with_llm(domain: str, homepage_text: str, prompt: Prompt) -> Optional[Dict[str, Any]]:
    """
    Analyze a domain's homepage content using the local LLM model.
    Returns the parsed JSON response or None if analysis fails.
    """
    logger.debug(f"Analyzing {domain} with LLM using prompt '{prompt.name}' (text length: {len(homepage_text)})")
    
    # Format user prompt with domain and homepage text
    user_prompt = prompt.format_user_prompt(
        domain=domain,
        homepage_text=homepage_text,
        gegenstand="(nicht angegeben)",
    )

    # Enforce LLM concurrency limit
    with LLM_SEMAPHORE:
        try:
            start_time = time.time()
            response = local_client.chat.completions.create(
                model=LOCAL_MODEL,
                temperature=0.3,  # Lower temperature for more consistent scoring
                messages=[
                    {"role": "system", "content": prompt.system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            elapsed = time.time() - start_time
            logger.debug(f"LLM call for {domain} took {elapsed:.2f}s")

            content = response.choices[0].message.content.strip()
            
            # Extract JSON from response
            data = extract_json_from_response(content)
            
            if data:
                logger.info(f"Successfully analyzed {domain} with prompt '{prompt.name}'")
                return data
            else:
                logger.warning(f"Could not extract JSON from LLM response for {domain}: {content[:200]}...")
                return None
                
        except Exception as e:
            logger.error(f"Error calling LLM for {domain}: {e}", exc_info=True)
            return None


def process_domain(domain: str, prompt: Prompt) -> Dict[str, Any]:
    """
    Process a single domain: fetch homepage and analyze with LLM.
    Returns a dict with domain, analysis result, and status.
    """
    if not domain or not domain.strip():
        logger.debug(f"Skipping empty domain")
        return {
            "domain": domain,
            "result": None,
            "status": "empty_domain",
            "error": None
        }
    
    domain = domain.strip()
    logger.debug(f"Processing domain: {domain}")
    
    # Fetch homepage content
    homepage_text = extract_homepage_text(domain)
    if not homepage_text:
        logger.warning(f"Failed to extract homepage text for {domain}")
        return {
            "domain": domain,
            "result": None,
            "status": "fetch_failed",
            "error": "Could not fetch homepage"
        }
    
    # Analyze with LLM
    result = analyze_domain_with_llm(domain, homepage_text, prompt)
    if result is None:
        logger.warning(f"LLM analysis failed for {domain}")
        return {
            "domain": domain,
            "result": None,
            "status": "analysis_failed",
            "error": "LLM analysis failed"
        }
    
    logger.info(f"Successfully processed {domain}")
    return {
        "domain": domain,
        "result": result,
        "status": "success",
        "error": None
    }


def detect_delimiter(file_path: Path) -> str:
    """
    Detect CSV delimiter.

    Prefer the delimiter that yields the widest first row via csv.reader, so
    semicolon-separated files are not misread as comma (one synthetic column
    named \"query;title;url;...\").
    Falls back to csv.Sniffer, then raw delimiter counts on the first line.
    """
    candidates = [",", ";", "\t", "|"]
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(65536)
    if not sample.strip():
        return ","

    best_delimiter = ","
    best_width = 0
    for delim in candidates:
        try:
            reader = csv.reader(io.StringIO(sample), delimiter=delim)
            row = next(reader)
        except (StopIteration, csv.Error):
            continue
        width = len(row)
        if width > best_width:
            best_width = width
            best_delimiter = delim

    if best_width > 1:
        return best_delimiter

    try:
        dialect = csv.Sniffer().sniff(sample, delimiters="".join(candidates))
        d = dialect.delimiter
        if d in candidates:
            reader = csv.reader(io.StringIO(sample), delimiter=d)
            row = next(reader)
            if len(row) > best_width:
                return d
    except Exception:
        pass

    first_line = sample.splitlines()[0] if sample.splitlines() else ""
    counts = {d: first_line.count(d) for d in candidates}
    if counts and max(counts.values()) > 0:
        return max(counts, key=counts.get)
    return ","


def process_csv(
    input_path: Path,
    output_path: Path,
    prompt: Prompt,
    domain_column: str = "domain",
    max_workers: int = 10,
    skip_existing: bool = True,
    limit: Optional[int] = None,
) -> None:
    """
    Process CSV file: analyze domains and write results.
    
    Args:
        input_path: Path to input CSV file
        output_path: Path to output CSV file
        prompt: Prompt to use for analysis
        domain_column: Name of the column containing domains
        max_workers: Maximum number of concurrent workers
        skip_existing: Skip rows that already have results
        limit: Maximum number of rows to process (None = no limit)
    """
    logger.info(f"Starting CSV processing: input={input_path}, output={output_path}, prompt={prompt.name}")
    print(f"Loading CSV from {input_path}")
    print(f"Using prompt: {prompt.name} (v{prompt.version})")
    if prompt.description:
        print(f"  {prompt.description}")
    
    delimiter = detect_delimiter(input_path)
    logger.info(f"Detected CSV delimiter: '{delimiter}'")
    print(f"Detected delimiter: '{delimiter}'")
    
    # Read CSV
    rows = []
    with open(input_path, "r", encoding="utf-8-sig", newline="") as infile:
        reader = csv.DictReader(infile, delimiter=delimiter)
        fieldnames = list(reader.fieldnames or [])
        
        logger.debug(f"CSV columns: {fieldnames}")
        
        # Check if domain column exists
        if domain_column not in fieldnames:
            # Try case-insensitive search
            domain_col_lower = domain_column.lower()
            for col in fieldnames:
                if col.lower() == domain_col_lower:
                    logger.info(f"Found domain column (case-insensitive): '{col}' (requested: '{domain_column}')")
                    domain_column = col
                    break
            else:
                error_msg = f"Column '{domain_column}' not found in CSV. Available columns: {fieldnames}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        # Ensure output columns exist
        if "analysis_result" not in fieldnames:
            fieldnames.append("analysis_result")
        if "analysis_status" not in fieldnames:
            fieldnames.append("analysis_status")
        if "analysis_error" not in fieldnames:
            fieldnames.append("analysis_error")
        
        # Always expose match_score as a flat column when present in LLM JSON
        # ("match_score" or alias "score" for 0–10 style prompts).
        if "match_score" not in fieldnames:
            fieldnames.append("match_score")
        
        for row in reader:
            existing_result = (row.get("analysis_result") or "").strip()
            if existing_result:
                parsed = extract_json_from_response(existing_result)
                if parsed is not None:
                    flat = apply_flat_analysis_result(row, parsed)
                    for key in flat:
                        if key not in fieldnames:
                            fieldnames.append(key)
            rows.append(row)
    
    logger.info(f"Loaded {len(rows)} rows from CSV")
    print(f"Loaded {len(rows)} rows")
    
    # Filter rows to process
    rows_to_process = []
    skipped_empty = 0
    skipped_existing = 0
    for i, row in enumerate(rows):
        domain = row.get(domain_column, "").strip()
        if not domain:
            skipped_empty += 1
            continue
        
        # Skip if already processed
        if skip_existing:
            existing_result = row.get("analysis_result", "").strip()
            existing_score = row.get("match_score", "").strip()
            if existing_result or (existing_score and existing_score.isdigit()):
                skipped_existing += 1
                continue
        
        rows_to_process.append((i, domain))
    
    # Apply limit if specified
    original_count = len(rows_to_process)
    if limit is not None and limit > 0:
        rows_to_process = rows_to_process[:limit]
        logger.info(f"Limited processing to first {limit} rows (from {original_count} available)")
        if len(rows_to_process) < original_count:
            print(f"Limited to first {limit} rows (from {original_count} available)")
    
    logger.info(f"Rows to process: {len(rows_to_process)} (skipped {skipped_empty} empty, {skipped_existing} existing)")
    print(f"Rows to process: {len(rows_to_process)}")
    if skipped_existing > 0:
        print(f"  (Skipped {skipped_existing} rows with existing scores)")
    if limit is not None and limit > 0 and original_count > limit:
        print(f"  (Limited to first {limit} rows)")
    
    if not rows_to_process:
        logger.info("No rows to process, writing output file")
        print("No rows to process. Writing output file...")
        with open(output_path, "w", encoding="utf-8-sig", newline="") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            for row in rows:
                # Ensure all output columns exist
                if "analysis_result" not in row:
                    row["analysis_result"] = ""
                if "analysis_status" not in row:
                    row["analysis_status"] = ""
                if "analysis_error" not in row:
                    row["analysis_error"] = ""
                if "match_score" in fieldnames and "match_score" not in row:
                    row["match_score"] = ""
                writer.writerow(row)
        logger.info("Output file written successfully")
        return
    
    # Process domains
    logger.info(f"Starting domain processing with {max_workers} workers using prompt '{prompt.name}'")
    results = {}
    stats = {
        "success": 0,
        "fetch_failed": 0,
        "analysis_failed": 0,
        "error": 0,
        "empty_domain": 0
    }
    
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_domain = {
            executor.submit(process_domain, domain, prompt): (i, domain)
            for i, domain in rows_to_process
        }
        
        # Process results with enhanced progress bar
        with tqdm(
            total=len(rows_to_process),
            desc="Analyzing domains",
            unit="domain",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}"
        ) as pbar:
            for future in as_completed(future_to_domain):
                i, domain = future_to_domain[future]
                try:
                    result = future.result()
                    results[i] = result
                    stats[result["status"]] = stats.get(result["status"], 0) + 1
                    
                    # Update progress bar with current stats
                    pbar.set_postfix({
                        "✓": stats["success"],
                        "✗": stats["fetch_failed"] + stats["analysis_failed"] + stats["error"],
                        "current": domain[:30] + "..." if len(domain) > 30 else domain
                    })
                except Exception as e:
                    logger.error(f"Exception processing domain {domain}: {e}", exc_info=True)
                    print(f"\n⚠ Error processing domain {domain}: {e}")
                    results[i] = {
                        "domain": domain,
                        "result": None,
                        "match_score": None,
                        "status": "error",
                        "error": str(e)
                    }
                    stats["error"] += 1
                pbar.update(1)
    
    elapsed_time = time.time() - start_time
    logger.info(f"Domain processing completed in {elapsed_time:.2f}s")
    
    # Update rows with results
    logger.debug("Updating rows with results")
    for i, result_data in results.items():
        rows[i]["analysis_status"] = result_data["status"]
        rows[i]["analysis_error"] = result_data.get("error", "") or ""
        
        # Store the full result as JSON
        result_payload = result_data.get("result")
        if result_payload:
            flat = apply_flat_analysis_result(rows[i], result_payload)
            for key in flat:
                if key not in fieldnames:
                    fieldnames.append(key)
        else:
            rows[i]["analysis_result"] = ""
            rows[i]["match_score"] = ""
    
    # Write output CSV
    logger.info(f"Writing results to {output_path}")
    print(f"\nWriting results to {output_path}")
    with open(output_path, "w", encoding="utf-8-sig", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        for row in rows:
            # Ensure all output columns exist
            if "match_score" not in row:
                row["match_score"] = ""
            if "analysis_status" not in row:
                row["analysis_status"] = ""
            if "analysis_error" not in row:
                row["analysis_error"] = ""
            writer.writerow(row)
    
    logger.info("Output file written successfully")
    
    # Print summary
    successful = stats["success"]
    failed = len(results) - successful
    print(f"\n✅ Analysis complete! (took {elapsed_time:.2f}s)")
    print(f"   Successful: {successful}")
    print(f"   Failed: {failed}")
    if failed > 0:
        print(f"   Breakdown:")
        if stats["fetch_failed"] > 0:
            print(f"     - Fetch failed: {stats['fetch_failed']}")
        if stats["analysis_failed"] > 0:
            print(f"     - Analysis failed: {stats['analysis_failed']}")
        if stats["error"] > 0:
            print(f"     - Errors: {stats['error']}")
        if stats["empty_domain"] > 0:
            print(f"     - Empty domains: {stats['empty_domain']}")
    
    if successful > 0:
        scores = []
        for r in results.values():
            res = r.get("result")
            if not res:
                continue
            v = res.get("match_score") if "match_score" in res else res.get("score")
            if isinstance(v, int):
                scores.append(v)

        if scores:
            use_ten_scale = any(s > 5 for s in scores)
            rng = range(11) if use_ten_scale else range(6)
            print(f"   Score distribution:")
            for score in rng:
                count = scores.count(score)
                if count > 0:
                    percentage = (count / len(scores)) * 100
                    print(f"     Score {score}: {count} ({percentage:.1f}%)")
    
    logger.info(f"Processing complete: {successful} successful, {failed} failed")


def main(argv: list[str]) -> int:
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Analyze domains from CSV using configurable prompts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available prompts
  python scripts/analyze_handwerk_domains.py --list-prompts
  
  # Basic usage with default prompt
  python scripts/analyze_handwerk_domains.py input.csv output.csv
  
  # Use a specific prompt
  python scripts/analyze_handwerk_domains.py input.csv output.csv --prompt handwerk_analysis
  
  # Use a custom prompt file
  python scripts/analyze_handwerk_domains.py input.csv output.csv --prompt-file custom_prompt.json
  
  # Specify domain column name
  python scripts/analyze_handwerk_domains.py input.csv output.csv --domain-column Domain
  
  # Process all rows (don't skip existing)
  python scripts/analyze_handwerk_domains.py input.csv output.csv --no-skip-existing
  
  # Use more workers for faster processing
  python scripts/analyze_handwerk_domains.py input.csv output.csv --max-workers 20
  
  # Process only first 10 rows (for testing)
  python scripts/analyze_handwerk_domains.py input.csv output.csv --limit 10
        """
    )
    
    parser.add_argument(
        "input_csv",
        nargs="?",
        help="Path to input CSV file (must have a domain column)"
    )
    parser.add_argument(
        "output_csv",
        nargs="?",
        help="Path to output CSV file"
    )
    parser.add_argument(
        "--list-prompts",
        action="store_true",
        help="List all available prompts and exit"
    )
    parser.add_argument(
        "--prompt",
        default="handwerk_analysis",
        help="Name of the prompt to use (default: 'handwerk_analysis')"
    )
    parser.add_argument(
        "--prompt-file",
        type=Path,
        help="Path to a custom prompt JSON file (overrides --prompt)"
    )
    parser.add_argument(
        "--domain-column",
        default="domain",
        help="Name of the column containing domains (default: 'domain')"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum number of concurrent workers (default: 10)"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Process all rows, even if they already have a match_score"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Limit processing to first N rows (useful for testing or batch processing)"
    )
    
    args = parser.parse_args(argv[1:])
    
    # Handle list-prompts
    if args.list_prompts:
        print("\nAvailable prompts:")
        print("=" * 80)
        prompts = list_prompts()
        if not prompts:
            print("No prompts found. Create JSON files in scripts/prompts/ directory.")
        else:
            for prompt in prompts:
                print(f"\n  Name: {prompt.name}")
                print(f"  Version: {prompt.version}")
                if prompt.description:
                    print(f"  Description: {prompt.description}")
        print("\n" + "=" * 80)
        return 0
    
    # Validate required arguments
    if not args.input_csv or not args.output_csv:
        parser.error("input_csv and output_csv are required (unless using --list-prompts)")
    
    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.info("Verbose logging enabled")
    
    # Load prompt
    prompt: Optional[Prompt] = None
    if args.prompt_file:
        if not args.prompt_file.exists():
            error_msg = f"Prompt file does not exist: {args.prompt_file}"
            logger.error(error_msg)
            print(f"❌ Error: {error_msg}")
            return 1
        prompt = load_prompt_from_file(args.prompt_file)
        if not prompt:
            error_msg = f"Failed to load prompt from {args.prompt_file}"
            logger.error(error_msg)
            print(f"❌ Error: {error_msg}")
            return 1
        logger.info(f"Loaded prompt from file: {args.prompt_file}")
    else:
        prompt = get_prompt(args.prompt)
        if not prompt:
            available = ", ".join([p.name for p in list_prompts()])
            error_msg = f"Prompt '{args.prompt}' not found. Available prompts: {available or 'none'}"
            logger.error(error_msg)
            print(f"❌ Error: {error_msg}")
            print(f"\nUse --list-prompts to see all available prompts.")
            return 1
    
    # Log configuration
    logger.info(f"ML Studio URL: {ML_STUDIO_BASE_URL}")
    logger.info(f"Local model: {LOCAL_MODEL}")
    logger.info(f"Prompt: {prompt.name} (v{prompt.version})")
    logger.info(f"Max workers: {args.max_workers}")
    if args.limit:
        logger.info(f"Processing limit: {args.limit} rows")
    
    input_path = Path(args.input_csv)
    output_path = Path(args.output_csv)
    
    if not input_path.exists():
        error_msg = f"Input file does not exist: {input_path}"
        logger.error(error_msg)
        print(f"❌ Error: {error_msg}")
        return 1
    
    try:
        process_csv(
            input_path=input_path,
            output_path=output_path,
            prompt=prompt,
            domain_column=args.domain_column,
            max_workers=args.max_workers,
            skip_existing=not args.no_skip_existing,
            limit=args.limit,
        )
        return 0
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        print("\n\n⚠️  Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))

