from __future__ import annotations

from pathlib import Path

from list_processing import ListProcessingConfig
from list_processing.io import load_lead_records_from_csv, write_internal_csv
from list_processing.llm.base import LLMClient
from list_processing.pipeline import ListProcessingPipeline


SAMPLES_DIR = Path(__file__).parent.parent / "list_processing" / "samples"


class DummyLLM(LLMClient):
    """
    Simple LLMClient that always returns an empty JSON object.

    This allows running the pipeline in tests without any real model or
    network access.
    """

    def _raw_chat(self, system_prompt, user_prompt, response_format, temperature) -> str:  # type: ignore[override]
        return "{}"


def test_load_and_write_internal(tmp_path: Path) -> None:
    input_path = SAMPLES_DIR / "sample_minimal.csv"
    assert input_path.exists()

    records = load_lead_records_from_csv(input_path)
    assert len(records) == 2

    output_path = tmp_path / "output_internal.csv"
    write_internal_csv(output_path, records)
    assert output_path.exists()
    contents = output_path.read_text(encoding="utf-8")
    assert "Company" not in contents  # header should be canonical fields, not raw


def test_pipeline_no_llm_steps(tmp_path: Path) -> None:
    """
    Small integration test: run the pipeline with all LLM-based steps
    disabled to verify basic wiring and checkpoint/output behavior.
    """
    input_path = SAMPLES_DIR / "sample_minimal.csv"
    output_path = tmp_path / "pipeline_output.csv"

    config = ListProcessingConfig(
        input_path=input_path,
        output_path=output_path,
        backend="local",
        enable_enrichment=False,
        enable_salutation=False,
        enable_scoring=False,
        resume=False,
    )

    pipeline = ListProcessingPipeline(config, DummyLLM())
    pipeline.run()

    assert output_path.exists()
    text = output_path.read_text(encoding="utf-8")
    # At least the company names should appear somewhere in the CSV.
    assert "Beispiel GmbH" in text
    assert "Test AG" in text

