from __future__ import annotations

import logging
from typing import List

from ..llm.base import LLMClient
from ..models import LeadRecord, PromptUsage

logger = logging.getLogger(__name__)

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover - optional dependency
    def tqdm(iterable, **kwargs):  # type: ignore[no-redef]
        return iterable


SALUTATION_SYSTEM_PROMPT = "You classify first names by gender."


SALUTATION_USER_TEMPLATE = (
    "Du klassifizierst deutsche Vornamen nach Anrede.\n"
    "Vorname: '{first_name}'\n"
    "Antwortformat:\n"
    "- Antworte mit genau einem Wort: 'Herr' oder 'Frau'.\n"
    "- Keine Erklärungen, keine Satzzeichen, keine weiteren Wörter."
)


class SalutationService:
    """
    Infers German salutations (Herr/Frau) from first names using an LLMClient.
    """

    def __init__(
        self,
        llm: LLMClient,
        *,
        prompt_name: str = "salutation_inference",
        prompt_version: str = "1.0",
    ) -> None:
        self._llm = llm
        self._prompt_name = prompt_name
        self._prompt_version = prompt_version

    def infer_salutation(self, first_name: str) -> str:
        first_name = (first_name or "").strip()
        if not first_name:
            return "Herr"

        user_prompt = SALUTATION_USER_TEMPLATE.format(first_name=first_name)

        try:
            raw_answer = self._llm.chat(
                system_prompt=SALUTATION_SYSTEM_PROMPT,
                user_prompt=user_prompt,
                response_format=None,
                temperature=0.0,
            )
            logger.debug("First name %s -> raw salutation answer: %r", first_name, raw_answer)
            answer = (raw_answer or "").strip().lower()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Salutation LLM call failed for %s: %s", first_name, exc)
            return "Herr"

        if answer == "frau":
            return "Frau"
        if answer == "herr":
            return "Herr"

        if "female" in answer or "weiblich" in answer:
            return "Frau"
        if "male" in answer or "männlich" in answer:
            return "Herr"

        return "Herr"

    def apply_to_record(self, record: LeadRecord) -> bool:
        """
        Fill record.salutation if it is missing, based on first_name or
        managing_director_first.

        Returns True if the record was updated.
        """
        if record.salutation:
            return False

        first_name = record.first_name or record.managing_director_first or ""
        inferred = self.infer_salutation(first_name)
        record.salutation = inferred

        record.prompts_used.append(
            PromptUsage(
                step="salutation",
                prompt_name=self._prompt_name,
                version=self._prompt_version,
                backend=type(self._llm).__name__,
                model=getattr(self._llm, "model_name", None),
            )
        )
        return True


def apply_salutation(records: List[LeadRecord], service: SalutationService) -> None:
    """
    Apply salutation inference to a list of records in-place.
    """
    for record in tqdm(records, desc="Salutation", unit="record"):
        try:
            service.apply_to_record(record)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "Error inferring salutation for %s (%s): %s",
                record.company_name,
                record.website,
                exc,
            )

