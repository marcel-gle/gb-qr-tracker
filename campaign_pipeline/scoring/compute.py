from __future__ import annotations

from typing import Mapping

from .extract import TechnicalSignals
from .indicators.registry import is_scoring_key_triggered, list_detected_scoring_keys


def compute_veraltung_score(
    signals: TechnicalSignals,
    visual_age_bonus: int,
    signal_weights: Mapping[str, float],
    *,
    max_score: int = 10,
) -> tuple[float, float, list[str], list[str]]:
    technical_sum = 0.0
    scored_labels: list[str] = []

    for key, weight in signal_weights.items():
        if weight <= 0:
            continue
        if is_scoring_key_triggered(key, signals):
            technical_sum += float(weight)
            scored_labels.append(key)

    bonus = max(0, min(3, int(visual_age_bonus)))
    total = min(float(max_score), max(0.0, technical_sum + bonus))
    all_detected = list_detected_scoring_keys(signals)
    return total, technical_sum, scored_labels, all_detected
