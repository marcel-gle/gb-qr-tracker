from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional, Set

from .naming import manifest_path, state_path

logger = logging.getLogger(__name__)

StageStatus = Literal["raw", "scored", "imprint", "final", "dropped"]


@dataclass
class DomainState:
    domain: str
    stage: StageStatus = "raw"
    drop_reason: Optional[str] = None
    updated_at: str = ""

    def touch(self, stage: StageStatus, drop_reason: Optional[str] = None) -> None:
        self.stage = stage
        self.drop_reason = drop_reason
        self.updated_at = datetime.now(timezone.utc).isoformat()


@dataclass
class PipelineRegistry:
    campaign_dir: Path
    domains: Dict[str, DomainState] = field(default_factory=dict)
    drop_counts: Dict[str, int] = field(default_factory=dict)
    target_final_count: Optional[int] = None

    @classmethod
    def load(cls, campaign_dir: Path) -> "PipelineRegistry":
        path = state_path(campaign_dir)
        if not path.exists():
            return cls(campaign_dir=campaign_dir)
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load registry from %s: %s", path, exc)
            return cls(campaign_dir=campaign_dir)

        domains: Dict[str, DomainState] = {}
        for domain, raw in (data.get("domains") or {}).items():
            domains[domain] = DomainState(
                domain=domain,
                stage=raw.get("stage", "raw"),
                drop_reason=raw.get("drop_reason"),
                updated_at=raw.get("updated_at", ""),
            )
        return cls(
            campaign_dir=campaign_dir,
            domains=domains,
            drop_counts=dict(data.get("drop_counts") or {}),
            target_final_count=data.get("target_final_count"),
        )

    def save(self) -> None:
        path = state_path(self.campaign_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "domains": {
                d: {
                    "stage": s.stage,
                    "drop_reason": s.drop_reason,
                    "updated_at": s.updated_at,
                }
                for d, s in self.domains.items()
            },
            "drop_counts": self.drop_counts,
            "target_final_count": self.target_final_count,
        }
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def known_domains(self) -> Set[str]:
        return set(self.domains.keys())

    def domains_at_least(self, stage: StageStatus) -> Set[str]:
        order = ["raw", "scored", "imprint", "final"]
        if stage not in order:
            return set()
        min_idx = order.index(stage)
        return {d for d, s in self.domains.items() if s.stage in order[min_idx:]}

    def mark(self, domain: str, stage: StageStatus, drop_reason: Optional[str] = None) -> None:
        state = self.domains.get(domain) or DomainState(domain=domain)
        state.touch(stage, drop_reason)
        self.domains[domain] = state
        if drop_reason:
            self.drop_counts[drop_reason] = self.drop_counts.get(drop_reason, 0) + 1

    def record_drop(self, domain: str, reason: str) -> None:
        self.mark(domain, "dropped", drop_reason=reason)

    def is_new_domain(self, domain: str) -> bool:
        return domain not in self.domains

    def should_process(self, domain: str, from_stage: StageStatus) -> bool:
        state = self.domains.get(domain)
        if state is None:
            return True
        if state.stage == "dropped":
            return False
        order = ["raw", "scored", "imprint", "final"]
        if from_stage not in order:
            return True
        current_idx = order.index(state.stage) if state.stage in order else -1
        target_idx = order.index(from_stage)
        return current_idx < target_idx

    def update_manifest(
        self,
        *,
        raw: int = 0,
        scored: int = 0,
        imprint: int = 0,
        final: int = 0,
    ) -> None:
        path = manifest_path(self.campaign_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "counts": {"raw": raw, "scored": scored, "imprint": imprint, "final": final},
            "drop_counts": dict(self.drop_counts),
            "target_final_count": self.target_final_count,
        }
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def funnel_summary(self) -> Dict[str, object]:
        counts = {"raw": 0, "scored": 0, "imprint": 0, "final": 0, "dropped": 0}
        for state in self.domains.values():
            if state.stage in counts:
                counts[state.stage] += 1
        return {
            "counts": counts,
            "drop_counts": dict(self.drop_counts),
            "target_final_count": self.target_final_count,
            "total_tracked": len(self.domains),
        }
