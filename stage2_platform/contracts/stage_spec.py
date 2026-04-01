from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from .series_spec import SeriesSpec


@dataclass
class StageSpec:
    name: str
    source_key: str = "df"
    series: List[SeriesSpec] = field(default_factory=list)


def normalize_stage(raw_stage: Dict[str, Any]) -> StageSpec:
    stage_name = str(raw_stage.get("name", "data"))
    source_key = str(raw_stage.get("source", "df"))

    raw_series = raw_stage.get("series")
    if isinstance(raw_series, list):
        series = []
        for idx, item in enumerate(raw_series, start=1):
            if not isinstance(item, dict):
                continue
            series.append(
                SeriesSpec(
                    name=str(item.get("name", f"series_{idx}")),
                    label=str(item.get("label", "")),
                    input_key=str(item.get("input_key", source_key)),
                    output_key=str(item.get("output_key", "")),
                    steps=list(item.get("steps", []) or []),
                    enabled=bool(item.get("enabled", True)),
                    continue_on_error=bool(item.get("continue_on_error", True)),
                    output_policy=str(item.get("output_policy", "overwrite")),
                ))
        return StageSpec(name=stage_name, source_key=source_key, series=series)

    # Legacy compatibility: stage-level steps become one default series.
    legacy_steps = list(raw_stage.get("steps", []) or [])
    default_series = SeriesSpec(
        name="default",
        label="",
        input_key=source_key,
        output_key=str(raw_stage.get("output_key", "")),
        steps=legacy_steps,
        enabled=True,
        continue_on_error=bool(raw_stage.get("continue_on_error", True)),
        output_policy=str(raw_stage.get("output_policy", "overwrite")),
    )
    return StageSpec(name=stage_name, source_key=source_key, series=[default_series])
