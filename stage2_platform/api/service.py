from __future__ import annotations

from typing import Any, Dict, List

from stage2_platform.config import load_project, normalize_project, validate_project
from stage2_platform.contracts import DatasetRef, normalize_stage
from stage2_platform.execution import ProjectRunner, SeriesExecutor, Stage2Context, StageOrchestrator
from stage2_platform.execution.builtin_ops import BUILTIN_OPS
from stage2_platform.execution.transform_registry import STAGE2_TRANSFORMS
from stage2_platform.ingestion.loader import load_dataset


class Stage2Service:

    def __init__(self):
        self.project: Dict[str, Any] | None = None
        self.context = Stage2Context()
        self.runner = ProjectRunner()

    def _prepare_context(self, project: Dict[str, Any]) -> Stage2Context:
        ctx = Stage2Context()
        normalized = normalize_project(project)
        for item in normalized.get("inputs", []) or []:
            ref = DatasetRef(
                name=str(item.get("name", "input")),
                source_type=str(item.get("source_type", "file")),
                source_params=dict(item.get("source_params", {}) or {}),
                schema=dict(item.get("schema", {}) or {}),
                tags=list(item.get("tags", []) or []),
            )
            ctx.catalog.put(ref.name, load_dataset(ref), policy="overwrite")
        self.context = ctx
        return ctx

    def load_project(self, path: str) -> Dict[str, Any]:
        self.project = load_project(path)
        return self.project

    def set_project(self, project: Dict[str, Any]) -> Dict[str, Any]:
        self.project = normalize_project(project)
        return self.project

    def validate_project(self, project: Dict[str, Any] | None = None) -> List[str]:
        target = normalize_project(project or self.project or {})
        return validate_project(target)

    def simulate(self, project: Dict[str, Any] | None = None) -> Dict[str, Any]:
        target = normalize_project(project or self.project or {})
        series_executor = SeriesExecutor()
        out: Dict[str, Any] = {"name": target.get("name", "stage2_project"), "stages": []}
        for raw_stage in target.get("stages", []) or []:
            stage = normalize_stage(raw_stage)
            stage_rows = []
            for series in stage.series:
                stage_rows.append({
                    "series_name": series.name,
                    "input_key": series.input_key,
                    "output_key": series.output_key or series.input_key,
                    "steps": series_executor.simulate(series),
                })
            out["stages"].append({"stage_name": stage.name, "series": stage_rows})
        return out

    def run_series(self, stage_name: str, series_name: str):
        if not self.project:
            raise ValueError("project not loaded")
        normalized = normalize_project(self.project)
        self.context = self._prepare_context(normalized)
        self.runner._cancel_requested = False
        raw_stages = [s for s in normalized.get("stages", []) if s.get("name") == stage_name]
        if not raw_stages:
            raise KeyError(f"stage not found: {stage_name}")
        stage = normalize_stage(raw_stages[0])
        return StageOrchestrator().run_single_series(stage, series_name, self.context)

    def run_stage(self, stage_name: str):
        if not self.project:
            raise ValueError("project not loaded")
        normalized = normalize_project(self.project)
        self.context = self._prepare_context(normalized)
        self.runner._cancel_requested = False
        raw_stages = [s for s in normalized.get("stages", []) if s.get("name") == stage_name]
        if not raw_stages:
            raise KeyError(f"stage not found: {stage_name}")
        return StageOrchestrator().run_stage(normalize_stage(raw_stages[0]), self.context)

    def run_project(self):
        if not self.project:
            raise ValueError("project not loaded")
        self.context = Stage2Context()
        self.runner._cancel_requested = False
        return self.runner.run(self.project, self.context)

    def cancel_run(self) -> None:
        self.runner.cancel()

    def get_run_status(self):
        return self.context.manifest

    def list_ops(self) -> Dict[str, List[str]]:
        return {
            "builtin_ops": sorted(BUILTIN_OPS.keys()),
            "transforms": sorted(STAGE2_TRANSFORMS.keys()),
        }
