from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict, List
from uuid import uuid4

from stage2_platform.config.normalizer import normalize_project
from stage2_platform.contracts import DatasetRef, RunManifest, StageSpec, normalize_stage
from stage2_platform.execution.context import Stage2Context
from stage2_platform.execution.stage_orchestrator import StageOrchestrator
from stage2_platform.ingestion.loader import load_dataset


class ProjectRunner:

    def __init__(self):
        self.stage_orchestrator = StageOrchestrator()
        self._cancel_requested = False

    def cancel(self) -> None:
        self._cancel_requested = True

    def run(self,
            project: Dict,
            ctx: Stage2Context | None = None,
            observer: Callable[[str, dict], None] | None = None) -> RunManifest:
        normalized = normalize_project(project)
        ctx = ctx or Stage2Context()

        manifest = RunManifest(
            run_id=str(uuid4()),
            project_name=str(normalized.get("name", "stage2_project")),
            status="running",
            started_at=datetime.now().isoformat(sep=" ", timespec="seconds"),
        )
        ctx.manifest = manifest

        if observer:
            observer("run_started", {"run_id": manifest.run_id, "project_name": manifest.project_name})

        for item in normalized.get("inputs", []) or []:
            ref = DatasetRef(
                name=str(item.get("name", "input")),
                source_type=str(item.get("source_type", "file")),
                source_params=dict(item.get("source_params", {}) or {}),
                schema=dict(item.get("schema", {}) or {}),
                tags=list(item.get("tags", []) or []),
            )
            dataset = load_dataset(ref)
            ctx.catalog.put(ref.name, dataset, policy="overwrite")

        all_records = []
        for raw_stage in normalized.get("stages", []) or []:
            stage = normalize_stage(raw_stage)
            stage_records = self.stage_orchestrator.run_stage(
                stage,
                ctx,
                observer=observer,
                cancel_check=lambda: self._cancel_requested,
            )
            all_records.extend(stage_records)
            if self._cancel_requested:
                break

        manifest.series_records = all_records
        manifest.finished_at = datetime.now().isoformat(sep=" ", timespec="seconds")

        if self._cancel_requested:
            manifest.status = "cancelled"
        elif any(r.status == "failed" for r in all_records):
            manifest.status = "partial"
        else:
            manifest.status = "done"

        if observer:
            observer("run_finished", {"manifest": manifest})
        return manifest
