from __future__ import annotations

from typing import Callable, List

from stage2_platform.contracts import SeriesManifest, StageSpec
from stage2_platform.execution.context import Stage2Context
from stage2_platform.execution.series_executor import SeriesExecutor


class StageOrchestrator:

    def __init__(self):
        self.series_executor = SeriesExecutor()

    def run_single_series(self,
                          stage: StageSpec,
                          series_name: str,
                          ctx: Stage2Context,
                          observer: Callable[[str, dict], None] | None = None) -> SeriesManifest:
        for series in stage.series:
            if series.name == series_name:
                return self.series_executor.run(series, ctx, observer=observer)
        raise KeyError(f"series not found in stage '{stage.name}': {series_name}")

    def run_stage(self,
                  stage: StageSpec,
                  ctx: Stage2Context,
                  observer: Callable[[str, dict], None] | None = None,
                  cancel_check: Callable[[], bool] | None = None) -> List[SeriesManifest]:
        manifests: List[SeriesManifest] = []
        for series in stage.series:
            if cancel_check and cancel_check():
                manifests.append(
                    SeriesManifest(
                        series_name=series.name,
                        input_key=series.input_key,
                        output_key=series.output_key or series.input_key,
                        status="skipped",
                        error="cancelled",
                    ))
                continue

            manifest = self.series_executor.run(series, ctx, observer=observer)
            manifests.append(manifest)
            if manifest.status == "failed" and not series.continue_on_error:
                break
        return manifests
