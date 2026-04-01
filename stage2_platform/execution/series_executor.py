from __future__ import annotations

from datetime import datetime
from typing import Any, Callable

from core.data_stage import DataStage
from decorators.processor import ProcessingContext
from stage2_platform.contracts import SeriesManifest, SeriesSpec
from stage2_platform.execution.context import Stage2Context
from stage2_platform.execution.transform_registry import STAGE2_TRANSFORMS


class SeriesExecutor:

    def run(self,
            spec: SeriesSpec,
            ctx: Stage2Context,
            observer: Callable[[str, dict], None] | None = None) -> SeriesManifest:
        input_key = spec.input_key or "df"
        output_key = spec.output_key or input_key
        started_at = datetime.now()

        if not spec.enabled:
            return SeriesManifest(
                series_name=spec.name,
                input_key=input_key,
                output_key=output_key,
                status="skipped",
                started_at=started_at.isoformat(sep=" ", timespec="seconds"),
                finished_at=started_at.isoformat(sep=" ", timespec="seconds"),
            )

        df = ctx.catalog.get(input_key)
        if df is None:
            finished_at = datetime.now()
            return SeriesManifest(
                series_name=spec.name,
                input_key=input_key,
                output_key=output_key,
                status="failed",
                started_at=started_at.isoformat(sep=" ", timespec="seconds"),
                finished_at=finished_at.isoformat(sep=" ", timespec="seconds"),
                error=f"input dataset not found: {input_key}",
            )

        if observer:
            observer("series_started", {"series_name": spec.name, "input_key": input_key, "output_key": output_key})

        proc_context = ProcessingContext()
        ds = DataStage(proc_context)
        ds._transforms = STAGE2_TRANSFORMS

        def _step_observer(event):
            if observer:
                observer("step_event", {
                    "series_name": spec.name,
                    "step_id": event.step_id,
                    "op_name": event.op_name,
                    "kind": event.kind,
                    "error": event.error,
                })

        rows_in = int(len(df)) if hasattr(df, "__len__") else 0
        try:
            result = ds.run_steps(df,
                                  spec.steps,
                                  on_error="continue" if spec.continue_on_error else "abort",
                                  observer=_step_observer,
                                  return_result=True)
            final_output_key = output_key
            status = "success" if result.success else "failed"
            error = result.error
            rows_out = int(len(result.df)) if hasattr(result.df, "__len__") else 0

            if status == "success":
                final_output_key = ctx.catalog.put(output_key, result.df, policy=spec.output_policy)
            else:
                ctx.log(f"Series failed: {spec.name} -> {error}")
        except Exception as exc:
            final_output_key = output_key
            status = "failed"
            error = str(exc)
            rows_out = 0
            ctx.log(f"Series failed: {spec.name} -> {error}")

        finished_at = datetime.now()
        manifest = SeriesManifest(
            series_name=spec.name,
            input_key=input_key,
            output_key=final_output_key,
            status=status,
            started_at=started_at.isoformat(sep=" ", timespec="seconds"),
            finished_at=finished_at.isoformat(sep=" ", timespec="seconds"),
            duration_s=(finished_at - started_at).total_seconds(),
            rows_in=rows_in,
            rows_out=rows_out,
            error=error,
        )
        if observer:
            observer("series_finished", {"manifest": manifest})
        return manifest

    def simulate(self, spec: SeriesSpec) -> list[dict]:
        ds = DataStage(ProcessingContext())
        return ds.simulate_steps(None, spec.steps)
