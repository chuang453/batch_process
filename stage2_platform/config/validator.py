from __future__ import annotations

from typing import Dict, List, Set

from stage2_platform.execution.builtin_ops import BUILTIN_OPS
from stage2_platform.execution.transform_registry import STAGE2_TRANSFORMS
from stage2_platform.ingestion.loader import ADAPTER_REGISTRY


def _validate_input(item: Dict, idx: int) -> List[str]:
    errors: List[str] = []
    name = item.get("name")
    if not name:
        errors.append(f"inputs[{idx}].name is required")

    source_type = item.get("source_type")
    if source_type not in ADAPTER_REGISTRY:
        errors.append(
            f"inputs[{idx}].source_type unsupported: {source_type} (allowed: {sorted(ADAPTER_REGISTRY.keys())})"
        )

    if not isinstance(item.get("source_params"), dict):
        errors.append(f"inputs[{idx}].source_params must be a dict")
    return errors


def _validate_step(step: Dict, loc: str) -> List[str]:
    errors: List[str] = []
    if not isinstance(step, dict):
        errors.append(f"{loc} must be a dict")
        return errors

    if "run" in step:
        funcs = step.get("run") or []
        for name in funcs:
            if name not in STAGE2_TRANSFORMS:
                errors.append(f"{loc}.run has unknown transform: {name}")
        return errors

    if "group_by" in step:
        sub = step.get("steps", []) or []
        if not isinstance(sub, list):
            errors.append(f"{loc}.steps must be a list")
        else:
            for sidx, s in enumerate(sub, start=1):
                errors.extend(_validate_step(s, f"{loc}.steps[{sidx}]") )
        return errors

    op_name = None
    for key in step:
        if key != "config":
            op_name = key
            break
    if op_name is None or op_name not in BUILTIN_OPS:
        errors.append(f"{loc} has unknown builtin op: {op_name}")
    return errors


def validate_project(project: Dict) -> List[str]:
    errors: List[str] = []

    if not isinstance(project, dict):
        return ["project must be a dict"]

    if not project.get("name"):
        errors.append("project.name is required")

    inputs = project.get("inputs", []) or []
    if not isinstance(inputs, list):
        errors.append("project.inputs must be a list")
        inputs = []
    for idx, item in enumerate(inputs, start=1):
        errors.extend(_validate_input(item, idx))

    stages = project.get("stages", []) or []
    if not isinstance(stages, list):
        errors.append("project.stages must be a list")
        stages = []

    for sidx, stage in enumerate(stages, start=1):
        loc = f"stages[{sidx}]"
        if not isinstance(stage, dict):
            errors.append(f"{loc} must be a dict")
            continue

        if not stage.get("name"):
            errors.append(f"{loc}.name is required")

        series = stage.get("series", []) or []
        if not isinstance(series, list):
            errors.append(f"{loc}.series must be a list")
            continue

        output_keys: Set[str] = set()
        for ridx, row in enumerate(series, start=1):
            rloc = f"{loc}.series[{ridx}]"
            if not isinstance(row, dict):
                errors.append(f"{rloc} must be a dict")
                continue
            if not row.get("name"):
                errors.append(f"{rloc}.name is required")

            output_key = str(row.get("output_key", "") or "")
            if output_key:
                if output_key in output_keys:
                    errors.append(f"{rloc}.output_key duplicated in stage: {output_key}")
                output_keys.add(output_key)

            steps = row.get("steps", []) or []
            if not isinstance(steps, list):
                errors.append(f"{rloc}.steps must be a list")
                continue
            for didx, step in enumerate(steps, start=1):
                errors.extend(_validate_step(step, f"{rloc}.steps[{didx}]"))

    return errors
