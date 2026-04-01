from __future__ import annotations

from typing import Any, Dict, List

from stage2_platform.contracts.step_descriptor import StepDescriptor


def _builtin_name(step: Dict[str, Any]) -> str:
    for key in step:
        if key != "config":
            return str(key)
    return "builtin_op"


def parse_steps(raw_steps: List[dict] | None,
                prefix: str = "",
                level: int = 0,
                flatten: bool = False) -> List[StepDescriptor]:
    out: List[StepDescriptor] = []
    for idx, raw in enumerate(raw_steps or [], start=1):
        step = raw if isinstance(raw, dict) else {}
        step_id = f"{prefix}{idx}"

        if "group_by" in step:
            cols = step.get("group_by", [])
            op_name = ", ".join(cols) if isinstance(cols, list) else str(cols)
            sub_steps = list(step.get("steps", []) or [])
            desc = StepDescriptor(
                step_id=step_id,
                op_type="group_by",
                op_name=op_name,
                params=cols,
                detail=step,
                sub_steps=sub_steps,
                level=level,
            )
            out.append(desc)
            if flatten:
                out.extend(parse_steps(sub_steps,
                                       prefix=f"{step_id}.",
                                       level=level + 1,
                                       flatten=True))
            continue

        if "run" in step:
            names = [str(x) for x in (step.get("run") or [])]
            out.append(
                StepDescriptor(
                    step_id=step_id,
                    op_type="transform",
                    op_name=", ".join(names),
                    params=names,
                    detail=step,
                    sub_steps=[],
                    level=level,
                ))
            continue

        name = _builtin_name(step)
        out.append(
            StepDescriptor(
                step_id=step_id,
                op_type="builtin",
                op_name=name,
                params=step.get(name),
                detail=step,
                sub_steps=[],
                level=level,
            ))
    return out
