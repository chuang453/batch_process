from .builtin_ops import BUILTIN_OPS, register_builtin_op
from .context import DatasetCatalog, Stage2Context
from .step_parser import parse_steps
from .transform_registry import (
	STAGE2_TRANSFORMS,
	apply_transform,
	df_transform,
	import_legacy_transforms,
	register_transform,
)

__all__ = [
	"BUILTIN_OPS",
	"register_builtin_op",
	"DatasetCatalog",
	"Stage2Context",
	"SeriesExecutor",
	"StageOrchestrator",
	"ProjectRunner",
	"parse_steps",
	"STAGE2_TRANSFORMS",
	"df_transform",
	"register_transform",
	"import_legacy_transforms",
	"apply_transform",
]


def __getattr__(name):
	if name == "SeriesExecutor":
		from .series_executor import SeriesExecutor as _SeriesExecutor

		return _SeriesExecutor
	if name == "StageOrchestrator":
		from .stage_orchestrator import StageOrchestrator as _StageOrchestrator

		return _StageOrchestrator
	if name == "ProjectRunner":
		from .project_runner import ProjectRunner as _ProjectRunner

		return _ProjectRunner
	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
