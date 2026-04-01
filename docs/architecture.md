# Batch Process Framework Architecture

## Overview

batch_process is a recursive batch-processing framework with GUI and CLI entry points. It applies named processors to files and directories using YAML/JSON configuration.

The architecture now has two execution layers:

- BatchProcessor: file traversal and per-path processor execution
- Pipeline + Stage 2 Platform: walk orchestration plus standalone DataFrame processing platform

For Stage 2 platform details (principles, module split, config model, CLI/API/UI usage, and Stage 1 bridge), see:

- [stage2_platform.md](stage2_platform.md)

## Core Modules

- decorators/processor.py: ProcessingContext, processor decorators, global registries
- core/engine.py: BatchProcessor traversal, matching, execution, progress, status log
- core/pipeline.py: stage orchestrator for walk/data stages
- core/data_stage.py: DataFrame step engine (builtin ops, transforms, group_by)
- stage2_platform/: standalone Stage 2 package (contracts/config/ingestion/execution/registry/delivery/api/cli/ui)
- config/loader.py: config loading and plugin auto-discovery
- cli/app.py: CLI routing for classic mode and pipeline mode

## ProcessingContext Model

ProcessingContext is the shared runtime state container.

Fields:

- root_path: root Path for this run
- data/results/metadata/shared: existing generic state buckets
- main: user-managed main working objects (for example df, summary)
- pipe: engine-managed intermediate outputs, keyed by processor/transform name
- pipe_log: lightweight write history for pipe entries

Methods:

- main methods: set_main, get_main, delete_main, list_main
- pipe read methods: get_pipe, get_pipe_log
- nested dict helpers: set_data/get_data, set_shared/get_shared, metadata helpers

## Registration and Decorators

Global registries in decorators/processor.py:

- PROCESSORS: file/dir processors
- PRE_PROCESSORS: global/phase pre hooks
- POST_PROCESSORS: global/phase post hooks
- TRANSFORMS: DataFrame transforms
- AVAILABLE_PROCESSORS: compatibility/global lookup map

Decorators:

- @processor(name=...)
- @pre_processor(name=...)
- @post_processor(name=...)
- @transform(name=...)

All decorators support optional inputs and outputs metadata declarations.

## BatchProcessor Execution

BatchProcessor run flow:

1. Resolve and run global pre_process if configured
2. Recursively walk tree from root_path
3. For each path, match config rules and execute pre + inline
4. Recurse children if directory
5. Execute post processors
6. Run global post_process if configured

Matching details:

- pattern "." matches root
- patterns ending with "/" are directory-only
- glob matching uses wcmatch GLOBSTAR

Execution details:

- priority sorting per phase
- optional deep-copy of processor config per call
- worker hooks: step_started, step_finished
- status log persisted to debug_logs/status.log (configurable)

Pipe capture:

- when a processor returns dict, engine stores it in context.pipe[proc_name]
- engine appends a lightweight row into context.pipe_log
- applies to global pre/post and per-path processor calls

## DataStage Execution

DataStage runs a list of steps over an input DataFrame.

Step dispatch:

- group_by step: recursive group processing
- run step: chain named @transform functions
- otherwise: builtin DataFrame operation

Builtin ops include:

- rename
- dropna
- filter (query)
- select
- sort
- fillna
- eval
- astype
- drop
- head/tail

Group recursion:

- group_by accepts string or list of columns
- uses split_dataframe_by_groups helper
- records runtime stacks in metadata.runtime_info.loop_cols/loop_vars
- pops stack when recursion returns
- collect=true concatenates per-group transformed outputs with group keys prepended

Transform chain behavior:

- transform signature: (df, context, **kwargs) -> df
- missing transform name is recorded in context.results
- transform returning None is treated as warning and previous df is kept
- successful transform writes summary info into context.pipe

## Pipeline Orchestration

Pipeline executes ordered stages with a shared ProcessingContext.

Supported stage types:

- walk: run BatchProcessor with stage config and stage root
- data: fetch DataFrame from context.main[source], run DataStage steps, write back

simulate() produces a combined stage-aware dry-run plan.

## Configuration Model

Classic engine config (BatchProcessor only):

- top-level glob/path rules containing processors, pre_processors, post_processors, config, priority
- optional global keys pre_process, post_process, config_pre, config_post

Pipeline config:

- top-level pipeline: list of stages
- walk stage: type=walk, config, optional root
- data stage: type=data, source key in context.main, steps list

CLI routing:

- config with pipeline key -> Pipeline mode
- config without pipeline key -> BatchProcessor mode

## Plugin Discovery

config/loader.py load_plugins() imports all plugins/*.py modules (except __init__.py). Decorated functions register automatically in global registries.

## Built-in Recorder Pattern

processors/builtin_recorders.py provides recorder processors and persistence helpers.

- record_to_shared: append lightweight execution rows into context.shared
- persist_history_sqlite: async queue writer to SQLite
- optional auto-injection via enable_builtin_recorders

## Backward Compatibility

- Existing processor decorators and config shape remain valid
- Existing BatchProcessor workflows continue to run unchanged
- New Pipeline/DataStage is additive
- New context fields have safe defaults
