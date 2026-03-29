# Pipeline Processor — Data Processing Engine

A production-ready data processing pipeline with chainable stages, input validation, error handling strategies, middleware hooks, conditional branching, batch processing, and execution metrics. Built with pure Python — zero external dependencies.

## Features

- **Chainable stages** — fluent API with method chaining
- **Input validation** — optional validators per stage
- **Error strategies** — STOP, SKIP, or FALLBACK on failure
- **Middleware hooks** — before/after each stage (logging, timing, auditing)
- **Conditional branching** — route data through sub-pipelines
- **Batch processing** — process multiple items independently
- **Execution metrics** — calls, successes, failures, skips, timing per stage
- **Type-safe** — passes `mypy --strict` with zero errors
- **Battle-tested** — 55/55 tests passing
- **Zero dependencies** — pure Python standard library

## Installation

```bash
git clone https://github.com/YOUR_USERNAME/pipeline-processor.git
cd pipeline-processor
```

## Quick Start

```python
from pipeline import Pipeline, ErrorStrategy

# Build a pipeline with method chaining
etl = (
    Pipeline("etl")
    .add_stage("parse", handler=lambda raw: raw.strip().split(","))
    .add_stage("validate", handler=lambda items: [i for i in items if i],
               validator=lambda items: len(items) > 0,
               on_error=ErrorStrategy.STOP)
    .add_stage("transform", handler=lambda items: [i.upper() for i in items])
)

result = etl.process("  alice, bob, charlie  ")
print(result.data)      # ['ALICE', ' BOB', ' CHARLIE']
print(result.success)   # True
```

### Error Strategies

```python
pipeline = Pipeline()

# STOP — halt pipeline on error (default)
pipeline.add_stage("strict", handler=risky_fn, on_error=ErrorStrategy.STOP)

# SKIP — pass data through unchanged on error
pipeline.add_stage("optional", handler=risky_fn, on_error=ErrorStrategy.SKIP)

# FALLBACK — use a default value on error
pipeline.add_stage("safe", handler=risky_fn,
                   on_error=ErrorStrategy.FALLBACK, fallback_value=[])
```

### Middleware

```python
def logger(stage_name, phase, data, **kwargs):
    duration = kwargs.get("duration_ms", "")
    print(f"[{phase}] {stage_name} {duration}")

pipeline = Pipeline()
pipeline.add_middleware(logger)
pipeline.add_stage("step1", handler=lambda x: x + 1)
pipeline.process(1)
# [before] step1
# [after] step1 0.015
```

### Conditional Branching

```python
premium_pipeline = Pipeline("premium")
premium_pipeline.add_stage("discount", handler=lambda order: {**order, "discount": 0.2})

main = Pipeline("orders")
main.add_stage("validate", handler=validate_order)
main.add_branch("premium_route",
                condition=lambda order: order.get("tier") == "premium",
                pipeline=premium_pipeline)
```

### Batch Processing

```python
pipeline = Pipeline()
pipeline.add_stage("double", handler=lambda x: x * 2)

results = pipeline.process_batch([1, 2, 3, 4, 5])
# [2, 4, 6, 8, 10]
```

### Metrics

```python
pipeline = Pipeline()
pipeline.add_stage("compute", handler=heavy_computation)
pipeline.process_batch(large_dataset)

metrics = pipeline.get_metrics()
print(metrics["compute"])
# {'calls': 1000, 'successes': 998, 'failures': 2,
#  'skips': 0, 'fallbacks': 0, 'total_ms': 4521.3, 'avg_ms': 4.521}

pipeline.reset_metrics()  # Start fresh
```

## API Reference

### `Pipeline(name="pipeline")`

#### Stage Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `add_stage(name, handler, validator, on_error, fallback_value)` | `Pipeline` | Add processing stage |
| `add_middleware(hook)` | `Pipeline` | Add before/after hook |
| `add_branch(name, condition, pipeline)` | `Pipeline` | Add conditional branch |

#### Execution Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `process(data)` | `PipelineResult` | Process single item |
| `process_batch(items)` | `List[PipelineResult]` | Process multiple items |
| `get_metrics()` | `Dict` | Stage execution statistics |
| `reset_metrics()` | `None` | Reset all counters |

### Error Strategies

| Strategy | Behavior |
|----------|----------|
| `STOP` | Halt pipeline, return error (default) |
| `SKIP` | Pass data unchanged, continue |
| `FALLBACK` | Use fallback_value, continue |

### `PipelineResult`

| Field | Type | Description |
|-------|------|-------------|
| `success` | `bool` | Whether pipeline completed |
| `data` | `Any` | Final transformed data |
| `stages` | `List[StageResult]` | Per-stage details |
| `total_duration_ms` | `float` | Total execution time |
| `error` | `Optional[str]` | Error message if failed |

## Running Tests

```bash
pip install pytest
pytest test_pipeline.py -v
```

### Test Results

```
55 passed in 0.17s
```

### Test Coverage

| Category | Tests | Description |
|----------|-------|-------------|
| Basic Pipeline | 7 | Creation, chaining, stage results |
| Validation | 7 | Duplicate names, types, empty values |
| Stage Validators | 5 | Pass, fail with each strategy |
| Error Strategies | 4 | STOP, SKIP, FALLBACK, remaining stages |
| Middleware | 4 | Hooks, multiple, exceptions, timing |
| Branching | 4 | Taken, not taken, exception, nested |
| Batch Processing | 4 | Basic, empty, failures, independence |
| Metrics | 7 | Counters, accumulation, reset, averages |
| Data Types | 4 | Dict, list, string, None |
| Performance | 3 | 100 stages, 1000 batch, metrics after batch |
| Repr | 2 | Empty and full representation |
| Result Details | 4 | Duration, errors, success fields |

## Architecture

```
Pipeline
├── add_stage()          # Register transformation stage
├── add_middleware()     # Register before/after hooks
├── add_branch()         # Register conditional sub-pipeline
├── process()            # Execute full pipeline
│   ├── _execute_stage() # Run single stage with validation & error handling
│   └── _notify_middleware()  # Call hooks (swallows exceptions)
├── process_batch()      # Map process() over list
├── get_metrics()        # Accumulated stage statistics
└── reset_metrics()      # Clear counters
```

## License

MIT
