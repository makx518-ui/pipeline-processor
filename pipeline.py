"""
Pipeline Processor — Data Processing Engine
=============================================
Chainable data transformation pipeline with validation,
error handling strategies, middleware hooks, conditional
branching, and execution metrics.

Author: Vlad M.
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol


class ErrorStrategy(enum.Enum):
    """How to handle stage errors."""

    STOP = "stop"
    SKIP = "skip"
    FALLBACK = "fallback"


class MiddlewareHook(Protocol):
    """Protocol for middleware callables."""

    def __call__(
        self,
        stage_name: str,
        phase: str,
        data: Any,
        **kwargs: Any,
    ) -> None: ...


@dataclass
class StageResult:
    """Result of a single stage execution."""

    stage_name: str
    status: str  # "success", "skipped", "failed", "fallback"
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class PipelineResult:
    """Result of full pipeline execution."""

    success: bool
    data: Any
    stages: List[StageResult] = field(default_factory=list)
    total_duration_ms: float = 0.0
    error: Optional[str] = None


@dataclass
class Stage:
    """Internal representation of a pipeline stage."""

    name: str
    handler: Callable[[Any], Any]
    validator: Optional[Callable[[Any], bool]] = None
    on_error: ErrorStrategy = ErrorStrategy.STOP
    fallback_value: Any = None


@dataclass
class Branch:
    """Conditional branch in the pipeline."""

    name: str
    condition: Callable[[Any], bool]
    pipeline: "Pipeline"


@dataclass
class _StageMetrics:
    """Accumulated metrics for a single stage."""

    calls: int = 0
    successes: int = 0
    failures: int = 0
    skips: int = 0
    fallbacks: int = 0
    total_ms: float = 0.0

    @property
    def avg_ms(self) -> float:
        """Average execution time in milliseconds."""
        return self.total_ms / self.calls if self.calls > 0 else 0.0


class Pipeline:
    """
    Data processing pipeline with chainable stages.

    Supports validation, error strategies (stop/skip/fallback),
    middleware hooks, conditional branching, batch processing,
    and execution metrics.

    Example:
        pipeline = Pipeline("etl")
        pipeline.add_stage("parse", handler=parse_data)
        pipeline.add_stage("validate", handler=validate_data)
        pipeline.add_stage("transform", handler=transform_data)
        result = pipeline.process(raw_data)
    """

    def __init__(self, name: str = "pipeline") -> None:
        self._name: str = name
        self._stages: List[Stage] = []
        self._branches: List[Branch] = []
        self._middleware: List[Callable[..., None]] = []
        self._metrics: Dict[str, _StageMetrics] = {}

    @property
    def name(self) -> str:
        """Pipeline name."""
        return self._name

    @property
    def stage_count(self) -> int:
        """Number of registered stages."""
        return len(self._stages)

    def add_stage(
        self,
        name: str,
        handler: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]] = None,
        on_error: ErrorStrategy = ErrorStrategy.STOP,
        fallback_value: Any = None,
    ) -> "Pipeline":
        """
        Add a processing stage to the pipeline.

        Args:
            name: Unique stage name.
            handler: Function that transforms data.
            validator: Optional function to validate input data.
            on_error: Error handling strategy.
            fallback_value: Value to use when on_error is FALLBACK.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If stage name is empty or duplicate.
            TypeError: If handler is not callable.
        """
        if not name or not isinstance(name, str):
            raise ValueError("Stage name must be a non-empty string")

        if any(s.name == name for s in self._stages):
            raise ValueError(f"Stage '{name}' already exists")

        if not callable(handler):
            raise TypeError(
                f"handler must be callable, got {type(handler).__name__}"
            )

        if validator is not None and not callable(validator):
            raise TypeError(
                f"validator must be callable, got {type(validator).__name__}"
            )

        self._stages.append(Stage(
            name=name,
            handler=handler,
            validator=validator,
            on_error=on_error,
            fallback_value=fallback_value,
        ))

        if name not in self._metrics:
            self._metrics[name] = _StageMetrics()

        return self

    def add_middleware(self, hook: Callable[..., None]) -> "Pipeline":
        """
        Add a middleware hook called before/after each stage.

        The hook receives: stage_name, phase ("before"/"after"),
        data, and optional kwargs (duration_ms, error, status).

        Args:
            hook: Middleware callable.

        Returns:
            Self for method chaining.

        Raises:
            TypeError: If hook is not callable.
        """
        if not callable(hook):
            raise TypeError(
                f"middleware hook must be callable, got {type(hook).__name__}"
            )
        self._middleware.append(hook)
        return self

    def add_branch(
        self,
        name: str,
        condition: Callable[[Any], bool],
        pipeline: "Pipeline",
    ) -> "Pipeline":
        """
        Add a conditional branch.

        If condition(data) returns True after all stages,
        data is also processed through the sub-pipeline.

        Args:
            name: Branch name.
            condition: Function that decides if branch executes.
            pipeline: Sub-pipeline to execute.

        Returns:
            Self for method chaining.

        Raises:
            ValueError: If name is empty.
            TypeError: If condition is not callable or pipeline is wrong type.
        """
        if not name or not isinstance(name, str):
            raise ValueError("Branch name must be a non-empty string")

        if not callable(condition):
            raise TypeError("condition must be callable")

        if not isinstance(pipeline, Pipeline):
            raise TypeError("pipeline must be a Pipeline instance")

        self._branches.append(Branch(
            name=name,
            condition=condition,
            pipeline=pipeline,
        ))
        return self

    def _notify_middleware(
        self,
        stage_name: str,
        phase: str,
        data: Any,
        **kwargs: Any,
    ) -> None:
        """Notify all middleware hooks, swallowing exceptions."""
        for hook in self._middleware:
            try:
                hook(stage_name, phase, data, **kwargs)
            except Exception:
                pass  # Middleware must not break the pipeline

    def _execute_stage(self, stage: Stage, data: Any) -> StageResult:
        """
        Execute a single stage with validation, error handling, and timing.

        Args:
            stage: Stage to execute.
            data: Input data.

        Returns:
            StageResult with execution details.
        """
        metrics = self._metrics.setdefault(stage.name, _StageMetrics())
        metrics.calls += 1

        # Notify middleware: before
        self._notify_middleware(stage.name, "before", data)

        # Validate input
        if stage.validator is not None:
            try:
                is_valid = stage.validator(data)
            except Exception as exc:
                is_valid = False
                error_msg = f"Validator error: {exc}"
                metrics.failures += 1
                self._notify_middleware(
                    stage.name, "after", data,
                    status="failed", error=error_msg,
                )
                return StageResult(
                    stage_name=stage.name,
                    status="failed",
                    input_data=data,
                    error=error_msg,
                )

            if not is_valid:
                if stage.on_error == ErrorStrategy.SKIP:
                    metrics.skips += 1
                    self._notify_middleware(
                        stage.name, "after", data, status="skipped",
                    )
                    return StageResult(
                        stage_name=stage.name,
                        status="skipped",
                        input_data=data,
                        output_data=data,
                    )
                elif stage.on_error == ErrorStrategy.FALLBACK:
                    metrics.fallbacks += 1
                    self._notify_middleware(
                        stage.name, "after", stage.fallback_value,
                        status="fallback",
                    )
                    return StageResult(
                        stage_name=stage.name,
                        status="fallback",
                        input_data=data,
                        output_data=stage.fallback_value,
                    )
                else:  # STOP
                    metrics.failures += 1
                    error_msg = f"Validation failed at stage '{stage.name}'"
                    self._notify_middleware(
                        stage.name, "after", data,
                        status="failed", error=error_msg,
                    )
                    return StageResult(
                        stage_name=stage.name,
                        status="failed",
                        input_data=data,
                        error=error_msg,
                    )

        # Execute handler
        start = time.perf_counter()
        try:
            result = stage.handler(data)
            elapsed = (time.perf_counter() - start) * 1000
            metrics.successes += 1
            metrics.total_ms += elapsed

            self._notify_middleware(
                stage.name, "after", result,
                status="success", duration_ms=elapsed,
            )

            return StageResult(
                stage_name=stage.name,
                status="success",
                input_data=data,
                output_data=result,
                duration_ms=elapsed,
            )

        except Exception as exc:
            elapsed = (time.perf_counter() - start) * 1000
            metrics.total_ms += elapsed
            error_msg = str(exc)

            if stage.on_error == ErrorStrategy.SKIP:
                metrics.skips += 1
                self._notify_middleware(
                    stage.name, "after", data,
                    status="skipped", error=error_msg,
                    duration_ms=elapsed,
                )
                return StageResult(
                    stage_name=stage.name,
                    status="skipped",
                    input_data=data,
                    output_data=data,
                    error=error_msg,
                    duration_ms=elapsed,
                )
            elif stage.on_error == ErrorStrategy.FALLBACK:
                metrics.fallbacks += 1
                self._notify_middleware(
                    stage.name, "after", stage.fallback_value,
                    status="fallback", error=error_msg,
                    duration_ms=elapsed,
                )
                return StageResult(
                    stage_name=stage.name,
                    status="fallback",
                    input_data=data,
                    output_data=stage.fallback_value,
                    error=error_msg,
                    duration_ms=elapsed,
                )
            else:  # STOP
                metrics.failures += 1
                self._notify_middleware(
                    stage.name, "after", data,
                    status="failed", error=error_msg,
                    duration_ms=elapsed,
                )
                return StageResult(
                    stage_name=stage.name,
                    status="failed",
                    input_data=data,
                    error=error_msg,
                    duration_ms=elapsed,
                )

    def process(self, data: Any) -> PipelineResult:
        """
        Process data through all stages.

        Args:
            data: Input data to transform.

        Returns:
            PipelineResult with final data, stage results, and metrics.
        """
        pipeline_start = time.perf_counter()
        stage_results: List[StageResult] = []
        current_data = data

        # Empty pipeline — pass through
        if not self._stages:
            elapsed = (time.perf_counter() - pipeline_start) * 1000
            return PipelineResult(
                success=True,
                data=current_data,
                total_duration_ms=elapsed,
            )

        # Execute stages
        for stage in self._stages:
            result = self._execute_stage(stage, current_data)
            stage_results.append(result)

            if result.status == "failed":
                elapsed = (time.perf_counter() - pipeline_start) * 1000
                return PipelineResult(
                    success=False,
                    data=current_data,
                    stages=stage_results,
                    total_duration_ms=elapsed,
                    error=result.error,
                )

            # Update current data
            current_data = result.output_data

        # Execute branches
        for branch in self._branches:
            try:
                if branch.condition(current_data):
                    branch_result = branch.pipeline.process(current_data)
                    if branch_result.success:
                        current_data = branch_result.data
                        stage_results.extend(branch_result.stages)
            except Exception:
                pass  # Branch condition failure is non-fatal

        elapsed = (time.perf_counter() - pipeline_start) * 1000
        return PipelineResult(
            success=True,
            data=current_data,
            stages=stage_results,
            total_duration_ms=elapsed,
        )

    def process_batch(self, items: List[Any]) -> List[PipelineResult]:
        """
        Process multiple items through the pipeline.

        Args:
            items: List of data items to process.

        Returns:
            List of PipelineResult, one per item.
        """
        return [self.process(item) for item in items]

    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get execution metrics for all stages.

        Returns:
            Dictionary with stage names as keys and metric dicts as values.
        """
        result: Dict[str, Dict[str, Any]] = {}
        for name, m in self._metrics.items():
            result[name] = {
                "calls": m.calls,
                "successes": m.successes,
                "failures": m.failures,
                "skips": m.skips,
                "fallbacks": m.fallbacks,
                "total_ms": round(m.total_ms, 3),
                "avg_ms": round(m.avg_ms, 3),
            }
        return result

    def reset_metrics(self) -> None:
        """Reset all stage metrics to zero."""
        for m in self._metrics.values():
            m.calls = 0
            m.successes = 0
            m.failures = 0
            m.skips = 0
            m.fallbacks = 0
            m.total_ms = 0.0

    def __repr__(self) -> str:
        return (
            f"Pipeline(name='{self._name}', "
            f"stages={len(self._stages)}, "
            f"branches={len(self._branches)}, "
            f"middleware={len(self._middleware)})"
        )
