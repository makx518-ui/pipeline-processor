"""
Comprehensive tests for Pipeline Processor.
Covers: stages, validation, error strategies, middleware,
branching, batch processing, metrics, edge cases, performance.
"""

import pytest
from typing import Any
from pipeline import (
    Pipeline,
    ErrorStrategy,
)


# ============================================================
# 1. Basic Pipeline
# ============================================================

class TestBasicPipeline:
    """Tests for basic pipeline creation and processing."""

    def test_empty_pipeline(self):
        p = Pipeline()
        result = p.process(42)
        assert result.success is True
        assert result.data == 42
        assert result.stages == []

    def test_single_stage(self):
        p = Pipeline()
        p.add_stage("double", handler=lambda x: x * 2)
        result = p.process(5)
        assert result.success is True
        assert result.data == 10

    def test_chain_stages(self):
        p = Pipeline()
        p.add_stage("add_one", handler=lambda x: x + 1)
        p.add_stage("double", handler=lambda x: x * 2)
        p.add_stage("to_str", handler=lambda x: str(x))
        result = p.process(4)
        assert result.success is True
        assert result.data == "10"  # (4+1)*2 = 10 -> "10"

    def test_method_chaining(self):
        p = (
            Pipeline("chain")
            .add_stage("a", handler=lambda x: x + 1)
            .add_stage("b", handler=lambda x: x * 3)
        )
        result = p.process(2)
        assert result.data == 9  # (2+1)*3

    def test_stage_results_recorded(self):
        p = Pipeline()
        p.add_stage("step1", handler=lambda x: x + 1)
        p.add_stage("step2", handler=lambda x: x * 2)
        result = p.process(5)
        assert len(result.stages) == 2
        assert result.stages[0].stage_name == "step1"
        assert result.stages[0].status == "success"
        assert result.stages[1].stage_name == "step2"

    def test_pipeline_name(self):
        p = Pipeline("etl")
        assert p.name == "etl"

    def test_stage_count(self):
        p = Pipeline()
        assert p.stage_count == 0
        p.add_stage("a", handler=lambda x: x)
        p.add_stage("b", handler=lambda x: x)
        assert p.stage_count == 2


# ============================================================
# 2. Validation
# ============================================================

class TestValidation:
    """Tests for input validation and stage creation errors."""

    def test_duplicate_stage_name(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x)
        with pytest.raises(ValueError):
            p.add_stage("step", handler=lambda x: x)

    def test_empty_stage_name(self):
        p = Pipeline()
        with pytest.raises(ValueError):
            p.add_stage("", handler=lambda x: x)

    def test_non_callable_handler(self):
        p = Pipeline()
        with pytest.raises(TypeError):
            p.add_stage("bad", handler="not_callable")  # type: ignore[arg-type]

    def test_non_callable_validator(self):
        p = Pipeline()
        with pytest.raises(TypeError):
            p.add_stage("bad", handler=lambda x: x, validator="nope")  # type: ignore[arg-type]

    def test_non_callable_middleware(self):
        p = Pipeline()
        with pytest.raises(TypeError):
            p.add_middleware("not_callable")  # type: ignore[arg-type]

    def test_empty_branch_name(self):
        p = Pipeline()
        with pytest.raises(ValueError):
            p.add_branch("", condition=lambda x: True, pipeline=Pipeline())

    def test_branch_wrong_pipeline_type(self):
        p = Pipeline()
        with pytest.raises(TypeError):
            p.add_branch("b", condition=lambda x: True, pipeline="not_pipe")  # type: ignore[arg-type]


# ============================================================
# 3. Stage Validators
# ============================================================

class TestStageValidators:
    """Tests for input validation at stage level."""

    def test_validator_passes(self):
        p = Pipeline()
        p.add_stage("check", handler=lambda x: x * 2,
                    validator=lambda x: x > 0)
        result = p.process(5)
        assert result.success is True
        assert result.data == 10

    def test_validator_fails_stop(self):
        p = Pipeline()
        p.add_stage("check", handler=lambda x: x * 2,
                    validator=lambda x: x > 0,
                    on_error=ErrorStrategy.STOP)
        result = p.process(-1)
        assert result.success is False
        assert "Validation failed" in (result.error or "")

    def test_validator_fails_skip(self):
        p = Pipeline()
        p.add_stage("check", handler=lambda x: x * 2,
                    validator=lambda x: x > 0,
                    on_error=ErrorStrategy.SKIP)
        p.add_stage("add_one", handler=lambda x: x + 1)
        result = p.process(-1)
        assert result.success is True
        assert result.data == 0  # -1 skipped, then -1 + 1 = 0

    def test_validator_fails_fallback(self):
        p = Pipeline()
        p.add_stage("check", handler=lambda x: x * 2,
                    validator=lambda x: x > 0,
                    on_error=ErrorStrategy.FALLBACK,
                    fallback_value=0)
        result = p.process(-1)
        assert result.success is True
        assert result.data == 0

    def test_validator_exception(self):
        def bad_validator(x: int) -> bool:
            raise RuntimeError("validator crash")

        p = Pipeline()
        p.add_stage("check", handler=lambda x: x,
                    validator=bad_validator)
        result = p.process(5)
        assert result.success is False
        assert "Validator error" in (result.error or "")


# ============================================================
# 4. Error Strategies
# ============================================================

class TestErrorStrategies:
    """Tests for handler error handling strategies."""

    def test_handler_error_stop(self):
        def explode(x: int) -> int:
            raise RuntimeError("boom")

        p = Pipeline()
        p.add_stage("bomb", handler=explode, on_error=ErrorStrategy.STOP)
        result = p.process(1)
        assert result.success is False
        assert result.error == "boom"

    def test_handler_error_skip(self):
        def explode(x: int) -> int:
            raise RuntimeError("boom")

        p = Pipeline()
        p.add_stage("bomb", handler=explode, on_error=ErrorStrategy.SKIP)
        p.add_stage("add", handler=lambda x: x + 10)
        result = p.process(5)
        assert result.success is True
        assert result.data == 15  # skip bomb, 5 + 10

    def test_handler_error_fallback(self):
        def explode(x: int) -> int:
            raise RuntimeError("boom")

        p = Pipeline()
        p.add_stage("bomb", handler=explode,
                    on_error=ErrorStrategy.FALLBACK,
                    fallback_value=99)
        result = p.process(1)
        assert result.success is True
        assert result.data == 99

    def test_error_stops_remaining_stages(self):
        calls: list[str] = []

        def bomb_handler(x: int) -> int:
            raise RuntimeError("stop")

        p = Pipeline()
        p.add_stage("first", handler=lambda x: calls.append("first") or x)
        p.add_stage("bomb", handler=bomb_handler)
        p.add_stage("never", handler=lambda x: calls.append("never") or x)
        result = p.process(1)
        assert result.success is False
        assert "first" in calls
        assert "never" not in calls


# ============================================================
# 5. Middleware
# ============================================================

class TestMiddleware:
    """Tests for middleware hooks."""

    def test_middleware_called(self):
        log: list[str] = []

        def logger(stage_name: str, phase: str, data: Any, **kw: Any) -> None:
            log.append(f"{stage_name}:{phase}")

        p = Pipeline()
        p.add_middleware(logger)
        p.add_stage("step", handler=lambda x: x + 1)
        p.process(1)
        assert "step:before" in log
        assert "step:after" in log

    def test_middleware_multiple(self):
        log1: list[str] = []
        log2: list[str] = []

        p = Pipeline()
        p.add_middleware(lambda name, phase, data, **kw: log1.append(name))
        p.add_middleware(lambda name, phase, data, **kw: log2.append(name))
        p.add_stage("s1", handler=lambda x: x)
        p.process(1)
        assert len(log1) == 2  # before + after
        assert len(log2) == 2

    def test_middleware_exception_swallowed(self):
        def bad_hook(stage_name: str, phase: str, data: Any, **kw: Any) -> None:
            raise RuntimeError("middleware crash")

        p = Pipeline()
        p.add_middleware(bad_hook)
        p.add_stage("step", handler=lambda x: x + 1)
        result = p.process(1)
        assert result.success is True
        assert result.data == 2  # middleware crash didn't affect pipeline

    def test_middleware_receives_duration(self):
        durations: list[float] = []

        def timer(stage_name: str, phase: str, data: Any, **kw: Any) -> None:
            if "duration_ms" in kw:
                durations.append(kw["duration_ms"])

        p = Pipeline()
        p.add_middleware(timer)
        p.add_stage("step", handler=lambda x: x + 1)
        p.process(1)
        assert len(durations) == 1
        assert durations[0] >= 0


# ============================================================
# 6. Branching
# ============================================================

class TestBranching:
    """Tests for conditional pipeline branching."""

    def test_branch_taken(self):
        sub = Pipeline("sub")
        sub.add_stage("multiply", handler=lambda x: x * 10)

        p = Pipeline()
        p.add_stage("add", handler=lambda x: x + 1)
        p.add_branch("big_numbers", condition=lambda x: x > 5, pipeline=sub)
        result = p.process(10)
        assert result.data == 110  # (10+1) * 10

    def test_branch_not_taken(self):
        sub = Pipeline("sub")
        sub.add_stage("multiply", handler=lambda x: x * 10)

        p = Pipeline()
        p.add_stage("add", handler=lambda x: x + 1)
        p.add_branch("big_numbers", condition=lambda x: x > 100, pipeline=sub)
        result = p.process(5)
        assert result.data == 6  # 5+1, branch not taken

    def test_branch_condition_exception(self):
        sub = Pipeline("sub")
        sub.add_stage("multiply", handler=lambda x: x * 10)

        def bad_condition(x: int) -> bool:
            raise RuntimeError("condition crash")

        p = Pipeline()
        p.add_stage("add", handler=lambda x: x + 1)
        p.add_branch("bad", condition=bad_condition, pipeline=sub)
        result = p.process(5)
        # Branch failure is non-fatal, main result preserved
        assert result.success is True
        assert result.data == 6

    def test_nested_branches(self):
        inner = Pipeline("inner")
        inner.add_stage("square", handler=lambda x: x ** 2)

        outer = Pipeline("outer")
        outer.add_stage("negate", handler=lambda x: -x)

        p = Pipeline()
        p.add_stage("add", handler=lambda x: x + 1)
        p.add_branch("route_a", condition=lambda x: x > 0, pipeline=outer)
        result = p.process(5)
        assert result.data == -6  # (5+1), then negated


# ============================================================
# 7. Batch Processing
# ============================================================

class TestBatchProcessing:
    """Tests for process_batch."""

    def test_batch_basic(self):
        p = Pipeline()
        p.add_stage("double", handler=lambda x: x * 2)
        results = p.process_batch([1, 2, 3])
        assert len(results) == 3
        assert [r.data for r in results] == [2, 4, 6]
        assert all(r.success for r in results)

    def test_batch_empty(self):
        p = Pipeline()
        p.add_stage("double", handler=lambda x: x * 2)
        results = p.process_batch([])
        assert results == []

    def test_batch_with_failures(self):
        def safe_divide(x: int) -> float:
            return 100 / x

        p = Pipeline()
        p.add_stage("divide", handler=safe_divide,
                    on_error=ErrorStrategy.STOP)
        results = p.process_batch([10, 0, 5])
        assert results[0].success is True
        assert results[0].data == 10.0
        assert results[1].success is False  # division by zero
        assert results[2].success is True
        assert results[2].data == 20.0

    def test_batch_independent(self):
        """Each item is processed independently."""
        counter = {"n": 0}

        def count(x: int) -> int:
            counter["n"] += 1
            return x + counter["n"]

        p = Pipeline()
        p.add_stage("count", handler=count)
        results = p.process_batch([0, 0, 0])
        assert results[0].data == 1
        assert results[1].data == 2
        assert results[2].data == 3


# ============================================================
# 8. Metrics
# ============================================================

class TestMetrics:
    """Tests for execution metrics."""

    def test_metrics_basic(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x + 1)
        p.process(1)
        metrics = p.get_metrics()
        assert "step" in metrics
        assert metrics["step"]["calls"] == 1
        assert metrics["step"]["successes"] == 1
        assert metrics["step"]["total_ms"] >= 0

    def test_metrics_accumulate(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x + 1)
        p.process(1)
        p.process(2)
        p.process(3)
        metrics = p.get_metrics()
        assert metrics["step"]["calls"] == 3
        assert metrics["step"]["successes"] == 3

    def test_metrics_failure_counted(self):
        def explode(x: int) -> int:
            raise RuntimeError("boom")

        p = Pipeline()
        p.add_stage("bomb", handler=explode)
        p.process(1)
        metrics = p.get_metrics()
        assert metrics["bomb"]["failures"] == 1

    def test_metrics_skip_counted(self):
        def explode(x: int) -> int:
            raise RuntimeError("boom")

        p = Pipeline()
        p.add_stage("skip_me", handler=explode,
                    on_error=ErrorStrategy.SKIP)
        p.process(1)
        metrics = p.get_metrics()
        assert metrics["skip_me"]["skips"] == 1

    def test_metrics_fallback_counted(self):
        def explode(x: int) -> int:
            raise RuntimeError("boom")

        p = Pipeline()
        p.add_stage("fallback_me", handler=explode,
                    on_error=ErrorStrategy.FALLBACK, fallback_value=0)
        p.process(1)
        metrics = p.get_metrics()
        assert metrics["fallback_me"]["fallbacks"] == 1

    def test_reset_metrics(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x)
        p.process(1)
        p.reset_metrics()
        metrics = p.get_metrics()
        assert metrics["step"]["calls"] == 0
        assert metrics["step"]["successes"] == 0

    def test_metrics_avg_ms(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x)
        p.process(1)
        p.process(2)
        metrics = p.get_metrics()
        assert metrics["step"]["avg_ms"] >= 0


# ============================================================
# 9. Data Types
# ============================================================

class TestDataTypes:
    """Tests for various data types flowing through pipeline."""

    def test_dict_data(self):
        p = Pipeline()
        p.add_stage("add_key", handler=lambda d: {**d, "new": True})
        result = p.process({"existing": 1})
        assert result.data == {"existing": 1, "new": True}

    def test_list_data(self):
        p = Pipeline()
        p.add_stage("sort", handler=lambda lst: sorted(lst))
        p.add_stage("reverse", handler=lambda lst: list(reversed(lst)))
        result = p.process([3, 1, 2])
        assert result.data == [3, 2, 1]

    def test_string_data(self):
        p = Pipeline()
        p.add_stage("upper", handler=lambda s: s.upper())
        p.add_stage("strip", handler=lambda s: s.strip())
        result = p.process("  hello  ")
        assert result.data == "HELLO"

    def test_none_handler_result(self):
        p = Pipeline()
        p.add_stage("to_none", handler=lambda x: None)
        p.add_stage("check", handler=lambda x: "was_none" if x is None else "not_none")
        result = p.process(42)
        assert result.data == "was_none"


# ============================================================
# 10. Performance
# ============================================================

class TestPerformance:
    """Stress tests."""

    def test_hundred_stages(self):
        p = Pipeline()
        for i in range(100):
            p.add_stage(f"stage_{i}", handler=lambda x: x + 1)
        result = p.process(0)
        assert result.success is True
        assert result.data == 100
        assert len(result.stages) == 100

    def test_large_batch(self):
        p = Pipeline()
        p.add_stage("double", handler=lambda x: x * 2)
        results = p.process_batch(list(range(1000)))
        assert len(results) == 1000
        assert all(r.success for r in results)
        assert results[999].data == 1998

    def test_metrics_after_batch(self):
        p = Pipeline()
        p.add_stage("inc", handler=lambda x: x + 1)
        p.process_batch(list(range(500)))
        metrics = p.get_metrics()
        assert metrics["inc"]["calls"] == 500
        assert metrics["inc"]["successes"] == 500


# ============================================================
# 11. Repr
# ============================================================

class TestRepr:
    """Test string representation."""

    def test_repr_empty(self):
        p = Pipeline("test")
        r = repr(p)
        assert "test" in r
        assert "stages=0" in r

    def test_repr_full(self):
        p = Pipeline("etl")
        p.add_stage("a", handler=lambda x: x)
        p.add_middleware(lambda *a, **k: None)
        p.add_branch("b", condition=lambda x: True, pipeline=Pipeline())
        r = repr(p)
        assert "stages=1" in r
        assert "branches=1" in r
        assert "middleware=1" in r


# ============================================================
# 12. Pipeline Result Details
# ============================================================

class TestPipelineResult:
    """Tests for result object details."""

    def test_total_duration(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x)
        result = p.process(1)
        assert result.total_duration_ms >= 0

    def test_stage_duration(self):
        p = Pipeline()
        p.add_stage("step", handler=lambda x: x)
        result = p.process(1)
        assert result.stages[0].duration_ms >= 0

    def test_failed_result_has_error(self):
        def explode(x: int) -> int:
            raise ValueError("test error")

        p = Pipeline()
        p.add_stage("bomb", handler=explode)
        result = p.process(1)
        assert result.success is False
        assert result.error == "test error"

    def test_successful_result_no_error(self):
        p = Pipeline()
        p.add_stage("ok", handler=lambda x: x)
        result = p.process(1)
        assert result.success is True
        assert result.error is None


# ============================================================
# 13. Extended Edge Cases
# ============================================================

class TestExtendedEdgeCases:
    """Additional edge cases for maximum coverage."""

    def test_stage_result_input_data_preserved(self):
        """StageResult captures the original input data."""
        p = Pipeline()
        p.add_stage("double", handler=lambda x: x * 2)
        result = p.process(7)
        assert result.stages[0].input_data == 7
        assert result.stages[0].output_data == 14

    def test_multiple_branches_both_true(self):
        """When two branches both match, both execute sequentially."""
        branch_a = Pipeline("a")
        branch_a.add_stage("add_10", handler=lambda x: x + 10)

        branch_b = Pipeline("b")
        branch_b.add_stage("double", handler=lambda x: x * 2)

        p = Pipeline()
        p.add_stage("start", handler=lambda x: x + 1)
        p.add_branch("route_a", condition=lambda x: True, pipeline=branch_a)
        p.add_branch("route_b", condition=lambda x: True, pipeline=branch_b)
        result = p.process(4)
        # 4+1=5, branch_a: 5+10=15, branch_b: 15*2=30
        assert result.success is True
        assert result.data == 30

    def test_branch_receives_data_after_stages(self):
        """Branch receives data AFTER all stages, not before."""
        received: list[int] = []

        branch = Pipeline("check")
        branch.add_stage("capture", handler=lambda x: received.append(x) or x)

        p = Pipeline()
        p.add_stage("add_100", handler=lambda x: x + 100)
        p.add_branch("spy", condition=lambda x: True, pipeline=branch)
        p.process(1)
        # Branch should receive 101 (after stage), not 1 (before stage)
        assert received == [101]
