import pytest

from data_steps.single_frame import Step


def test_step_no_args():
    def sample_function(dummy):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 0
    assert len(step._expected_kw) == 0


def test_step_args_no_defaults():
    def sample_function(dummy, a, b, c):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 0
    assert "a" in step._expected_kw
    assert "b" in step._expected_kw
    assert "c" in step._expected_kw


def test_step_kwonlyargs_no_defaults():
    def sample_function(dummy, *, a, b, c):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 0
    assert "a" in step._expected_kw
    assert "b" in step._expected_kw
    assert "c" in step._expected_kw


def test_step_args_mix_no_defaults():
    def sample_function(dummy, a, b, *, c):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 0
    assert "a" in step._expected_kw
    assert "b" in step._expected_kw
    assert "c" in step._expected_kw


def test_step_args_defaults():
    def sample_function(dummy, a, b=10, c=20):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 2
    assert step.function_kwargs["b"] == 10
    assert step.function_kwargs["c"] == 20
    assert "a" in step._expected_kw
    assert "b" in step._expected_kw
    assert "c" in step._expected_kw


def test_step_kwonlyargs_defaults():
    def sample_function(dummy, *, a, b=10, c=20):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 2
    assert step.function_kwargs["b"] == 10
    assert step.function_kwargs["c"] == 20
    assert "a" in step._expected_kw
    assert "b" in step._expected_kw
    assert "c" in step._expected_kw


def test_step_args_mix_defaults():
    def sample_function(dummy, a, b=10, *, c=20):
        ...

    step = Step(priority=1, function=sample_function)
    assert isinstance(step.function_kwargs, dict)
    assert len(step.function_kwargs) == 2
    assert step.function_kwargs["b"] == 10
    assert step.function_kwargs["c"] == 20
    assert "a" in step._expected_kw
    assert "b" in step._expected_kw
    assert "c" in step._expected_kw


def test_update_args():
    def sample_function(dummy, a, b=10, *, c=20):
        ...

    step = Step(priority=1, function=sample_function)
    assert len(step.function_kwargs) == 2
    step.update_function_kwargs({"a": "new_value", "b": 30})
    assert len(step.function_kwargs) == 3
    assert step.function_kwargs["a"] == "new_value"
    assert step.function_kwargs["b"] == 30


def test_update_args_invalid_argument():
    def sample_function(dummy, a, b=10, *, c=20):
        ...

    step = Step(priority=1, function=sample_function)
    with pytest.raises(ValueError) as exc_info:
        step.update_function_kwargs({"invalid_argument": "irrelevant_value"})

    assert "invalid_argument" in str(exc_info)


def test_step_invalid_function():
    def no_arg_function():
        ...

    with pytest.raises(ValueError):
        Step(priority=1, function=no_arg_function)


def test_apply_step_no_side_result():
    def inc(x):
        return x + 1

    res, side_result = Step(priority=1, function=inc).apply(5)
    assert res == 6
    assert side_result is None


def test_apply_step_side_result():
    def sum_len(x):
        return sum(x), len(x)

    res, side_result = Step(priority=1, function=sum_len).apply([1, 2, 3])
    assert res == 6
    assert side_result == 3


def test_step_name():
    def sample_function(dummy):
        ...

    assert Step(priority=1, function=sample_function).name == "sample_function"
