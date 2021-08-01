import pandas as pd
import pytest

from data_steps import DataSteps


@pytest.fixture
def raw_frame():
    return pd.DataFrame(
        {
            "Col1": [1, 2, 3, 4, 5],
            "Col2": ["A", "B", "C", "D", "E"],
            "Col3": [0.01, 0.1, 1, 10, 100],
        }
    )


def test_orig_frame(raw_frame):
    data = DataSteps(raw_frame)
    assert data.original.equals(raw_frame)
    assert data.transformed.equals(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    assert data.original.equals(raw_frame)
    assert len(data.steps) == 1
    assert not data.transformed.equals(raw_frame)
    assert data.original.pipe(inc_col1).equals(data.transformed)


def test_set_original(raw_frame):
    data = DataSteps()
    data.set_original(raw_frame)
    assert data.original.equals(raw_frame)


def test_secondary_results(raw_frame):
    data = DataSteps(raw_frame)
    assert len(data.secondary_results) == 0

    @data.step(has_secondary_result=True)
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1), frame.Col1.sum()

    assert len(data.steps) == 1
    assert len(data.secondary_results) == 1
    assert "inc_col1" in data.secondary_results
    assert raw_frame.Col1.sum() == data.secondary_results["inc_col1"]


def test_empty_steps(raw_frame):
    data = DataSteps(raw_frame)
    assert data.steps.empty


def test_redefined_step(raw_frame):
    """Test for redefinition for nb."""
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    @data.step  # noqa: F811
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    assert len(data.steps) == 1


def test_removed_step(raw_frame):
    """Test for inactive definite in a nb."""
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    @data.step(active=False)  # noqa: F811
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    assert len(data.steps) == 0


def test_partial_application(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def add_col4(frame):
        return frame.assign(Col4="constant")

    @data.step(priority=10)
    def add_col5(frame):
        return frame.assign(Col5="constant")

    assert len(data.steps) == 2
    assert data.partial_transform(-1).equals(raw_frame)
    assert "Col4" in data.partial_transform(0)
    assert "Col5" not in data.partial_transform(0)
    assert "Col4" in data.partial_transform(1)
    assert "Col5" in data.partial_transform(1)


def test_partial_secondary_results(raw_frame):
    data = DataSteps(raw_frame)

    @data.step(has_secondary_result=True)
    def add_col4(frame):
        return frame.assign(Col4="constant"), "result_1"

    @data.step(priority=10, has_secondary_result=True)
    def add_col5(frame):
        return frame.assign(Col5="constant"), "result_2"

    assert len(data.partial_secondary_results(-1)) == 0
    assert len(data.partial_secondary_results(0)) == 1
    assert data.partial_secondary_results(0)["add_col4"] == "result_1"
    assert len(data.partial_secondary_results(1)) == 2
    assert data.partial_secondary_results(1)["add_col4"] == "result_1"
    assert data.partial_secondary_results(1)["add_col5"] == "result_2"


def test_step_with_args(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame, value=10):
        return frame.assign(Col4=value)

    assert data.transformed.Col4.unique()[0] == 10

    data.update_step_kwargs("inc_col1", {"value": 20})
    assert data.transformed.Col4.unique()[0] == 20
