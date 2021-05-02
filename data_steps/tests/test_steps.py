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

    print(data._steps._collection)

    assert data.original.equals(raw_frame)
    assert len(data.steps) == 1
    assert not data.transformed.equals(raw_frame)
    assert data.original.pipe(inc_col1).equals(data.transformed)


def test_empty_steps(raw_frame):
    data = DataSteps(raw_frame)
    assert data.steps.empty


def test_redefined_step(raw_frame):
    """Test for redefinition for nb."""
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    @data.step
    def inc_col1(frame):  # noqa: F811
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    assert len(data.steps) == 1


def test_removed_step(raw_frame):
    """Test for inactive definite in a nb."""
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    @data.step(active=False)
    def inc_col1(frame):  # noqa: F811
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    assert len(data.steps) == 0


def test_partial_application(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def inc_col4(frame):
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
