import pandas as pd
import pytest

from data_steps import DataSteps
from data_steps.single_frame import StepCollection


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


def test_step_collectio_add():
    sc = StepCollection()

    def sample_function():
        pass

    assert len(sc._collection) == 0
    sc.add_step(sample_function, priority=3)
    assert len(sc._collection) == 1
    assert "sample_function" in sc._collection
    assert sc._collection["sample_function"].priority == 3


def test_step_collection_overview():
    sc = StepCollection()

    def sample_function():
        pass

    sc.add_step(sample_function, priority=3)
    assert not sc.step_overview().empty


def test_riority_adding():
    sc = StepCollection()

    def sample_function_low_prio():
        pass

    def sample_function_mid_prio():
        pass

    def sample_function_high_prio():
        pass

    sc.add_step(sample_function_low_prio, priority=5)
    sc.add_step(sample_function_high_prio, priority=1)
    sc.add_step(sample_function_mid_prio, priority=3)

    ordered_steps = sc.ordered_steps

    assert ordered_steps[0].function.__name__ == "sample_function_high_prio"
    assert ordered_steps[1].function.__name__ == "sample_function_mid_prio"
    assert ordered_steps[2].function.__name__ == "sample_function_low_prio"


def test_reregister_no_doubles():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function():
        pass

    sc.add_step(sample_function, priority=5)
    sc.add_step(sample_function, priority=5)
    assert len(sc.ordered_steps) == 1


def test_priority_update():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function():
        pass

    sc.add_step(sample_function, priority=5)
    sc.add_step(sample_function, priority=3)
    assert sc.ordered_steps[0].priority == 3


def test_function_update():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function():
        return False

    sc.add_step(sample_function, priority=5)
    assert not sc.ordered_steps[0].function()

    def sample_function():
        return True

    sc.add_step(sample_function, priority=5)
    assert sc.ordered_steps[0].function()


def test_remove_function():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function():
        pass

    sc.add_step(sample_function, priority=5)
    assert len(sc.ordered_steps) == 1
    sc.remove_step(sample_function)
    assert len(sc.ordered_steps) == 0
