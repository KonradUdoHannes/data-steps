import inspect

import pandas as pd
import pytest

from data_steps import DataSteps
from data_steps.export import DataStepsStringExport
from data_steps.single_frame import Step


@pytest.fixture
def raw_frame():
    return pd.DataFrame(
        {
            "Col1": [1, 2, 3, 4, 5],
            "Col2": ["A", "B", "C", "D", "E"],
            "Col3": [0.01, 0.1, 1, 10, 100],
        }
    )


def test_extract_name():
    data = DataSteps()

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    dsse = DataStepsStringExport(data._steps)
    assert dsse.data_steps_name == "data"

    data = DataSteps()

    @data.step  # noqa: F811
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    dsse = DataStepsStringExport(data._steps, export_name="exported_pipeline")
    assert dsse.data_steps_name == "exported_pipeline"

    important_data = DataSteps()

    @important_data.step  # noqa: F811
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    dsse = DataStepsStringExport(important_data._steps)
    assert dsse.data_steps_name == "important_data"


def test_remove_decorator():
    data = DataSteps()

    @data.step
    def sample_function(dummy):
        ...

    removed_dec = DataStepsStringExport._remove_decorator(inspect.getsource(sample_function))
    assert removed_dec.strip().startswith("def")

    data = DataSteps()

    @data.step()
    def sample_function(dummy):
        ...

    removed_dec = DataStepsStringExport._remove_decorator(inspect.getsource(sample_function))
    assert removed_dec.strip().startswith("def")


def test_step_to_pipe_regular():
    def sample_function(dummy):
        ...

    step = Step(priority=1, function=sample_function)
    assert DataStepsStringExport._step_to_pipe(step) == ".pipe(sample_function)"

    def sample_function(dummy, a=20):
        ...

    step = Step(priority=1, function=sample_function)
    assert DataStepsStringExport._step_to_pipe(step) == ".pipe(sample_function,**{'a': 20})"


def test_step_to_pipe_secondary_results():
    def sample_function(dummy):
        ...

    step = Step(priority=1, function=sample_function, has_secondary_result=True)
    assert DataStepsStringExport._step_to_pipe(step) == ".pipe(lambda x: sample_function(x)[0])"

    def sample_function(dummy, a=20):
        ...

    step = Step(priority=1, function=sample_function, has_secondary_result=True)
    assert (
        DataStepsStringExport._step_to_pipe(step)
        == ".pipe(lambda x: sample_function(x)[0],**{'a': 20})"
    )


def assert_reimport(data_steps, data_steps_export, name):
    _locals = {}

    exec(str(data_steps_export), globals(), _locals)
    direct_result = data_steps.transformed
    reimport_result = _locals[name].set_original(data_steps.original).transformed

    assert direct_result.equals(reimport_result)


def assert_independent_reimport(data_steps, export, name):

    # ToDo Find better solution than to use the global
    # name space
    exec(str(export), globals())
    direct_result = data_steps.transformed
    input_data = data_steps.original
    result = globals()[name](input_data)

    assert direct_result.equals(result)


def test_export_equivalence(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1)

    assert_reimport(data, data.export("reimport"), "reimport")
    assert_independent_reimport(
        data, data.export("my_transformation", without_data_steps=True), "my_transformation"
    )


def test_export_equivalence_secondary_results(raw_frame):
    data = DataSteps(raw_frame)

    @data.step(has_secondary_result=True)
    def inc_col1(frame):
        return frame.assign(Col1=lambda df: df["Col1"] + 1), "secondary_value"

    assert_reimport(data, data.export("reimport"), "reimport")
    assert_independent_reimport(
        data, data.export("my_transformation", without_data_steps=True), "my_transformation"
    )


def test_export_equivalence_additional_parameters(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame, a=10):
        return frame.assign(Col1=lambda df: df["Col1"] + a)

    assert_reimport(data, data.export("reimport"), "reimport")
    assert_independent_reimport(
        data, data.export("my_transformation", without_data_steps=True), "my_transformation"
    )


def test_export_equivalence_additional_parameters_modified(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def inc_col1(frame, a=10):
        return frame.assign(Col1=lambda df: df["Col1"] + a)

    data.update_step_kwargs("inc_col1", {"a": 20})

    assert_reimport(data, data.export("reimport"), "reimport")
    assert_independent_reimport(
        data, data.export("my_transformation", without_data_steps=True), "my_transformation"
    )


def test_export_equivalence_two_transforms(raw_frame):
    data = DataSteps(raw_frame)

    @data.step
    def create_col4(frame):
        return frame.assign(Col4=lambda df: df["Col1"] * df["Col3"])

    @data.step(priority=1)
    def inc_col1(frame, a=10):
        return frame.assign(Col1=lambda df: df["Col1"] + a)

    data.update_step_kwargs("inc_col1", {"a": 20})

    assert_reimport(data, data.export("reimport"), "reimport")
    assert_independent_reimport(
        data, data.export("my_transformation", without_data_steps=True), "my_transformation"
    )
