import pytest
import pandas as pd

from data_steps import DataSteps
from data_steps.single_frame import StepCollection


@pytest.fixture
def raw_frame():
    return pd.DataFrame(
        { 'Col1' : [1,2,3,4,5],
          'Col2' : ['A','B','C','D','E'],
          'Col3' : [0.01,0.1,1,10,100],
        }
    )

def test_orig_frame(raw_frame):
    data = DataSteps(raw_frame)
    assert data.original.equals(raw_frame)
    assert data.transformed.equals(raw_frame)

    @data.step
    def inc_col1(frame):
        return frame.assign(Col1 = lambda df:df['Col1'] + 1)

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
    sc.add_step(sample_function,priority = 3)
    assert len(sc._collection) == 1
    assert 'sample_function' in sc._collection
    assert sc._collection['sample_function'].priority == 3

def test_step_collection_overview():
    sc = StepCollection()
    def sample_function():
        pass 
    sc.add_step(sample_function,priority = 3)
    assert not sc.step_overview().empty
