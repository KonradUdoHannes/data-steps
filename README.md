# data-steps

This projects provides a minmal framework to
organize data transformations in pandas.

It is intended to be used in both notebooks
and code files.

The main idea is to provide a simple decorator
syntax that is easy to maintains when data
transfromation steps get changed or added
throughout the project. A prime example
is data cleaning where only later in the project
some required cleaning steps become apparent

## Features

After wrapping a pandas DataFrame in a `DataSteps`
class. The following features are available.

- register data transformations with the instances `.step`
    decorator
- get an overview of the registered steps with `.steps`
- inspect the original data the fully transformed data
    and any partially transformed data in between
- change parameters of registered steps
- interactively redefine or deactivate steps in jupyter notebooks

## Usage Example

Wrap your data in an instance

```python
from data_steps import DataSteps

data = DataSteps(my_pandas_df)

#register transformation steps

@data.step
def data_transformation(df):
    #transfromation steps
    ...
    return transformed_df

@data.step
def transform_with_parameters(df,param1,param2=4):
    #transfromation steps
    ...
    return transformed_df

#access original data
data.original

#set or update transformation parameters
data.update_step_kwargs('transform_with_parameters',{'param1':10})

#access data after all transformation steps
data.transformed


#get an overview of the registered steps
data.steps

#only execute some steps to help debugging transformations
data.partial_transform(0)
```
