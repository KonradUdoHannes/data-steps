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

## Usage

Wrap your data in an instance

```python
from data_steps import DataSteps

my_data = DataSteps(my_pandas_df)

#register transformation steps

@my_data.step
def data_transformation(data):
    #transfromation steps
    ...
    return transformed data

#access original data
my_data.original

#access data after all transformation steps
my_data.transformed

#get an overview of the registered steps
my_data.steps

#only execute some steps to help debugging transformations
my_data.partial_transform(0)
```
