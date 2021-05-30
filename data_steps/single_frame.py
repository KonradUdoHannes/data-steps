import inspect
from dataclasses import dataclass, field
from operator import attrgetter
from typing import Callable

import pandas as pd

from data_steps.export import DataStepsStringExport


@dataclass
class Step:
    priority: int
    function: Callable
    has_secondary_result: bool = False
    function_kwargs: dict = field(init=False)

    def __post_init__(self):
        argspec = inspect.getfullargspec(self.function)
        if len(argspec.args) == 0:
            raise ValueError("Steps need at least one argument")
        self._expected_kw = argspec.args[1:] + argspec.kwonlyargs
        args_defaults = argspec.defaults or []
        kwonly_defaults = argspec.kwonlydefaults or {}
        combined_arg_defaults = reversed(list(zip(reversed(argspec.args), reversed(args_defaults))))

        self.function_kwargs = {
            arg: value for arg, value in combined_arg_defaults
        } | kwonly_defaults

    @property
    def name(self):
        return self.function.__name__

    def update_function_kwargs(self, kwargs):
        for key in kwargs:
            if key not in self._expected_kw:
                raise ValueError(f"Unexpected argument {key} for {self.function.__name__}")
        self.function_kwargs |= kwargs

    def apply(self, data):
        """Returns the results of applying the steps.

        Result are split in two. The first return value
        is the primary result intended to be passed along
        in a pipeline applications. The secondary result
        is not intended to be passed along and therefore optional.
        In case no secondary results have been calculated None is returned
        such that the return value is always a two tuple.
        """
        step_result = self.function(data, **self.function_kwargs)
        if self.has_secondary_result:
            return step_result
        return step_result, None


class StepCollection:
    def __init__(self):
        self._collection: dict[str, Step] = {}

    def update_step(self, func, priority, **kwargs):
        active = kwargs.pop("active", True)
        if active:
            self._add_step(func, priority, **kwargs)
        elif func.__name__ in self._collection:
            self._remove_step(func)

    def _add_step(self, func, priority, **kwargs):
        self._collection[func.__name__] = Step(priority, func, **kwargs)

    def _remove_step(self, func):
        del self._collection[func.__name__]

    def update_step_kwargs(self, function_name, kwargs):
        self._collection[function_name].update_function_kwargs(kwargs)

    @property
    def ordered_steps(self):
        return sorted(self._collection.values(), key=attrgetter("priority"))

    def __iter__(self):
        return iter(self.ordered_steps)

    def step_overview(self):
        steps = list(self.ordered_steps)
        if len(steps) == 0:
            return pd.DataFrame()

        overview = (
            pd.DataFrame(list(self.ordered_steps))
            .assign(function_name=lambda df: df["function"].map(lambda x: x.__name__))
            .drop(["function"], axis=1)
            .loc[:, ["priority", "function_name", "function_kwargs", "has_secondary_result"]]
        )
        overview.index.rename("application_order", inplace=True)
        return overview


class DataSteps:
    def __init__(self, original: pd.DataFrame = None):
        self._steps = StepCollection()
        self._original = original

    @property
    def original(self) -> pd.DataFrame:
        """Original data before any transformations."""
        if self._original is None:
            raise Exception("Original data not set. ")
        return self._original

    def step(self, function=None, *, priority=5, active=True, **kwargs):
        """Decorator registering functions as steps.

        The decorator can be used in a bare version, i.e.
        as <instance>.step. This should be sufficient
        as long as the transformation order does not matter
        or one doesn't want to remove steps.
        Alternatively the decorator can be used with keyword
        arguments as <instance>.step(...)

        Args:
            priority (int, optional): Priority of the transformation
                lower priorities are executed first, so prioiriteis
                should be read as 1st, 2nd etc. Default value is 5.
            active (bool, optional): Function is registered
                as a step. Defaults to True.

        Kwargs:
            has_secondary_result (bool, optional): If True the decorated
            function is expected to return a 2-tuple. The first entry of
            the tuple is expected to be the result that is passed along the
            transformation steps. The second entry is the secondary result.
            This is not passed along and is collected seperately in the
            secondary_results property. Usage might be for diagnostic
            summaries figure objects for plots etc.
        """

        def register_function(func):
            self._steps.update_step(func, priority=priority, active=active, **kwargs)
            return func

        if function is None:
            return register_function

        return register_function(function)

    @property
    def steps(self):
        """All currently active steps.

        Steps are applied in the specified order, i.e.
        steps with a smaller applicaton_order number are applied
        before steps with a larger one. Steps with small priority
        numbers are applied before steps with higher priority numbers.
        Steps with the same prioirity should be interchangegable.
        """
        return self._steps.step_overview()

    @property
    def transformed(self):
        """Transformed data after all transformations."""
        new_data = self.original.copy()
        for step in self._steps:
            new_data, _ = step.apply(new_data)
        return new_data

    @property
    def secondary_results(self):
        """All secondary results after all transformations."""
        results = {}
        new_data = self.original.copy()
        for step in self._steps:
            new_data, secondary_result = step.apply(new_data)
            if secondary_result is not None:
                results[step.name] = secondary_result
        return results

    def partial_secondary_results(self, n: int):
        """Shows secondary resutls data after the nth step.

        Args:
            n (int): Step after which to return
                the data. Using -1 as is allowed to support
                the same arguments as partial_transform, but
                will always return an empty dictionary.
        """
        results = {}
        new_data = self.original.copy()
        for step in list(self._steps)[: n + 1]:
            new_data, secondary_result = step.apply(new_data)
            if secondary_result is not None:
                results[step.name] = secondary_result
        return results

    def partial_transform(self, n: int):
        """Shows transformed data after the nth step.

        Args:
            n (int): Step after which to return
                the data. Using -1 as input returns the orignal
                data.
        """
        new_data = self.original.copy()
        for step in list(self._steps)[: n + 1]:
            new_data, _ = step.apply(new_data)
        return new_data

    def set_original(self, original: pd.DataFrame) -> "DataSteps":
        """Set the original of the data.

        This method is intended to to set original
        data in case a DataSteps instnace is created
        without the data. This helps to create instances
        in modules and scripts, where the data will only be
        set after import.

        The method returns the DataSteps instance itself
        such that it can coveniently be chained with the
        with the transform property when needed.
        """
        self._original = original
        return self

    def update_step_kwargs(self, step_name: str, kwargs):
        """Set set or update keyworkd arguments for steps.

        Changes the additional arguments used by the steps.
        The first argument of each step is always a dataframe,
        but all firther arguments can be passed as keyword
        arguments. In order to use the transformed property
        of data steps all required arguments must be set.


        Args:
            step_name (str): Name of the step for which
                parameters should be updated. This is equal
                to the name of the function that is decorated
                with the step decorator.
            kwargs (dict): Keyword argument which are used
                for the update.
        """
        self._steps.update_step_kwargs(step_name, kwargs)

    def export(self, name=None, without_data_steps=False):
        """Exports Data Steps as a String.

        DataSteps are supposed to help with work in
        notebooks. In order to help convert an exploratory
        analysis into a module this function helps to obtain
        strings that can be copied into a module. The string
        is intended to be copied manually. In order to
        work nicely with notebooks this function does not
        return a string directly but an Object with an
        implmeneted __repr__ method. To get the string
        object use pythons built in `str` function.

        The default export that uses DataSteps itself
        is intended to be used as followes. It defines
        a DataSteps instance without original data set,
        but with all stepts registered. This instance can
        then be imported from a module and used by setting
        the original data with the .set_original method.

        Args:
            name (str, optional): Replace the name of the
                data steps object to be used for the export.
            without_data_teps (bool): Creates an export that does
                not rely on the datasteps module. Instead a
                function is created that applies all the
                transformations. The default name of the function
                is the name of the DataSteps instance.
        """
        return DataStepsStringExport(self._steps, name, without_data_steps=without_data_steps)
