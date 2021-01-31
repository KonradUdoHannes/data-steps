from collections import namedtuple
from operator import attrgetter
import pandas as pd

Step = namedtuple('Step','priority function')

class StepCollection:
    def __init__(self):
        self._collection = {}
        
    def update_step(self,func,*args,**kwargs):
        active = kwargs.pop('active',True)
        if active:
            self.add_step(func,*args,**kwargs)
        elif func.__name__ in self._collection:
            self.remove_step(func)
    
    def add_step(self,func,priority=5):
        self._collection[func.__name__] = Step(priority,func)
    
    def remove_step(self,func):
        del self._collection[func.__name__] 
    
    @property
    def ordered_steps(self):
        return sorted(self._collection.values(),key=attrgetter('priority'))
    
    def __iter__(self):
        return map(attrgetter('function'),self.ordered_steps)
    
    def step_overview(self):
        overview = (
            pd.DataFrame(list(self.ordered_steps))
            .assign(function_name = lambda df:df['function'].map(lambda x:x.__name__))
            .drop(['function'],axis=1)
        )
        overview.index.rename('application_order',inplace=True)
        return overview

class DataSteps:
    def __init__(self,original=None):
        self._steps = StepCollection()
        self._original= original 
        
    @property
    def original(self):
        """Original data before any transformations.
        """
        if self._original is None:
            raise Exception('Original data not set. ')
        return self._original
    
    def step(self,*args,**kwargs):
        """Decorator registering functions as steps.
        
        Args:
            active (bool, optional): Function is registered
            as a step. Defaults to True.
            priority (int, optional): Priority
        
        """
        def register_function(func):
            self._steps.update_step(func,*args,**kwargs)
            return func
        return register_function
    
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
        """Transformed data after all transformations.
        """
        new_data = self.original.copy()
        for step in self._steps:
            new_data = step(new_data)
        return new_data
    
    def partial_transform(self,n):
        """Shows transformed data after the nth step.
        
        Args:
            n (int): Step after which to return
            the data. Using -1 as input returns the orignal
            data.
        """
        new_data = self.original.copy()
        for step in list(self._steps)[:n+1]:
            new_data = step(new_data)
        return new_data
        
        
    def set_original(self,original):
        """Set the original of the data.
        
        This method is intended to to set original
        data in case a DataSetps instnace is created
        without the data. This helps to create instances
        in modules and scripts, where the data will only be
        set after import.
        
        The method returns the DataSteps instance itself
        such that it can coveniently be chained with the
        with the transform property when needed.
        """
        self._original
        return self 