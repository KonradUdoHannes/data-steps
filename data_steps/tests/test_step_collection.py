from data_steps.single_frame import StepCollection


def test_step_collectio_add():
    sc = StepCollection()

    def sample_function(dummy):
        pass

    assert len(sc._collection) == 0
    sc._add_step(sample_function, priority=3)
    assert len(sc._collection) == 1
    assert "sample_function" in sc._collection
    assert sc._collection["sample_function"].priority == 3


def test_step_collection_overview():
    sc = StepCollection()

    def sample_function(dummy):
        pass

    sc._add_step(sample_function, priority=3)
    overview = sc.step_overview()
    assert not overview.empty
    assert "function_name" in overview
    assert "function_kwargs" in overview
    assert "priority" in overview
    assert "has_secondary_result" in overview


def test_priority_adding():
    sc = StepCollection()

    def sample_function_low_prio(dummy):
        pass

    def sample_function_mid_prio(dummy):
        pass

    def sample_function_high_prio(dummy):
        pass

    sc._add_step(sample_function_low_prio, priority=5)
    sc._add_step(sample_function_high_prio, priority=1)
    sc._add_step(sample_function_mid_prio, priority=3)

    ordered_steps = sc.ordered_steps

    assert ordered_steps[0].function.__name__ == "sample_function_high_prio"
    assert ordered_steps[1].function.__name__ == "sample_function_mid_prio"
    assert ordered_steps[2].function.__name__ == "sample_function_low_prio"


def test_reregister_no_doubles():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function(dummy):
        pass

    sc._add_step(sample_function, priority=5)
    sc._add_step(sample_function, priority=5)
    assert len(sc.ordered_steps) == 1


def test_priority_update():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function(dummy):
        pass

    sc._add_step(sample_function, priority=5)
    sc._add_step(sample_function, priority=3)
    assert sc.ordered_steps[0].priority == 3


def test_function_update():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function(dummy):
        return False

    DUMMY_VALUE = 0

    sc._add_step(sample_function, priority=5)
    assert not sc.ordered_steps[0].function(DUMMY_VALUE)

    def sample_function(dummy):
        return True

    sc._add_step(sample_function, priority=5)
    assert sc.ordered_steps[0].function(DUMMY_VALUE)


def test_remove_function():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function(dummy):
        pass

    sc._add_step(sample_function, priority=5)
    assert len(sc.ordered_steps) == 1
    sc._remove_step(sample_function)
    assert len(sc.ordered_steps) == 0


def test_empty_overview():
    sc = StepCollection()

    assert sc.step_overview().empty


def test_update_step_kwargs():
    """Relevant for jupyter use cases."""
    sc = StepCollection()

    def sample_function(dummy, a=10):
        pass

    sc._add_step(sample_function, priority=5)
    assert sc._collection["sample_function"].function_kwargs["a"] == 10
    sc.update_step_kwargs("sample_function", {"a": 30})
    assert sc._collection["sample_function"].function_kwargs["a"] == 30
