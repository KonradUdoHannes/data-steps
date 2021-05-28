import inspect
import re


class DataStepsStringExport:
    def __init__(self, step_collection, export_name=None, without_data_steps=False):
        self._steps = step_collection
        self._name = export_name
        self.without_data_steps = without_data_steps

    @staticmethod
    def _remove_decorator(code):
        lines = code.split("\n")
        definition_index = 0
        for line in lines:
            if line.strip().startswith("def"):
                break
            definition_index += 1
        return "\n".join(code.split("\n")[definition_index:])

    @staticmethod
    def _step_to_pipe(step):
        if step.has_secondary_result:
            function = f"lambda x: {step.name}(x)[0]"
        else:
            function = step.name

        if len(step.function_kwargs) == 0:
            return f".pipe({function})"
        return f".pipe({function},**{step.function_kwargs})"

    def _create_transformation_function(self):
        TAB_SPACES = 4
        pipes = 2 * TAB_SPACES * " " + ("\n" + 2 * TAB_SPACES * " ").join(
            [self._step_to_pipe(step) for step in self._steps]
        )

        function_template = (
            f"def {self._name}(input_data):\n"
            "    return (\n"
            "        input_data\n"
            f"{pipes}\n"
            "    )\n"
        )
        return function_template

    @property
    def data_steps_name(self):
        if self._name is not None:
            return self._name
        return self._name_raw

    @property
    def _name_raw(self):
        first_step = inspect.getsource(list(self._steps)[0].function)
        matches = re.match(r"^\s*@(?P<name>[^.]*)\..*", first_step)
        if matches is None:
            raise RuntimeError("Could not determine data steps name")
        return matches["name"]

    @property
    def _data_steps_function_export(self):
        return "\n".join(
            [
                re.sub(f"^@{self._name_raw}" + r"\.", f"@{self.data_steps_name}.", function_def)
                for function_def in self._function_definition_strings
            ]
        )

    @property
    def _independent_function_export(self):
        return "\n".join(
            [
                self._remove_decorator(function_def)
                for function_def in self._function_definition_strings
            ]
        )

    @property
    def _function_definition_strings(self):
        return [self._remove_indentation(inspect.getsource(step.function)) for step in self._steps]

    @staticmethod
    def _remove_indentation(code_block):
        first_line, _ = code_block.split("\n", 1)
        start_index = 0
        for char in first_line:
            if char != " ":
                break
            start_index += 1
        return "\n".join([line[start_index:] for line in code_block.split("\n")])

    @property
    def _data_steps_definition(self):
        return f"{self.data_steps_name} = DataSteps()"

    @property
    def _kwargs_settings(self):
        return "\n".join(
            [
                f'{self.data_steps_name}.update_step_kwargs("{step.name}",{step.function_kwargs})'
                for step in self._steps
                if len(step.function_kwargs) > 0
            ]
        )

    @property
    def data_steps_export(self):
        return (
            self._data_steps_definition
            + "\n\n"
            + self._data_steps_function_export
            + "\n"
            + self._kwargs_settings
        )

    @property
    def data_steps_independent_export(self):
        return self._independent_function_export + "\n" + self._create_transformation_function()

    def __repr__(self):
        if self.without_data_steps:
            return self.data_steps_independent_export
        return self.data_steps_export
