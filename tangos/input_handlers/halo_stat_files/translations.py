"""Helper classes for defining translations between .stat file of different formats."""

class Function(object):
    """Define a column which is actually a function of other columns"""
    def __init__(self, fn, *input_arg_names):
        self.fn = fn
        self.input_arg_names = input_arg_names

    def __call__(self, raw_input_names, raw_input_values):
        input_args = [raw_input_values[raw_input_names.index(name)] for name in self.input_arg_names]
        return self.fn(*input_args)

    def inputs(self):
        return self.input_arg_names

class Rename(object):
    """Define a column by renaming an existing column"""
    def __init__(self, name):
        self.name = name

    def __call__(self, raw_input_names, raw_input_values):
        return raw_input_values[raw_input_names.index(self.name)]

    def inputs(self):
        return [self.name]

class Value(object):
    """Define a column by a fixed value"""
    def __init__(self, value):
        self.value = value

    def __call__(self, raw_input_names, raw_input_values):
        return self.value

    def inputs(self):
        return []