import abc
import six
import argparse
from .. import core
from .. import parallel_tasks

@six.add_metaclass(abc.ABCMeta)
class GenericTangosTool(object):
    parallel = True
    tool_name = None
    tool_description = None

    def parse_command_line(self, argv=None):
        parser = self._get_parser_obj()
        options = parser.parse_args(argv)
        self.process_options(options)
        core.process_options(options)

    def _get_parser_obj(self):
        parser = self._create_parser_obj()
        self.add_parser_arguments(parser)
        return parser

    def _create_parser_obj(self):
        parser = argparse.ArgumentParser()
        core.supplement_argparser(parser)
        return parser

    @abc.abstractmethod
    def process_options(self, options):
        pass

    @abc.abstractmethod
    def run_calculation_loop(self):
        pass

    @classmethod
    @abc.abstractmethod
    def add_parser_arguments(cls, parser):
        pass

    @classmethod
    def _add_tool_to_subparser(cls, subparse):
        command = cls.tool_name
        help = cls.tool_description

        this_subparser = subparse.add_parser(command, help=help)
        cls.add_parser_arguments(this_subparser)

        def run(options):
            obj = cls()
            obj.process_options(options)
            if obj.parallel:
                parallel_tasks.launch(obj.run_calculation_loop, 2, [])
            else:
                obj.run_calculation_loop()

        this_subparser.set_defaults(func=run)

    @classmethod
    def add_tools(cls, subparse):
        if cls.tool_name is not None:
            cls._add_tool_to_subparser(subparse)
        for c in cls.__subclasses__():
            c.add_tools(subparse)

from . import add_simulation, consistent_trees_importer, crosslink, property_importer, property_writer