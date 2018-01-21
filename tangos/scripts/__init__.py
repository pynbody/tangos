from . import manager, writer, timelink, crosslink
from .. import parallel_tasks
from tangos.tools.property_writer import PropertyWriter
from tangos.tools.crosslink import TimeLinker, CrossLinker


def add_generic_tool(subparse, class_, command, help):
    this_subparser = subparse.add_parser(command, help=help)
    class_.add_parser_arguments(this_subparser)
    def run(options):
        obj = class_()
        obj.process_options(options)
        parallel_tasks.launch(obj.run_calculation_loop, 2, [])
    this_subparser.set_defaults(func=run)

def add_commands(subparse):
    add_generic_tool(subparse, PropertyWriter, 'write', "Calculate properties and write them into the tangos database")

    add_generic_tool(subparse, TimeLinker, 'link',
                     "Generate merger tree and other information linking tangos objects over time")

    add_generic_tool(subparse, CrossLinker, 'crosslink',
                     "Identify the same objects between two simulations and link them")

def main():
    parser, subparse = manager.get_argument_parser_and_subparsers()

    add_commands(subparse)

    args = parser.parse_args()

    from .. import core
    core.process_options(args)
    core.init_db()
    args.func(args)


