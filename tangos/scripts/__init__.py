from . import manager, writer, timelink, crosslink
from .. import parallel_tasks
import tangos.tools
import sys
import os

def add_generic_tool(subparse, class_, command, help):
    this_subparser = subparse.add_parser(command, help=help)
    class_.add_parser_arguments(this_subparser)
    def run(options):
        obj = class_()
        obj.process_options(options)
        parallel_tasks.launch(obj.run_calculation_loop, 2, [])
    this_subparser.set_defaults(func=run)

def add_serve_tool(subparse):
    def serve(options):
        from pkg_resources import load_entry_point
        ini_file = options.config
        if os.path.exists(ini_file):
            ini_path = ini_file
        else:
            ini_path = os.path.join(__path__[0],"web",ini_file)
        sys.argv = ["",ini_path]
        sys.exit(
            load_entry_point('pyramid','console_scripts','pserve')()
        )

    web_subparser = subparse.add_parser("serve", help="Start a web server (shortcut to Pyramid's pserve)")
    web_subparser.add_argument('config', action='store', nargs="?",
                               help="The name of the pserve configuration file; either a path or production.ini/development.ini to use tangos' suggested configurations",
                               default="production.ini")
    web_subparser.set_defaults(func=serve)

def add_commands(subparse):
    tangos.tools.GenericTangosTool.add_tools(subparse)
    add_serve_tool(subparse)

def main():
    parser, subparse = manager.get_argument_parser_and_subparsers()

    add_commands(subparse)

    args = parser.parse_args()

    from .. import core
    core.process_options(args)
    core.init_db()
    args.func(args)


