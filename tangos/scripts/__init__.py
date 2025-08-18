import os
import socket
import sys

import tangos.tools

from .. import parallel_tasks
from . import crosslink, manager, timelink, writer


def add_generic_tool(subparse, class_, command, help):
    this_subparser = subparse.add_parser(command, help=help)
    class_.add_parser_arguments(this_subparser)
    def run(options):
        obj = class_()
        obj.process_options(options)
        parallel_tasks.launch(obj.run_calculation_loop, [])
    this_subparser.set_defaults(func=run)

def find_free_port(start=6543):
    for port in range(start, 65535):  # 65535 is the maximum port number
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("localhost", port))  # Try to bind to the port
                return port  # If successful, this is a free port
            except OSError:
                pass  # If unsuccessful, the port is in use. Continue to the next port.

def add_serve_tool(subparse):
    def serve(options):

        ini_file = options.config
        if os.path.exists(ini_file):
            ini_path = ini_file
        else:
            ini_path = os.path.join(__path__[0],"web",ini_file)
        port = int(options.port) if options.port != "auto" else None
        if port is None:
            port = find_free_port(6543)

        if options.title:
            import tangos.web.crumbs
            tangos.web.crumbs.servername = options.title


        sys.argv = ["",ini_path,f"port={port}"]

        from importlib.metadata import entry_points

        # Find the 'pserve' entry point in the 'console_scripts' group
        pserve_entry_point = next(
            (ep for ep in entry_points(group='console_scripts') if ep.name == 'pserve'),
            None
        )
        if pserve_entry_point is None:
            raise RuntimeError("Could not find the 'pserve' entry point in 'console_scripts'.")

        sys.exit(pserve_entry_point.load()())

    web_subparser = subparse.add_parser("serve", help="Start a web server (shortcut to Pyramid's pserve)")
    web_subparser.add_argument('config', action='store', nargs="?",
                               help="The name of the pserve configuration file; either a path or production.ini/development.ini to use tangos' suggested configurations",
                               default="production.ini")
    web_subparser.add_argument('port', action='store', nargs="?",
                               help="The port to listen on. If not specified, looks for a free port starting at 6543.",
                               default="auto")
    web_subparser.add_argument('--title', '-t', action='store', nargs="?",
                               help="The mame of the server to display in the web interface. Default is 'tangos on [hostname]'",
                               default=None)
    web_subparser.set_defaults(func=serve)

def add_commands(subparse):
    tangos.tools.GenericTangosTool.add_tools(subparse)
    add_serve_tool(subparse)

def main(argv=None):
    parser, subparse = manager.get_argument_parser_and_subparsers()

    add_commands(subparse)

    args = parser.parse_args(argv)

    from .. import core
    core.process_options(args)
    args.func(args)
