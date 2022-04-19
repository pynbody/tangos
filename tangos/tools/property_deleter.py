from .. import core, query
from . import GenericTangosTool


class PropertyDeleter(GenericTangosTool):
    tool_name = 'delete-properties'
    tool_description = 'Delete named properties from a halo, timestep, simulation or entire database'
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--for', '--sims', action='store', nargs='*',
                            metavar='name',
                            help='Specify one or more simulations, timesteps, or halos to run on',
                            dest="for_")

        parser.add_argument('--force', '-f', action='store_true',
                            help='Do not prompt before deleting')

        parser.add_argument('properties', action='store', nargs='+',
                            help="The names of the properties to delete")


    def process_options(self, options):
        self.options = options

    def run_calculation_loop(self):
        session = core.get_default_session()
        dictids = [core.get_dict_id(p) for p in self.options.properties]

        if self.options.for_ is not None:
            base_query = session.query(core.HaloProperty.id).filter(core.HaloProperty.name_id.in_(dictids))
            print(f"Delete {', '.join(self.options.properties)}")
            queries = []
            for s in self.options.for_:
                q = base_query
                obj = query.get_item(s)
                if isinstance(obj, core.Simulation):
                    q = q.join(core.SimulationObjectBase).join(core.TimeStep).join(core.Simulation).filter(core.Simulation.id == obj.id)
                elif isinstance(obj, core.TimeStep):
                    q = q.join(core.SimulationObjectBase).join(core.TimeStep).filter(core.TimeStep.id == obj.id)
                elif isinstance(obj, core.SimulationObjectBase):
                    q = q.join(core.SimulationObjectBase).filter(core.SimulationObjectBase.id == obj.id)

                print(f"  from {obj} ({q.count():d} total properties)")

                # it's not permitted to delete from an expression with a join, so now we need to create a wrapping query
                q = session.query(core.HaloProperty).filter(core.HaloProperty.id.in_(session.query(q.subquery())))
                queries.append(q)
        else:
            queries = [session.query(core.HaloProperty).filter(core.HaloProperty.name_id.in_(dictids))]
            print(f"Delete {', '.join(self.options.properties)} from entire database "
                  f"({queries[0].count()} total properties)")

        ok = self.options.force
        if not ok:
            print("""Type "yes" to continue""")
            ok = input(":").lower() == "yes"
        if ok:
            for q in queries:
                q.delete(synchronize_session=False)
            session.commit()
            print("Completed")
        else:
            print("Aborted")
