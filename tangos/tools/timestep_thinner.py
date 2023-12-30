import numpy as np
import sqlalchemy
from sqlalchemy import delete, select

from .. import core, query
from . import GenericTangosTool


class TimestepThinner(GenericTangosTool):
    tool_name = 'thin-timesteps'
    tool_description = 'Remove timesteps from a simulation, according to either an absolute or relative delta t'
    parallel = False

    @classmethod
    def add_parser_arguments(self, parser):
        parser.add_argument('--for', '--sims', action='store', nargs='*',
                            metavar='name',
                            help='Specify one or more simulations to run on',
                            dest="for_")

        parser.add_argument('--force', '-f', action='store_true',
                            help='Do not prompt before deleting')

        parser.add_argument('--relative', '-r', action='store_true', default=False,
                            help='Interpret the timestep interval as a fraction of the mean inter-timestep time; otherwise, as an absolute time')

        parser.add_argument('interval', action='store', type=float,
                            help="The maximum interval between timesteps, either as a fraction or an absolute time")

    def process_options(self, options):
        self.options = options

    def run_calculation_loop(self):
        session = core.get_default_session()
        if self.options.for_ is not None:
            sims = [query.get_simulation(s) for s in self.options.for_]
        else:
            sims = session.query(core.Simulation).all()

        for s in sims:
            self.thin_simulation(s)

    def thin_simulation(self, sim):
        print(f"Thinning simulation {sim.basename:s}")

        timesteps = sim.timesteps
        times = [ts.time_gyr for ts in timesteps]
        intervals = np.diff(times)


        if self.options.relative:
            mean_interval = np.mean(intervals)
            print(f"  Mean inter-timestep interval is {mean_interval:.3f} Gyr")
            min_interval = mean_interval * self.options.interval
        else:
            min_interval = self.options.interval

        print(f"  Timesteps where delta is less than {min_interval:.3f} Gyr:")

        to_remove = []

        dt_already_accumulated = 0.0

        for i, dt in enumerate(intervals, 1):
            if dt + dt_already_accumulated < min_interval:
                to_remove.append(timesteps[i])
                if dt_already_accumulated > 0.0:
                    print(f"    {timesteps[i].extension:s} (delta_t = {dt+dt_already_accumulated:.3f} Gyr; "
                          f"original delta_t =  {dt:.3f} Gyr)")
                else:
                    print(f"    {timesteps[i].extension:s} (delta_t = {dt:.3f} Gyr)")
                dt_already_accumulated += dt
            else:
                dt_already_accumulated = 0.0

        if len(to_remove) == 0:
            print("    None")
        else:
            print(f"  There are {len(to_remove)} timesteps to remove")

            if not self.options.force:
                print("""  Type "yes" to continue""")
                ok = input("  :").lower() == "yes"
            else:
                ok = True

            if ok:
                session = core.get_default_session()
                for ts in to_remove:
                    session.execute(
                       sqlalchemy.delete(core.TimeStep).filter(core.TimeStep.id == ts.id)
                    )
                session.commit()

            else:
                print("  Skipping")

        self._cleanup_orphan_objects()
        self._cleanup_orphan_links()
        self._cleanup_orphan_properties()

    def _cleanup_orphan_objects(self):
        engine = core.get_default_engine()
        with engine.connect() as connection:
            count = connection.execute(
                delete(core.SimulationObjectBase).filter(
                    ~core.SimulationObjectBase.timestep_id.in_(
                        select(core.TimeStep.id)
                    )
                )
            ).rowcount
            connection.commit()
        print(f"  Removed {count} orphan objects")

    def _cleanup_orphan_links(self):
        engine = core.get_default_engine()
        with engine.connect() as connection:
            count = connection.execute(
                delete(core.HaloLink).filter(
                    ~core.HaloLink.halo_to_id.in_(
                        select(core.SimulationObjectBase.id)
                    ) | ~core.HaloLink.halo_from_id.in_(
                        select(core.SimulationObjectBase.id)
                    )
                )
            ).rowcount
            connection.commit()
        print(f"  Removed {count} orphan links")

    def _cleanup_orphan_properties(self):
        engine = core.get_default_engine()

        with engine.connect() as connection:
            count = connection.execute(
                delete(core.HaloProperty).where(
                    ~core.HaloProperty.halo_id.in_(
                        select(core.SimulationObjectBase.id)
                    )
                )
            ).rowcount
            connection.commit()

        print(f"  Removed {count} orphan properties")
