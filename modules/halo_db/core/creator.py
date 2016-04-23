from sqlalchemy import Column, Integer, String, DateTime

from . import Base


class Creator(Base):
    __tablename__ = 'creators'

    id = Column(Integer, primary_key=True)
    command_line = Column(String)
    dtime = Column(DateTime)
    host = Column(String)
    username = Column(String)
    cwd = Column(String)

    def __repr__(self):
        return "<Creator " + self.username + " on " + self.host + " @ " + self.dtime.strftime("%d/%m/%y %H:%M") + " via " + self.command_line.split(" ")[0].split("/")[-1] + ">"

    def __init__(self, argv=None):
        import socket
        import getpass
        import datetime
        import os
        if argv == None:
            import sys
            argv = sys.argv

        self.command_line = " ".join(argv)
        self.host = socket.gethostname()
        self.username = getpass.getuser()
        self.dtime = datetime.datetime.now()
        self.cwd = os.getcwd()

    def print_info(self):
        run = self
        print "*" * 60
        print "Run ID = ", self.id
        print "Command line = ", self.command_line
        print "Host = ", self.host
        print "Username = ", self.username
        print "Time = ", self.dtime.strftime("%d/%m/%y %H:%M")
        if len(run.simulations) > 0:
            print ">>>   ", len(run.simulations), "simulations"
        if run.timesteps.count() > 0:
            print ">>>   ", (run.timesteps).count(), "timesteps"
        if run.halos.count() > 0:
            print ">>>   ", run.halos.count(), "halos"
        if run.halolinks.count() > 0:
            print ">>>   ", run.halolinks.count(), "halolinks"
        if run.properties.count() > 0:
            print ">>>   ", run.properties.count(), "halo properties"
        if run.simproperties.count() > 0:
            print ">>>", run.simproperties.count(), "simulation properties"