from __future__ import absolute_import
from __future__ import print_function
from sqlalchemy import Column, Integer, String, DateTime

from . import Base, get_default_session

_current_creator = None

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
        print("*" * 60)
        print("Run ID = ", self.id)
        print("Command line = ", self.command_line)
        print("Host = ", self.host)
        print("Username = ", self.username)
        print("Time = ", self.dtime.strftime("%d/%m/%y %H:%M"))
        if len(run.simulations) > 0:
            print(">>>   ", len(run.simulations), "simulations")
        if run.timesteps.count() > 0:
            print(">>>   ", (run.timesteps).count(), "timesteps")
        if run.halos.count() > 0:
            print(">>>   ", run.halos.count(), "halos")
        if run.halolinks.count() > 0:
            print(">>>   ", run.halolinks.count(), "halolinks")
        if run.properties.count() > 0:
            print(">>>   ", run.properties.count(), "halo properties")
        if run.simproperties.count() > 0:
            print(">>>   ", run.simproperties.count(), "simulation properties")


def get_creator(session=None):
    """Get a Creator object for this process, for the specified session.

    If session is None, return the object for the default session."""
    from sqlalchemy import inspect
    global _current_creator

    if session is None:
        session = get_default_session()

    if _current_creator is None:
        _current_creator = get_default_session().merge(Creator())
    if not inspect(_current_creator).persistent:
        get_default_session().commit()
    assert inspect(_current_creator).persistent
    
    if session is get_default_session():
        return _current_creator
    else:
        return session.query(Creator).filter_by(id=_current_creator.id).first()


def get_creator_id():
    return get_creator().id

def set_creator(creator):
    """Set the Creator object to be used in all writes during the lifetime of the current process.

    A Creator object is normally constructed automatically, but this function allows all future writes to be
    associated with a different Creator. This is mainly used by MPI runs to give the illusion that all data was
    written by one process."""
    global _current_creator
    _current_creator = creator


