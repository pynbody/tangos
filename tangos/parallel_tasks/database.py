import sys

from .. import core
from . import message, remote_import


class MessageRequestCreatorId(message.Message):
    def process(self):
        creator_id = core.creator.get_creator_id()
        MessageDeliverCreatorId(creator_id).send(self.source)

class MessageDeliverCreatorId(message.Message):
    pass


def synchronize_creator_object(session=None):
    """Causes the tangos module to use the rank 0 processor's 'Creator' object.

    Without calling this, each process of a distributed calculation has its own Creator object
    and so the user will see multiple calculations in their history when in effect they only ran
    one (through mpirun or similar)."""
    from . import parallelism_is_active

    if session is None:
        session = core.get_default_session()

    if not parallelism_is_active():
        return

    remote_import.ImportRequestMessage(__name__).send(0)
    MessageRequestCreatorId().send(0)
    id = MessageDeliverCreatorId.receive(0).contents
    core.creator.set_creator(session.query(core.creator.Creator).filter_by(id=id).first())
