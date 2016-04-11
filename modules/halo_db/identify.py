from . import core
import sqlalchemy, sqlalchemy.orm, sqlalchemy.orm.dynamic, sqlalchemy.orm.query, sqlalchemy.exc
from sqlalchemy.orm import Session, relationship
from sqlalchemy import and_, Table, Column, Integer, Float, String
import copy

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class IdentityDetectiveResults(Base):
    id = Column("id",Integer,primary_key=True)
    halo_from_id = Column("halo_from_id", Integer)
    halo_to_id = Column("halo_to_id", Integer)
    weight = Column("weight", Float)
    nhops = Column("nhops", Integer)
    links = Column("links",String)
    nodes = Column("nodes",String)

    __tablename__ = "identity_detective_results"
    __table_args__ = {'prefixes': ['temporary']}

class IdentityDetective(object):
    def __init__(self, halo_from, nhops_max):
        self.halo_from = halo_from
        self.nhops_max = nhops_max

        """
        table_description = (Column("id",Integer,primary_key=True),
                                    Column("halo_from_id", Integer),
                                    Column("halo_to_id", Integer),
                                    Column("weight", Float),
                                    Column("nhops", Integer),
                                    Column("links",String),
                                    Column("nodes",String)
                             )

        self._table_results = Table('identity_detective_results',core.Base.metadata,
                                    *copy.deepcopy(table_description),prefixes=['temporary'])

        self._table_candidates = Table('identity_detective_candidates',core.Base.metadata,
                                        *copy.deepcopy(table_description),prefixes=['temporary'])
        """

    def _create_temporary_tables(self):
        #create temp table halolink_multihop(id     integer primary key, halo_from_id integer, halo_to_id integer,
        #                            weight float, nhops int, links varchar, nodes varchar);
        IdentityDetectiveResults.__table__.create(Session.object_session(self.halo_from).engine)



    def _generate_seed(self):
        pass

    def _generate_new_candidates_at_hop(self, start_hop=0):
        pass

    def _merge_candidates_into_results(self):
        pass

    def _get_final_query(self):
        pass

    def _return_results(self):
        pass

    def _drop_temporary_tables(self):
        IdentityDetectiveResults.__table__.drop()
