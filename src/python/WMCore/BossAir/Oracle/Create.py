"""
_Create_

Base class for creating the BossAir database
"""
from __future__ import print_function

import threading

from WMCore.Database.DBCreator import DBCreator
from WMCore.WMException import WMException


class Create(DBCreator):
    """
    This should create the BossAir schema; since they don't do it.

    """
    sequence_tables = ['bl_runjob', 'bl_status']

    def __init__(self, logger=None, dbi=None, params=None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """

        myThread = threading.currentThread()

        if logger is None:
            logger = myThread.logger
        if dbi is None:
            dbi = myThread.dbi

        DBCreator.__init__(self, logger, dbi)

        tablespaceTable = ""
        tablespaceIndex = ""

        self.create = {}
        self.constraints = {}
        self.indexes = {}

        if params:
            if "tablespace_table" in params:
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if "tablespace_index" in params:
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.requiredTables = ["01bl_status", "02bl_runjob"]

        self.sequence_tables = []
        self.sequence_tables.append("bl_status")
        self.sequence_tables.append("bl_runjob")

        self.create['01bl_status'] = \
            """CREATE TABLE bl_status (
                id            INTEGER  NOT NULL,
                name          VARCHAR(255)
                ) %s  """ % (tablespaceTable)

        self.create['02bl_runjob'] = \
            """CREATE TABLE bl_runjob
               (
               id            INTEGER NOT NULL,
               wmbs_id       INTEGER,
               grid_id       VARCHAR(255),
               bulk_id       VARCHAR(255),
               status        CHAR(1)   DEFAULT '1',
               sched_status  INTEGER NOT NULL,
               retry_count   INTEGER,
               status_time   INTEGER,
               location      INTEGER,
               user_id       INTEGER
               ) %s  """ % (tablespaceTable)

        self.indexes["01_pk_bl_status"] = \
            """ALTER TABLE bl_status ADD
                 (CONSTRAINT bl_status_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.indexes["02_pk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
                 (CONSTRAINT bl_runjob_pk PRIMARY KEY (id) %s)""" % tablespaceIndex

        self.constraints["01_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk1 FOREIGN KEY(wmbs_id)
               REFERENCES wmbs_job(id) ON DELETE CASCADE)"""

        self.constraints["01_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_wmbs ON bl_runjob(wmbs_id) %s""" % tablespaceIndex

        self.constraints["02_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk2 FOREIGN KEY(sched_status)
               REFERENCES bl_status(id) ON DELETE CASCADE)"""

        self.constraints["02_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_status ON bl_runjob(sched_status) %s""" % tablespaceIndex

        self.constraints["03_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk3 FOREIGN KEY(user_id)
               REFERENCES wmbs_users(id) ON DELETE CASCADE)"""

        self.constraints["03_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_users ON bl_runjob(user_id) %s""" % tablespaceIndex

        self.constraints["04_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk4 FOREIGN KEY(location)
               REFERENCES wmbs_location(id) ON DELETE CASCADE)"""

        self.constraints["04_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_location ON bl_runjob(location) %s""" % tablespaceIndex

        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
                "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100" \
                % seqname

        return

    def execute(self, conn=None, transaction=None):
        """
        _execute_

        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create:
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")

        try:
            DBCreator.execute(self, conn, transaction)
            return True
        except Exception as e:
            print("ERROR: %s" % e)
            return False
