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

        tablespaceIndex = ""
        if params:
            if "tablespace_index" in params:
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        DBCreator.__init__(self, logger, dbi)

        self.requiredTables = ["01bl_status", "02bl_runjob"]

        self.create['01bl_status'] = \
            """CREATE TABLE bl_status
                (
                id            INT  auto_increment,
                name          VARCHAR(255),
                PRIMARY KEY (id),
                UNIQUE (name)
                )
                ENGINE=InnoDB ROW_FORMAT=DYNAMIC;
            """

        self.create['02bl_runjob'] = \
            """CREATE TABLE bl_runjob
               (
               id            INT   auto_increment,
               wmbs_id       INT,
               grid_id       VARCHAR(255),
               bulk_id       VARCHAR(255),
               status        CHAR(1)   DEFAULT '1',
               sched_status  INT NOT NULL,
               retry_count   INT,
               status_time   INT,
               location      INT,
               user_id       INT,
               PRIMARY KEY (id),
               FOREIGN KEY (wmbs_id) REFERENCES wmbs_job(id) ON DELETE CASCADE,
               FOREIGN KEY (sched_status) REFERENCES bl_status(id),
               FOREIGN KEY (user_id) REFERENCES wmbs_users(id) ON DELETE CASCADE,
               FOREIGN KEY (location) REFERENCES wmbs_location(id) ON DELETE CASCADE,
               UNIQUE (retry_count, wmbs_id)
               )
               ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

            """

        self.constraints["01_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_wmbs ON bl_runjob(wmbs_id) %s""" % tablespaceIndex

        self.constraints["02_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_status ON bl_runjob(sched_status) %s""" % tablespaceIndex

        self.constraints["03_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_users ON bl_runjob(user_id) %s""" % tablespaceIndex

        self.constraints["04_idx_bl_runjob"] = \
            """CREATE INDEX idx_bl_runjob_location ON bl_runjob(location) %s""" % tablespaceIndex

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
