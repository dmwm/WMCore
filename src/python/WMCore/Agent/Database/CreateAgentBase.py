"""
_CreateWMBS_

Base class for creating the WMBS database.
"""
from __future__ import print_function

import threading

from WMCore.Database.DBCreator import DBCreator
from WMCore.WMException import WMException


class CreateAgentBase(DBCreator):
    requiredTables = ["01wm_components", "02wm_workers"]

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

        self.create["01wm_components"] = \
            """CREATE TABLE wm_components (
               id               INTEGER      PRIMARY KEY AUTO_INCREMENT,
               name             VARCHAR(255) NOT NULL,
               pid              INTEGER      NOT NULL,
               update_threshold INTEGER      NOT NULL,
               UNIQUE (name))"""

        self.create["02wm_workers"] = \
            """CREATE TABLE wm_workers (
               component_id  INTEGER NOT NULL,
               name          VARCHAR(255) NOT NULL,
               last_updated  INTEGER      NOT NULL,
               state         VARCHAR(255),
               pid           INTEGER,
               poll_interval INTEGER      NOT NULL,
               last_error    INTEGER,
               cycle_time    FLOAT DEFAULT 0 NOT NULL,
               outcome       VARCHAR(1000),
               error_message VARCHAR(1000),
               UNIQUE (name))"""

        self.constraints["FK_wm_component_worker"] = \
            """ALTER TABLE wm_workers ADD CONSTRAINT FK_wm_component_worker
               FOREIGN KEY(component_id) REFERENCES wm_components(id)"""

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
