"""
_Create_

Base class for creating the BossAir database
"""

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION
from WMCore.JobStateMachine.ChangeState import Transitions

class Create(DBCreator):
    """
    This should create the BossLite schema; since they don't do it.

    """

    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()

        if logger == None:
            logger = myThread.logger
        if dbi == None:
            dbi = myThread.dbi

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
            ENGINE = InnoDB DEFAULT CHARSET=latin1;
        """

        
        self.create['02bl_runjob'] = \
        """CREATE TABLE bl_runjob
           (
           id            INT   auto_increment,
           wmbs_id       INT,
           grid_id       VARCHAR(255),
           bulk_id       VARCHAR(255),
           status        CHAR(1)   DEFAULT '1',
           sched_status  INT,
           retry_count   INT,
           status_time   INT,
           location      VARCHAR(255),
           user_id       INT,
           PRIMARY KEY (id),
           FOREIGN KEY (wmbs_id) REFERENCES wmbs_job(id) ON DELETE CASCADE,
           FOREIGN KEY (sched_status) REFERENCES bl_status(id),
           FOREIGN KEY (user_id) REFERENCES wmbs_users(id) ON DELETE CASCADE,
           UNIQUE (retry_count, wmbs_id)
           )
           ENGINE = InnoDB DEFAULT CHARSET=latin1;
           
        """


        return


    def execute(self, conn = None, transaction = None):
        """
        _execute_

        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create.keys():
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")

        try:
            DBCreator.execute(self, conn, transaction)
            return True
        except Exception, e:
            print "ERROR: %s" % e
            return False
