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
    sequence_tables = [ 'bl_runjob', 'bl_status' ]

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

        tablespaceTable = ""
        tablespaceIndex = ""

        self.create          = {}
        self.constraints     = {}
        self.indexes         = {}


        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]



        self.requiredTables = ["01bl_status", "02bl_runjob"]


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
           sched_status  INTEGER,
           retry_count   INTEGER,
           status_time   INTEGER,
           location      VARCHAR(255),
           user          INT
           ) %s  """ % (tablespaceTable)


        self.indexes["02_pk_bl_runjob"] = \
          """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_pk PRIMARY KEY (id) %s)""" % tablespaceIndex


        self.indexes["01_pk_bl_status"] = \
          """ALTER TABLE bl_status ADD
               (CONSTRAINT bl_status_pk PRIMARY KEY (id) %s)""" % tablespaceIndex


        self.constraints["01_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk1 FOREIGN KEY(wmbs_id)
               REFERENCES wmbs_job(id) ON DELETE CASCADE)"""


        self.constraints["02_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk2 FOREIGN KEY(sched_status)
               REFERENCES bl_status(id) ON DELETE CASCADE)"""


        self.constraints["02_fk_bl_runjob"] = \
            """ALTER TABLE bl_runjob ADD
               (CONSTRAINT bl_runjob_fk3 FOREIGN KEY(user)
               REFERENCES wmbs_users(id) ON DELETE CASCADE)"""

        

        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
              "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100" \
              % seqname


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
