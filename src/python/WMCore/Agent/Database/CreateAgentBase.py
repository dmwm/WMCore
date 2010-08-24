"""
_CreateWMBS_

Base class for creating the WMBS database.
"""




import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class CreateAgentBase(DBCreator):
    
    requiredTables = ["01wm_components", "02wm_workers"]
    
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

        tablespaceTable = ""
        tablespaceIndex = ""
        if params:
            if params.has_key("tablespace_table"):
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if params.has_key("tablespace_index"):
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

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
             last_error    INTEGER,
             error_message VARCHAR(1000),
             UNIQUE (component_id, name))"""
        
        self.constraints["FK_wm_component_worker"] = \
              """ALTER TABLE wm_workers ADD CONSTRAINT FK_wm_component_worker
                 FOREIGN KEY(component_id) REFERENCES wm_components(id)"""
  
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
