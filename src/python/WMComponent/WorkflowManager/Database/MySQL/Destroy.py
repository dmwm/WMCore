#/usr/bin/env python
"""
_Destroy_

Implementation of WorkflowManager.Destroy for MySQL 
"""




import threading
from WMCore.Database.DBCreator import DBCreator

class Destroy(DBCreator):
    """
    Class for destroying MySQL specific tables for the WorkflowManager 
    """ 
    def __init__(self):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
        self.delete["02wm_managed_workflow_location"] = \
            "DROP TABLE wm_managed_workflow_location"
        self.delete["01wm_managed_workflow"] = "DROP TABLE wm_managed_workflow"
