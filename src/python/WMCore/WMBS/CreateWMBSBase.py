"""
_CreateWMBS_

Base class for creating the WMBS database.
"""

__revision__ = "$Id: CreateWMBSBase.py,v 1.1 2008/09/15 09:04:53 sfoulkes Exp $"
__version__ = "$Reivison: $"

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class CreateWMBSBase(DBCreator):    
    def __init__(self, logger, dbinterface):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        DBCreator.__init__(self, logger, dbinterface)

        self.requiredTables = ["wmbs_fileset", "wmbs_fileset_files",
                               "wmbs_file_parent", "wmbs_file_details",
                               "wmbs_file_runlumi_map", "wmbs_location",
                               "wmbs_file_location", "wmbs_workflow",
                               "wmbs_subscription", "wmbs_sub_files_acquired",
                               "wmbs_sub_files_failed", "wmbs_jobgroup",
                               "wmbs_job", "wmbs_job_assoc",
                               "wmbs_sub_files_complete"]

    def execute(self, conn, transaction):
        """
        _execute_
        
        Check to make sure that all required tables have been defined.  If
        everything is in place have the DBCreator make everything.
        """
        for requiredTable in self.requiredTables:
            if requiredTable not in self.create.keys():
                raise WMException("The table '%s' is not defined." % \
                                  requiredTable, "WMCORE-2")
                                  
        DBCreator.execute(self, conn, transaction)
        return
