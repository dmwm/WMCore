"""
_CreateWMBS_

Base class for creating the WMBS database.
"""

__revision__ = "$Id: CreateWMBSBase.py,v 1.3 2008/09/18 13:26:13 metson Exp $"
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

        self.requiredTables = ["01wmbs_fileset",
                               "02wmbs_file_details",
                               "03wmbs_fileset_files",
                               "04wmbs_file_parent",
                               "05wmbs_file_runlumi_map",
                               "06wmbs_location",
                               "07wmbs_file_location",
                               "08wmbs_workflow",
                               "09wmbs_subscription",
                               "10wmbs_sub_files_acquired",
                               "11wmbs_sub_files_failed",
                               "12wmbs_sub_files_complete",
                               "13wmbs_jobgroup",
                               "14wmbs_job",
                               "15wmbs_job_assoc"]

    def execute(self, conn=None, transaction=None):
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
