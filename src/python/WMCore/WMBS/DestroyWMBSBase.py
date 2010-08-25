#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: DestroyWMBSBase.py,v 1.6 2009/10/12 21:11:18 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class DestroyWMBSBase(DBCreator):    
    def __init__(self, logger = None, dbi = None):
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

        self.delete["19wmbs_fileset"] = "DROP TABLE wmbs_fileset"
        self.delete["18wmbs_file_details"] = "DROP TABLE wmbs_file_details"
        self.delete["17wmbs_fileset_files"] = "DROP TABLE wmbs_fileset_files"
        self.delete["16wmbs_file_parent"] = "DROP TABLE wmbs_file_parent"
        self.delete["15wmbs_file_runlumi_map"] = "DROP TABLE wmbs_file_runlumi_map"
        self.delete["14wmbs_location"] = "DROP TABLE wmbs_location"
        self.delete["13wmbs_file_location"] = "DROP TABLE wmbs_file_location"
        self.delete["12wmbs_workflow"] = "DROP TABLE wmbs_workflow"
        self.delete["11wmbs_workflow_output"] = "DROP TABLE wmbs_workflow_output"
        self.delete["11wmbs_sub_types"] = "DROP TABLE wmbs_sub_types"
        self.delete["10wmbs_subscription"] = "DROP TABLE wmbs_subscription"
        self.delete["09wmbs_subscription_location"] = "DROP TABLE wmbs_subscription_location"
        self.delete["08wmbs_sub_files_acquired"] = "DROP TABLE wmbs_sub_files_acquired"
        self.delete["07wmbs_sub_files_failed"] = "DROP TABLE wmbs_sub_files_failed"
        self.delete["06wmbs_sub_files_complete"] = "DROP TABLE wmbs_sub_files_complete"
        self.delete["05wmbs_jobgroup"] = "DROP TABLE wmbs_jobgroup"
        self.delete["04wmbs_job_state"] = "DROP TABLE wmbs_job_state"
        self.delete["03wmbs_job"] = "DROP TABLE wmbs_job"
        self.delete["02wmbs_job_assoc"] = "DROP TABLE wmbs_job_assoc"
        self.delete["01wmbs_job_mask"] = "DROP TABLE wmbs_job_mask"
