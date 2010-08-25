#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: DestroyWMBSBase.py,v 1.4 2009/05/09 11:45:25 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

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
                
        self.create["19wmbs_fileset"] = "DROP TABLE wmbs_fileset"
        self.create["18wmbs_file_details"] = "DROP TABLE wmbs_file_details"
        self.create["17wmbs_fileset_files"] = "DROP TABLE wmbs_fileset_files"
        self.create["16wmbs_file_parent"] = "DROP TABLE wmbs_file_parent"
        self.create["15wmbs_file_runlumi_map"] = "DROP TABLE wmbs_file_runlumi_map"
        self.create["14wmbs_location"] = "DROP TABLE wmbs_location"
        self.create["13wmbs_file_location"] = "DROP TABLE wmbs_file_location"
        self.create["12wmbs_workflow"] = "DROP TABLE wmbs_workflow"
        self.create["11wmbs_workflow_output"] = "DROP TABLE wmbs_workflow_output"
        self.create["10wmbs_subscription"] = "DROP TABLE wmbs_subscription"
        self.create["09wmbs_subscription_location"] = "DROP TABLE wmbs_subscription_location"
        self.create["08wmbs_sub_files_acquired"] = "DROP TABLE wmbs_sub_files_acquired"
        self.create["07wmbs_sub_files_failed"] = "DROP TABLE wmbs_sub_files_failed"
        self.create["06wmbs_sub_files_complete"] = "DROP TABLE wmbs_sub_files_complete"
        self.create["05wmbs_jobgroup"] = "DROP TABLE wmbs_jobgroup"
        self.create["04wmbs_job_state"] = "DROP TABLE wmbs_job_state"
        self.create["03wmbs_job"] = "DROP TABLE wmbs_job"
        self.create["02wmbs_job_assoc"] = "DROP TABLE wmbs_job_assoc"
        self.create["01wmbs_job_mask"] = "DROP TABLE wmbs_job_mask"
