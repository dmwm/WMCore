#/usr/bin/env python2.4
"""
_Destroy_

"""

__revision__ = "$Id: Destroy.py,v 1.1 2008/11/20 17:23:21 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

import threading

from WMCore.Database.DBCreator import DBCreator

from WMCore.WMException import WMException
from WMCore.WMExceptions import WMEXCEPTION

class Destroy(DBCreator):    
    def __init__(self):
        """
        _init_

        Call the DBCreator constructor and create the list of required tables.
        """
        myThread = threading.currentThread()
        DBCreator.__init__(self, myThread.logger, myThread.dbi)
                
        self.create["20wmbs_fileset"] = "DROP TABLE wmbs_fileset"
        self.create["19wmbs_file_details"] = "DROP TABLE wmbs_file_details"
        self.create["18wmbs_fileset_files"] = "DROP TABLE wmbs_fileset_files"
        self.create["17wmbs_file_parent"] = "DROP TABLE wmbs_file_parent"
        self.create["16wmbs_file_runlumi_map"] = "DROP TABLE wmbs_file_runlumi_map"
        self.create["15wmbs_location"] = "DROP TABLE wmbs_location"
        self.create["14wmbs_file_location"] = "DROP TABLE wmbs_file_location"
        self.create["13wmbs_workflow"] = "DROP TABLE wmbs_workflow"
        self.create["12wmbs_subscription"] = "DROP TABLE wmbs_subscription"
        self.create["11wmbs_subscription_location"] = "DROP TABLE wmbs_subscription_location"
        self.create["10wmbs_sub_files_acquired"] = "DROP TABLE wmbs_sub_files_acquired"
        self.create["09wmbs_sub_files_failed"] = "DROP TABLE wmbs_sub_files_failed"
        self.create["08wmbs_sub_files_complete"] = "DROP TABLE wmbs_sub_files_complete"
        self.create["07wmbs_jobgroup"] = "DROP TABLE wmbs_jobgroup"
        self.create["06wmbs_job"] = "DROP TABLE wmbs_job"
        self.create["05wmbs_job_assoc"] = "DROP TABLE wmbs_job_assoc"
        self.create["04wmbs_group_job_acquired"] = "DROP TABLE wmbs_group_job_acquired"
        self.create["03wmbs_group_job_failed"] = "DROP TABLE wmbs_group_job_failed"
        self.create["02wmbs_group_job_complete"] = "DROP TABLE wmbs_group_job_complete"
        self.create["01wmbs_job_mask"] = "DROP TABLE wmbs_job_mask"
