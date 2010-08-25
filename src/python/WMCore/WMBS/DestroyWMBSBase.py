#/usr/bin/env python2.4
"""
_Destroy_

"""




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

        self.delete["22wmbs_fileset"] = "DROP TABLE wmbs_fileset"
        self.delete["21wmbs_file_details"] = "DROP TABLE wmbs_file_details"
        self.delete["20wmbs_fileset_files"] = "DROP TABLE wmbs_fileset_files"
        self.delete["19wmbs_file_parent"] = "DROP TABLE wmbs_file_parent"
        self.delete["18wmbs_file_runlumi_map"] = "DROP TABLE wmbs_file_runlumi_map"
        self.delete["17wmbs_location"] = "DROP TABLE wmbs_location"
        self.delete["16wmbs_file_location"] = "DROP TABLE wmbs_file_location"
        self.delete["15wmbs_workflow"] = "DROP TABLE wmbs_workflow"
        self.delete["14wmbs_workflow_output"] = "DROP TABLE wmbs_workflow_output"
        self.delete["13wmbs_sub_types"] = "DROP TABLE wmbs_sub_types"
        self.delete["12wmbs_subscription"] = "DROP TABLE wmbs_subscription"
        self.delete["10wmbs_sub_files_acquired"] = "DROP TABLE wmbs_sub_files_acquired"
        self.delete["10wmbs_sub_files_available"] = "DROP TABLE wmbs_sub_files_available"
        self.delete["09wmbs_sub_files_failed"] = "DROP TABLE wmbs_sub_files_failed"
        self.delete["08wmbs_sub_files_complete"] = "DROP TABLE wmbs_sub_files_complete"
        self.delete["07wmbs_jobgroup"] = "DROP TABLE wmbs_jobgroup"
        self.delete["06wmbs_job_state"] = "DROP TABLE wmbs_job_state"
        self.delete["05wmbs_job"] = "DROP TABLE wmbs_job"
        self.delete["04wmbs_job_assoc"] = "DROP TABLE wmbs_job_assoc"
        self.delete["03wmbs_job_mask"] = "DROP TABLE wmbs_job_mask"
        self.delete["02wmbs_job_mask"] = "DROP TABLE wmbs_checksum_type"
        self.delete["01wmbs_job_mask"] = "DROP TABLE wmbs_file_checksums"
