#!/usr/bin/env python
"""
_GetCache_

MySQL implementation of Jobs.GetState
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

import logging

class SetCache(DBFormatter):
    """
    _GetState_

    Given a job ID, get the state of a current job.
    """
    sql = """UPDATE wmbs_job
             SET cache_dir = :cacheDir
             WHERE id = :jobid"""

    def execute(self, id = None, cacheDir = None, conn = None, transaction = False, jobDictList = None):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        Sending a value for jobDictList triggers bulk mode
        """
        #jobDictList != None triggers bulk mode
        #jobDictList should be a list of dictionaries of the form {'id': id,'cacheDir': cacheDir}

        if jobDictList:
            binds = jobDictList
        elif id and cacheDir:
            binds = {"jobid": id, "cacheDir": cacheDir}
        else:
            logging.error("Jobs.SetCache not sent values to set!")
            return

        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)

        return
