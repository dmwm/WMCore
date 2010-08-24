#!/usr/bin/env python
"""
_LoadFileLocations_

MySQL implementation of Jobs.LoadFileLocations
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadFileLocations(DBFormatter):
    """
    _LoadFileLocations_

    Retrieve all locations for a given job
    NOTE: THIS ASSUMES THAT ALL FILES HAVE IDENTICAL LOCATIONS!
    """
    #sql = "SELECT file FROM wmbs_job_assoc WHERE JOB = :jobid"

    sql = """SELECT DISTINCT wl.site_name FROM wmbs_location wl
               INNER JOIN wmbs_file_location wfl ON wfl.location = wl.id
               INNER JOIN wmbs_job_assoc wja ON wja.file = wfl.file
               WHERE wja.job = :jobid
    """

    def formatList(self, results):
        """
        _formatList_

        """
        formattedResults = DBFormatter.formatDict(self, results)

        tmpList = []
        for entry in formattedResults:
            tmpList.append(entry['site_name'])
        
        return tmpList
    
    def execute(self, id, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        result = self.dbi.processData(self.sql, {"jobid": id}, conn = conn,
                                      transaction = transaction)
        
        return self.formatList(result)
