#!/usr/bin/env python
"""
_LoadFiles_

MySQL implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.5 2009/01/21 22:00:01 sryu Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFiles(DBFormatter):
    """
    _LoadFiles_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT FILE FROM wmbs_job_assoc WHERE JOB = :jobid"

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file attribute to an integer, and also handle changing the
        column name in Oracle from FILEID to FILE.
        """
        formattedResults = DBFormatter.formatDict(self, results)
        
        dictResults = []
        for formattedResult in formattedResults:
            dictResult = {}
            if "file" in formattedResult.keys():
                dictResult["id"] = int(formattedResult["file"])
                dictResults.append(dictResult)
        
        return dictResults

    def execute(self, id, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        result = self.dbi.processData(self.sql, {"jobid": id}, conn = conn,
                                      transaction = transaction)
        
        return self.formatDict(result)
