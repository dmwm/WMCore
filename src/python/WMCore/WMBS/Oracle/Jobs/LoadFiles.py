#!/usr/bin/env python
"""
_LoadFiles_

Oracle implementation of Jobs.LoadFiles
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.MySQL.Jobs.LoadFiles import LoadFiles as LoadFilesMySQL

class LoadFiles(LoadFilesMySQL):
    """
    _LoadFiles_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql = "SELECT fileid FROM wmbs_job_assoc WHERE JOB = :jobid"

    def formatDict(self, results):
        """
        _formatDict_

        Cast the file attribute to an integer,.
        """
        formattedResults = DBFormatter.formatDict(self, results)

        dictResults = []
        for formattedResult in formattedResults:
            dictResult = {}
            if "fileid" in formattedResult:
                dictResult["id"] = int(formattedResult["fileid"])
                dictResults.append(dictResult)

        return dictResults
