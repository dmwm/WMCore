#!/usr/bin/env python
"""
_LoadFiles_

Oracle implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.3 2009/01/13 17:39:19 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

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

        Change the name of the fileid key to be just file for compatibility
        with the MySQL DAO object.
        """
        formattedResults = LoadFilesMySQL.formatDict(self, results)

        for formattedResult in formattedResults:
            formattedResult["file"] = formattedResult["fileid"]
            del formattedResult["fileid"]

        return formattedResults
