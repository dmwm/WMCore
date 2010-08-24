#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Jobs.LoadFromName.
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.5 2009/01/16 22:38:01 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromName(DBFormatter):
    """
    _LoadFromName_

    Retrieve meta data for a job given it's name.  This includes the name,
    job group and last update time. 
    """
    sql = """SELECT ID, NAME, JOBGROUP, LAST_UPDATE FROM wmbs_job WHERE
             NAME = :name"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["jobgroup"] = int(formattedResult["jobgroup"])
        formattedResult["last_update"] = int(formattedResult["last_update"])
        return formattedResult
    
    def execute(self, name, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"name": name}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
