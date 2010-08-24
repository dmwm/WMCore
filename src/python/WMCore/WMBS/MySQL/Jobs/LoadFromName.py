#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Jobs.LoadFromName.
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.4 2009/01/14 16:44:06 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromName(DBFormatter):
    """
    _LoadFromName_

    Retrieve meta data for a job given it's name.  This includes the name,
    job group and last update time. 
    """
    sql = """SELECT ID, NAME, JOBGROUP, LAST_UPDATE FROM wmbs_job WHERE
             NAME = :name"""
    
    def execute(self, name, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"name": name}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)[0]
