#!/usr/bin/env python
"""
_SetLastUpdate_

MySQL implementation of Fileset.SetLastUpdate
"""

__all__ = []
__revision__ = "$Id: SetLastUpdate.py,v 1.1 2010/05/03 12:21:37 riahi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
import time

class SetLastUpdate(DBFormatter):
    sql = "UPDATE wmbs_fileset SET last_update = :p_1 WHERE name = :p_2"

    def execute(self, fileset = None, timeUpdate = int(time.time()) , \
                conn = None, transaction = False):
        bindVars = {"p_1": timeUpdate, "p_2": fileset}
        self.dbi.processData(self.sql, bindVars, conn = conn,
                            transaction = transaction)
        return
