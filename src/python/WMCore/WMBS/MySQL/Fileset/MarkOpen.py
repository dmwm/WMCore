#!/usr/bin/env python
"""
_MarkOpen_

MySQL implementation of Fileset.MarkOpen
"""

__all__ = []
__revision__ = "$Id: MarkOpen.py,v 1.2 2009/04/27 13:43:41 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class MarkOpen(DBFormatter):
    sql = "UPDATE wmbs_fileset SET open = :p_1 WHERE name = :p_2"
    
    def execute(self, fileset = None, isOpen = True, conn = None,
                transaction = False):
        bindVars = {"p_1": int(isOpen), "p_2": fileset} 
        self.dbi.processData(self.sql, bindVars, conn = conn,
                             transaction = transaction)
        return
