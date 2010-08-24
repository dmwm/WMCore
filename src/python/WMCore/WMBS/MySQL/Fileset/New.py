#!/usr/bin/env python
"""
_New_
 
MySQL implementation of Fileset.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.4 2009/03/03 14:54:52 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_fileset (name, last_update, open)
               VALUES (:NAME, :LAST_UPDATE, :OPEN)"""
    
    def getBinds(self, name = None, open = False):
        bindVars = {}
        bindVars["NAME"] = name
        bindVars["OPEN"] = int(open)
        bindVars["LAST_UPDATE"] = int(time.time())
        return bindVars
    
    def execute(self, name = None, open = False, conn = None,
                transaction = False):
        self.dbi.processData(self.sql, self.getBinds(name, open), 
                         conn = conn, transaction = transaction)
        return
