#!/usr/bin/env python
"""
_ReqTypes.InsertSQL_

MySQL Support to insert a list of valid Request Types values

"""
__revision__ = "$Id: Insert.py,v 1.1 2010/06/30 23:00:35 rpw Exp $"
__version__ = "$Revision: 1.1 $"

#CHANGED FVL multiple lines

#CHANGED FVL 2 line
from WMCore.Database.DBCreator import DBCreator
import threading


from WMCore.RequestManager.RequestDB.Settings.RequestTypes import TypesList

#CHANGED FVL multiple lines
class Insert(DBCreator):

    sql = "INSERT ALL "

    def __init__(self, logger=None, dbi=None):
        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        
        sql = "INTO reqmgr_request_type ( type_name ) VALUES (\'%s\') "
        for typename in TypesList:
            self.sql += sql % typename
        self.sql += "SELECT * FROM DUAL"
        self.create['create'] = self.sql


