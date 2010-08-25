#!/usr/bin/env python
"""
_Group.New_

Action to insert a new group into ReqMgr

"""
__revision__ = "$Id: New.py,v 1.1 2010/07/01 19:06:19 rpw Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):



    def execute(self, groupName, groupPriority,
                conn = None, transaction = False):

        self.sql = "INSERT INTO reqmgr_group ( "
        self.sql += "group_name,group_base_priority  )"
        self.sql += " VALUES (\'%s\', %s)" % (groupName, groupPriority)

        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = transaction)
        return self.format(result)

