#!/usr/bin/env python
"""
_Load_

MySQL implementation of Workflow.Load

"""
__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2008/07/03 09:43:55 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class LoadFromID(MySQLBase):
    sql = """select id, spec, name, owner from wmbs_workflow where id = :workflow"""
            
    def getBinds(self, workflow = None):
        return self.dbi.buildbinds(self.dbi.makelist(workflow), 'workflow')
    
    
    def execute(self, workflow = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(workflow), 
                         conn = conn, transaction = transaction)
        return self.format(result)  