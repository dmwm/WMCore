#!/usr/bin/env python
"""
_Load_

MySQL implementation of Workflow.Load

"""
__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.2 2008/11/20 21:52:33 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """select id, spec, name, owner from wmbs_workflow where id = :workflow"""
            
    def getBinds(self, workflow = None):
        return self.dbi.buildbinds(self.dbi.makelist(workflow), 'workflow')
    
    
    def execute(self, workflow = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(workflow), 
                         conn = conn, transaction = transaction)
        return self.format(result)  