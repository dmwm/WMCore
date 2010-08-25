#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Workflow.Exists

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """select id from wmbs_workflow
            where spec = :spec and owner = :owner and name = :name and task = :task"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return int(result[0][0])
    
    def getBinds(self, spec=None, owner=None, name = None, task = None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                    self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                     self.dbi.buildbinds(self.dbi.makelist(name), 'name',
                                      self.dbi.buildbinds(self.dbi.makelist(task), 'task'))))
        
    def execute(self, spec=None, owner=None, name = None, task = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(spec, owner, name, task), 
                         conn = conn, transaction = transaction)
        return self.format(result)
