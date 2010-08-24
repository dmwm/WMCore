#!/usr/bin/env python
"""
_NewWorkflow_

MySQL implementation of NewWorkflow

"""
__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    Create a workflow ready for subscriptions
    """
    sql = """insert into wmbs_workflow (spec, owner, name, task)
                values (:spec, :owner, :name, :task)"""
    
    def getBinds(self, spec=None, owner=None, name=None, task=None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                   self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                   self.dbi.buildbinds(self.dbi.makelist(name), 'name',
                                   self.dbi.buildbinds(self.dbi.makelist(task), 'task'))))
        
    def execute(self, spec=None, owner=None, name = None, task=None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(spec, owner, name, task), 
                         conn = conn, transaction = transaction)
        return True #or raise