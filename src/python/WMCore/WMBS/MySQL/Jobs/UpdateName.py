
#!/usr/bin/env python
"""
_UpdateName_
MySQL implementation of Jobs.UpdateName
"""
__all__ = []
__revision__ = "$Id: UpdateName.py,v 1.1 2008/10/01 21:54:39 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateName(DBFormatter):
    sql = "update wmbs_job set name = :name where id = :id"
    
            
    def execute(self, id=0, name=None, conn = None, transaction = False):
        binds = self.getBinds(id=id, name=name)
        self.logger.debug('Job.UpdateName sql: %s' % self.sql)
        self.logger.debug('Job.UpdateName binds: %s' % binds)
        
        return self.format(self.dbi.processData(self.sql, binds))