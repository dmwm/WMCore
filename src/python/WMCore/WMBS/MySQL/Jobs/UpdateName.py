
#!/usr/bin/env python
"""
_UpdateName_
MySQL implementation of Jobs.UpdateName
"""
__all__ = []
__revision__ = "$Id: UpdateName.py,v 1.2 2009/01/12 19:26:03 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateName(DBFormatter):
    sql = "update wmbs_job set name = :name where id = :id"
            
    def execute(self, id=0, name=None, conn = None, transaction = False):
        binds = self.getBinds(id=id, name=name)
        return self.format(self.dbi.processData(self.sql, binds, conn = conn,
                                                transaction = transaction))
