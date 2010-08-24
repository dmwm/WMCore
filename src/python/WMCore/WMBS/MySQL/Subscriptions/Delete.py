#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Subscriptions.Delete

"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/20 16:55:33 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Delete(MySQLBase):
    """

    """
    sql = """delete from wmbs_subscription where id = :id"""
    
    def getBinds(self, id=None):
        return self.dbi.buildbinds(self.dbi.makelist(id), 'id')
        
    def execute(self, id = -1, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(id), 
                     conn = conn, transaction = transaction)
        return True #or raise
