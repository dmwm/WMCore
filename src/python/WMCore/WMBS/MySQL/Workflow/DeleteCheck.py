#!/usr/bin/env python
"""
_DeleteFileset_

MySQL implementation of DeleteCheck

"""
__all__ = []
__revision__ = "$Id: DeleteCheck.py,v 1.1 2009/09/25 15:14:03 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class DeleteCheck(DBFormatter):
    sql = """DELETE FROM wmbs_workflow WHERE id = :workflow AND
           NOT EXISTS (SELECT id FROM wmbs_subscription WHERE workflow = :workflow AND id != :subscription)"""
    
    def execute(self, workid = None, subid = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, {'workflow': workid, 'subscription': subid}, 
                         conn = conn, transaction = transaction)
        return True #or raise
