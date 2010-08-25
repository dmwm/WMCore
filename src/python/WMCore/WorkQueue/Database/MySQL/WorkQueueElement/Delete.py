"""
_UpdateStaus_

MySQL implementation of WorkQueueElement.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2010/06/02 14:42:10 swakef Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter

class Delete(DBFormatter):
    sql = """DELETE FROM wq_element WHERE id = :id"""

    def execute(self, ids,
                conn = None, transaction = False):

        if len(ids) == 0:
            # if ids are not passed just declare success
            return True

        binds = [{'id' : x} for x in ids]
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return True
