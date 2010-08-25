#!/usr/bin/env/python
"""
_GetParentStatus_

MySQL implementation of DBSBufferFile.GetParentStatus
"""

__revision__ = "$Id: GetParentStatus.py,v 1.1 2010/01/13 19:54:43 sfoulkes Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetParentStatus(DBFormatter):
    sql = """SELECT status FROM dbsbuffer_file
               INNER JOIN dbsbuffer_file_parent ON
                 dbsbuffer_file.id = dbsbuffer_file_parent.parent
             WHERE dbsbuffer_file_parent.child =
               (SELECT id FROM dbsbuffer_file WHERE lfn  = :lfn)"""
    
    def format(self, results):
        """
        _format_

        Format the query results into a list of LFNs.
        """
        results = DBFormatter.format(self, results)

        status = []
        for result in results:
            status.append(result[0])
        return status
        
    def execute(self, lfn, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"lfn": lfn}, conn = conn,
                                      transaction = transaction)
        return self.format(result)
