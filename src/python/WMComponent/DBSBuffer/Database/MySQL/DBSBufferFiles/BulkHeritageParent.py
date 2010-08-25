#!/usr/bin/env python
"""
_HeritageLFNParent_

MySQL implementation of DBSBufferFiles.HeritageLFNParent
"""




from WMCore.Database.DBFormatter import DBFormatter

class BulkHeritageParent(DBFormatter):
    sql = """INSERT INTO dbsbuffer_file_parent (child, parent)
               SELECT dfc.id, dfp.id FROM dbsbuffer_file dfc
               INNER JOIN dbsbuffer_file dfp
               WHERE dfc.lfn = :child
               AND dfp.lfn = :parent             
    """

    
    def execute(self, binds, conn = None, transaction = False):
        """
        _execute_

        This requires you to send in a list of binds in the form:
        {child, parent}
        """
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        

        return
    
