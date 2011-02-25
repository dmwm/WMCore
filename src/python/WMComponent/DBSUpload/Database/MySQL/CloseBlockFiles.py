#!/usr/bin/env python
"""
_CloseBlockFiles_

MySQL implementation of CloseBlockFiles
"""




from WMCore.Database.DBFormatter import DBFormatter

class CloseBlockFiles(DBFormatter):
    sql = """UPDATE dbsbuffer_file df SET df.status = 'InDBS'
              WHERE df.block_id = (SELECT id FROM dbsbuffer_block
                                    WHERE blockname = :name)"""

    
    def execute(self, blockname, conn = None, transaction = False):
        """
        _execute_

        List as InDBS all files for a given blockname
        """
        self.dbi.processData(self.sql, {'name': blockname}, 
                             conn = conn, transaction = transaction)
        return 
