"""
MySQL implementation of Files.AddToFileset
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class AddToFileset(DBFormatter):
    sql = """
            insert into wmbs_fileset_files (file, fileset, insert_time) 
                select wmbs_file_details.id, wmbs_fileset.id, :insert_time 
                from wmbs_file_details, wmbs_fileset 
                where wmbs_file_details.lfn = :lfn
                and wmbs_fileset.name = :fileset"""
        
    def execute(self, file = None, fileset = None, conn = None,
                transaction = False):
        binds = []
        timestamp = int(time.time())
        for fileID in file:
            binds.append({"file_id": fileID, "fileset": fileset,
                          "insert_time": timestamp})        
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
