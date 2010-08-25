#!/usr/bin/env python
"""

SQLite implementation of AddRunLumi

"""






from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddRunLumi import AddRunLumi as MySQLAddRunLumi

class AddRunLumi(MySQLAddRunLumi):
    """

    SQLite implementation of AddRunLumi

    """

    sql = """INSERT INTO dbsbuffer_file_runlumi_map (filename, run, lumi) 
       SELECT id, :run, :lumi from dbsbuffer_file
       where lfn = :lfn"""

    def GetUpdateAddDialect(self):

        return 'SQLite'
