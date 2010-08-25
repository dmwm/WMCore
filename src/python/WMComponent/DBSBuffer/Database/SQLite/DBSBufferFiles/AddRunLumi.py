#!/usr/bin/env python
"""

SQLite implementation of AddRunLumi

"""

__revision__ = "$Id: AddRunLumi.py,v 1.2 2009/05/18 20:16:10 mnorman Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"


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
