#!/usr/bin/env python
"""

SQLite implementation of AddRunLumi

"""

__revision__ = "$Id: AddRunLumi.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddRunLumi import AddRunLumi as MySQLAddRunLumi

class AddRunLumi(MySQLAddRunLumi):
    """

    SQLite implementation of AddRunLumi

    """

    sql = """INSERT INTO dbsbuffer_file_runlumi_map (file, run, lumi) 
       SELECT id, :run, :lumi from dbsbuffer_file
       where lfn = :lfn"""

    def GetUpdateAddDialect(self):

        return 'SQLite'
