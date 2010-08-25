#!/usr/bin/env python
"""

SQLite implementation of SetLocation

"""

__revision__ = "$Id: SetLocation.py,v 1.1 2009/05/14 16:21:51 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"


from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetLocation import SetLocation as MySQLSetLocation

class SetLocation(MySQLSetLocation):
    """

    SQLite implementation of SetLocation

    """

    sql = """INSERT INTO dbsbuffer_file_location (file, location) 
    SELECT dbsbuffer_file.id, dbsbuffer_location.id from dbsbuffer_file, dbsbuffer_location 
    WHERE dbsbuffer_file.lfn = :lfn
    AND dbsbuffer_location.se_name = :location"""

    def GetUpdateSetLocationDialect(self):

        return 'SQLite'
