#!/usr/bin/env python
"""
_SetLocationByLFN_

SQLite implementation of DBSBuffer.SetLocationByLFN
"""




from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    """
    Set the location of files using lfn as the key

    """
