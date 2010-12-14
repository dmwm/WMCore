#!/usr/bin/env python
"""
_SetLocationByLFN_

Oracle implementation of DBSBuffer.SetLocationByLFN
"""




from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN

class SetLocationByLFN(MySQLSetLocationByLFN):
    """
    Set the location of files using lfn as the key

    """
    sql = """INSERT INTO dbsbuffer_file_location (filename, location)
               SELECT df.id, dl.id
               FROM dbsbuffer_file df, dbsbuffer_location dl
               WHERE df.lfn = :lfn
               AND dl.se_name = :sename
               """
