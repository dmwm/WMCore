#!/usr/bin/env python
"""
_SetLocationByLFN_

Oracle implementation of DBSBuffer.SetLocationByLFN
"""

from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.SetLocationByLFN import SetLocationByLFN as MySQLSetLocationByLFN


class SetLocationByLFN(MySQLSetLocationByLFN):
    """
    Set the location of files using lfn as the key

    """
    sql = """INSERT /*+ IGNORE_ROW_ON_DUPKEY_INDEX (dbsbuffer_file_location (filename, location)) */
               INTO dbsbuffer_file_location (filename, location)
             SELECT df.id, dl.id FROM dbsbuffer_file df, dbsbuffer_location dl
             WHERE df.lfn = :lfn AND dl.pnn = :pnn
          """
