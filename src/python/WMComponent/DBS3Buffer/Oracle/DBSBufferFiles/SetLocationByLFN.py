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
    sql = """INSERT INTO dbsbuffer_file_location (filename, location)
               SELECT (SELECT id FROM dbsbuffer_file WHERE lfn= :lfn) AS filename,
                      (SELECT id FROM dbsbuffer_location WHERE pnn= :pnn) AS location
               FROM DUAL WHERE NOT EXISTS
                 (SELECT filename FROM dbsbuffer_file_location
                   WHERE filename= (SELECT id FROM dbsbuffer_file WHERE lfn= :lfn) AND
                   location= (SELECT id FROM dbsbuffer_location WHERE pnn= :pnn))
               """
