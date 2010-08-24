#!/usr/bin/env python
"""
_GetByID_

SQLite implementation of DBSBufferFiles.GetByID
"""




import logging

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles import LoadBulkFilesByID as MySQLLoadBulkFilesByID

class LoadBulkFilesByID(MySQLLoadBulkFilesByID):
    """
    Same as MySQL

    """
