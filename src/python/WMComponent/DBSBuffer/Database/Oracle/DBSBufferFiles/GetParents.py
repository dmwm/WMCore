#!/usr/bin/env python
"""
Oracle implementation of File.GetParents

Return a list of lfn's which are parents for a file.
"""





from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetParents import GetParents as MySQLGetParents

class GetParents(MySQLGetParents):
    """
    Oracle implementation of File.GetParents
    
    Return a list of lfn's which are parents for a file.
    """
