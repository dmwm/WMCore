#!/usr/bin/env python

"""
Oracle implementation of GetChecksum
"""





from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetChecksum import GetChecksum as MySQLGetChecksum

class GetChecksum(MySQLGetChecksum):
    """
    Identical to MySQL Version

    """
