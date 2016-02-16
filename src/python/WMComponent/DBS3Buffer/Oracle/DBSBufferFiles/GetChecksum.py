#!/usr/bin/env python

"""
Oracle implementation of GetChecksum
"""





from WMComponent.DBS3Buffer.MySQL.DBSBufferFiles.GetChecksum import GetChecksum as MySQLGetChecksum

class GetChecksum(MySQLGetChecksum):
    """
    Identical to MySQL Version

    """
