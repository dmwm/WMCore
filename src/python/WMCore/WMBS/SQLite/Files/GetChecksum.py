#!/usr/bin/env python

"""
SQLite implementation of GetChecksum
"""





from WMCore.WMBS.MySQL.Files.GetChecksum import GetChecksum as MySQLGetChecksum

class GetChecksum(MySQLGetChecksum):
    """
    Identical to MySQL

    """
