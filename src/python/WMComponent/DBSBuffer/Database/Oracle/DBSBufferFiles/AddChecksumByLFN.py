#!/usr/bin/env python

"""
Oracle implementation of AddChecksumByLFN
"""





from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """

    Add Checksums using lfn as key

    """

