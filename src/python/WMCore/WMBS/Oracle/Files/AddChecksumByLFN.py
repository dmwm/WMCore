#!/usr/bin/env python

"""
Oracle implementation of AddChecksumByLFN
"""





from WMCore.WMBS.MySQL.Files.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """
    Add Checksums using LFN as file identifier

    """
