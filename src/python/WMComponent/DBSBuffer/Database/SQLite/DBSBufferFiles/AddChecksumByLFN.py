#!/usr/bin/env python

"""
Oracle implementation of AddChecksumByLFN
"""


__revision__ = "$Id: AddChecksumByLFN.py,v 1.2 2010/08/17 14:50:12 swakef Exp $"
__version__  = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddChecksumByLFN import AddChecksumByLFN as MySQLAddChecksumByLFN

class AddChecksumByLFN(MySQLAddChecksumByLFN):
    """

    Add Checksums using lfn as key

    """
    sql = MySQLAddChecksumByLFN.sql.replace('FROM dual', '')