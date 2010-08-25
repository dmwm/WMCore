#!/usr/bin/env python

"""
Oracle implementation of AddChecksum
"""


__revision__ = "$Id: AddChecksum.py,v 1.1 2009/12/02 20:03:48 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddChecksum import AddChecksum as MySQLAddChecksum

class AddChecksum(MySQLAddChecksum):
    """
    Identical to MySQL Version

    """
