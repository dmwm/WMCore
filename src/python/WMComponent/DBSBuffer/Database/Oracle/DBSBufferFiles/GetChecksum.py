#!/usr/bin/env python

"""
Oracle implementation of GetChecksum
"""


__revision__ = "$Id: GetChecksum.py,v 1.1 2009/12/02 20:04:59 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetChecksum import GetChecksum as MySQLGetChecksum

class GetChecksum(MySQLGetChecksum):
    """
    Identical to MySQL Version

    """
