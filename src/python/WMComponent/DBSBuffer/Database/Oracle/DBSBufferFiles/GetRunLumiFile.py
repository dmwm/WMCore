#!/usr/bin/env python
"""
Oracle implementation of GetRunLumiFile
"""

__revision__ = "$Id: GetRunLumiFile.py,v 1.2 2009/05/18 20:14:03 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetRunLumiFile import GetRunLumiFile as MySQLGetRunLumiFile


class GetRunLumiFile(MySQLGetRunLumiFile):
    """
    Oracle implementation of GetRunLumiFile
    """



