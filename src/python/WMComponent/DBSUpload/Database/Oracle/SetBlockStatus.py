#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

                                                                                                                                                                                                     Create new block in dbsbuffer_block
                                                                                                                                                                                                     Update file to reflect block information
                                                                                                                                                                                                     """
__revision__ = "$Id: SetBlockStatus.py,v 1.1 2009/08/12 22:15:10 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading
import exceptions

from WMComponent.DBSUpload.Database.MySQL.SetBlockStatus import SetBlockStatus as MySQLSetBlockStatus


class SetBlockStatus(MySQLSetBlockStatus):
    """
    Identical to MySQL version for now

    """
