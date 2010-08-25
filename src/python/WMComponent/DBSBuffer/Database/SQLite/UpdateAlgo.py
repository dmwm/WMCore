#!/usr/bin/env python
"""
_DBSBuffer.UpdateAlgo_

Add PSetHash to Algo in DBS Buffer

"""
__revision__ = "$Id: UpdateAlgo.py,v 1.1 2009/05/14 16:18:57 mnorman Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMComponent.DBSBuffer.Database.MySQL.UpdateAlgo import UpdateAlgo as MySQLUpdateAlgo

class UpdateAlgo(MySQLUpdateAlgo):
    """
    _DBSBuffer.UpdateAlgo_

    Add PSetHash to Algo in DBS Buffer

    """

    def GetUpdateAlgoDialect(self):

        return 'SQLite'
