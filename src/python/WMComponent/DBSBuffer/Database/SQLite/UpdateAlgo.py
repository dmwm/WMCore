#!/usr/bin/env python
"""
_DBSBuffer.UpdateAlgo_

Add PSetHash to Algo in DBS Buffer

"""
__revision__ = "$Id: UpdateAlgo.py,v 1.2 2009/07/13 19:36:26 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"
__author__ = "mnorman@fnal.gov"

import threading

from WMComponent.DBSBuffer.Database.MySQL.UpdateAlgo import UpdateAlgo as MySQLUpdateAlgo

class UpdateAlgo(MySQLUpdateAlgo):
    """
    _DBSBuffer.UpdateAlgo_

    Add PSetHash to Algo in DBS Buffer

    """
    pass
