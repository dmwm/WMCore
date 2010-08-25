#!/usr/bin/env python
"""
_DBSBuffer.NewAlgo_

Add a new algorithm to DBS Buffer: Oracle version
"""

__revision__ = "$Id: NewAlgo.py,v 1.2 2009/07/14 19:16:17 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.NewAlgo import NewAlgo as MySQLNewAlgo

class NewAlgo(MySQLNewAlgo):
    """
    _DBSBuffer.NewAlgo_

    Add a new algorithm to DBS Buffer: Oracle version
    """
    pass
