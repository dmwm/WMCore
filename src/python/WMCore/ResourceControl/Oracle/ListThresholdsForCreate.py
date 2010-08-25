#!/usr/bin/env python
"""
_ListThresholdsForCreate_

Oracle implementation of ResourceControl.ListThresholdsForCreate
"""

__revision__ = "$Id: ListThresholdsForCreate.py,v 1.1 2010/02/09 17:57:03 sfoulkes Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.ResourceControl.MySQL.ListThresholdsForCreate \
     import ListThresholdsForCreate as MySQLListThresholdsForCreate

class ListThresholdsForCreate(MySQLListThresholdsForCreate):
    pass
