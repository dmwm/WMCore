#!/usr/bin/env python
"""
_ListThresholdsForCreate_

SQLite implementation of ResourceControl.ListThresholdsForCreate
"""




from WMCore.ResourceControl.MySQL.ListThresholdsForCreate \
     import ListThresholdsForCreate as MySQLListThresholdsForCreate

class ListThresholdsForCreate(MySQLListThresholdsForCreate):
    pass
