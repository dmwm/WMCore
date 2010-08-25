#!/usr/bin/env python
"""
_ListThresholdsForSubmit_

SQLite implementation of ResourceControl.ListThresholdsForSubmit
"""




from WMCore.ResourceControl.MySQL.ListThresholdsForSubmit \
     import ListThresholdsForSubmit as MySQLListThresholdsForSubmit

class ListThresholdsForSubmit(MySQLListThresholdsForSubmit):
    pass
