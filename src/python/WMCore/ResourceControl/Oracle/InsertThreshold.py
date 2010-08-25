#!/usr/bin/env python
"""
_InsertThreshold_

This module inserts thresholds for the given sites for Oracle
"""




import threading

from WMCore.ResourceControl.MySQL.InsertThreshold import InsertThreshold as MySQLInsertThreshold

class InsertThreshold(MySQLInsertThreshold):
    pass
