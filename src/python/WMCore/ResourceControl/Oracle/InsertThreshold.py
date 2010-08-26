#!/usr/bin/env python
"""
_InsertThreshold_

This module inserts thresholds for the given sites for Oracle
"""

__revision__ = "$Id: InsertThreshold.py,v 1.2 2010/07/15 16:57:06 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

import threading

from WMCore.ResourceControl.MySQL.InsertThreshold import InsertThreshold as MySQLInsertThreshold

class InsertThreshold(MySQLInsertThreshold):
    pass
