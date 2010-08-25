#!/usr/bin/env python

"""
_InsertThreshold_

This module inserts thresholds for the given sites for Oracle

"""

__revision__ = "$Id: InsertThreshold.py,v 1.1 2009/10/05 20:05:52 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

import threading

from WMCore.ResourceControl.MySQL.InsertThreshold import InsertThreshold as MySQLInsertThreshold

class InsertThreshold(MySQLInsertThreshold):
    """
    _InsertThreshold_
    
    This module finds thresholds for the given sites for Oracle
    """
