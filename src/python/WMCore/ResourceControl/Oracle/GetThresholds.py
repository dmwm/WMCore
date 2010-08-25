#!/usr/bin/env python

"""
_GetThresholds_

This module finds thresholds for the given sites for Oracle

"""

__revision__ = "$Id: GetThresholds.py,v 1.1 2009/10/05 20:06:46 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

import threading

from WMCore.ResourceControl.MySQL.GetThresholds import GetThresholds as MySQLGetThresholds

class GetThresholds(MySQLGetThresholds):
    """
    Oracle implementation

    """
