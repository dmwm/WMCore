#!/usr/bin/env python

"""
_InsertSite_

This module inserts sites for Oracle

"""

__revision__ = "$Id: InsertSite.py,v 1.1 2009/10/05 20:04:55 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

import threading

from WMCore.ResourceControl.MySQL.InsertSite import InsertSite as MySQLInsertSite

class InsertSite(MySQLInsertSite):
    """
    _InsertSite_
    
    This module inserts sites for Oracle
    
    """
