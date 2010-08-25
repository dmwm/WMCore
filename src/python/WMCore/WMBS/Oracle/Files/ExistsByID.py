#!/usr/bin/env python
"""
_ExistsByID_

Oracle implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: ExistsByID.py,v 1.2 2009/04/29 23:17:47 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Files.ExistsByID import ExistsByID as ExistsByIDMySQL

class ExistsByID(ExistsByIDMySQL):
    sql = ExistsByIDMySQL.sql