#!/usr/bin/env python
"""
_ExistsByID_

SQLite implementation of Jobs.Exists
"""

__all__ = []
__revision__ = "$Id: ExistsByID.py,v 1.1 2008/12/05 21:06:58 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.ExistsByID import ExistsByID as ExistsByIDJobMySQL

class ExistsByID(ExistsByIDJobMySQL):
    sql = ExistsByIDJobMySQL.sql