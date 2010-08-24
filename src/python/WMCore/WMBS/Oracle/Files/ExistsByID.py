#!/usr/bin/env python
"""
_ExistsByID_

Oracle implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: ExistsByID.py,v 1.1 2009/02/16 16:06:08 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.ExistsByID import ExistsByID as ExistsByIDJobMySQL

class ExistsByID(ExistsByIDJobMySQL):
    sql = ExistsByIDJobMySQL.sql