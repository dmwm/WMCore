#!/usr/bin/env python
"""
_ExistsByID_

Oracle implementation of Files.Exists
"""

__all__ = []
__revision__ = "$Id: ExistsByID.py,v 1.1 2009/04/28 22:28:59 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.ExistsByID import ExistsByID as ExistsByIDMySQL

class ExistsByID(ExistsByIDMySQL):
    sql = ExistsByIDMySQL.sql