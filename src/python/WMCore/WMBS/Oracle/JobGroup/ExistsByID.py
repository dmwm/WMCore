#!/usr/bin/env python
"""
_ExistsByID_

Oracle implementation of JobGroup.ExistsByID
"""

__all__ = []
__revision__ = "$Id: ExistsByID.py,v 1.1 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.ExistsByID import ExistsByID as ExistsByIDJobGroupMySQL

class ExistsByID(ExistsByIDJobGroupMySQL):
    sql = ExistsByIDJobGroupMySQL.sql