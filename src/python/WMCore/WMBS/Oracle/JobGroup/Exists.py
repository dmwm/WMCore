#!/usr/bin/env python
"""
_Exists_

Oracle implementation of JobGroup.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.Exists import Exists as ExistsJobGroupMySQL

class Exists(ExistsJobGroupMySQL):
    sql = "select id from wmbs_jobgroup where guid = :guid"
    