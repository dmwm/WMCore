#!/usr/bin/env python
"""
_Exists_

MySQL implementation of JobGroup.Exists
"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/11/24 21:51:43 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.Exists import Exists as ExistsJobGroupMySQL

class Exists(ExistsJobGroupMySQL):
    sql = "select id from wmbs_jobgroup where guid = :guid"
    