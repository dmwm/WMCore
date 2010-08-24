#!/usr/bin/env python
"""
_New_
Oracle implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.3 2008/12/10 23:02:05 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.JobGroup.Status import Status as StatusJobGroupMySQL

class Status(StatusJobGroupMySQL):
    
    sql = StatusJobGroupMySQL.sql