#!/usr/bin/env python
"""
_New_
Oracle implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.2 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.Status import New as StatusJobGroupMySQL

class Status(StatusJobGroupMySQL):
    
    sql = StatusJobGroupMySQL.sql