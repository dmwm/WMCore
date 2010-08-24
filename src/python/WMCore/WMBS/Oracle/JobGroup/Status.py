#!/usr/bin/env python
"""
_New_
MySQL implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.1 2008/11/24 21:51:43 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.Status import New as StatusJobGroupMySQL

class Status(StatusJobGroupMySQL):
    
    sql = StatusJobGroupMySQL.sql