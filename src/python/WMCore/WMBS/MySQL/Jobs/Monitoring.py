#!/usr/bin/env python
"""
_Monitoring_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []
__revision__ = "$Id: Monitoring.py,v 1.1 2009/05/08 14:51:24 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class JobsByState(DBFormatter):
    sql = ""