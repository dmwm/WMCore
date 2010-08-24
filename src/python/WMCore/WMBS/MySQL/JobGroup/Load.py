#!/usr/bin/env python
"""
_Load_
MySQL implementation of Jobs.Load

Load a JobGroup from the DB
"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.1 2008/10/01 21:56:14 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class Load(DBFormatter):
    sql = ""