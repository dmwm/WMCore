#!/usr/bin/env python
"""
_Delete_

MySQL implementation of Masks.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/24 21:51:50 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Masks.Delete import Delete as DeleteMasksMySQL

class Delete(DeleteMasksMySQL):
    sql = DeleteMasksMySQL.sql
    