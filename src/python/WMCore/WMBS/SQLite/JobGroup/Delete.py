#!/usr/bin/env python
"""
_Delete_

SQLite implementation of JobGroup.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/21 17:14:58 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.Delete import Delete as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql
