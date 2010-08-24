#!/usr/bin/env python
"""
_Delete_

MySQL implementation of JobGroup.Delete

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/11/24 21:51:44 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.Delete import Delete as DeleteJobGroupMySQL

class Delete(DeleteJobGroupMySQL):
    sql = DeleteJobGroupMySQL.sql