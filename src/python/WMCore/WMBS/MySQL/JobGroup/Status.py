#!/usr/bin/env python
"""
_New_
MySQL implementation of JobGroup.Status
"""
__all__ = []
__revision__ = "$Id: Status.py,v 1.1 2008/10/01 15:43:41 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Status(MySQLBase):
    sql = """select (
        select count(id) from wmbs_job where jobgroup=:group
        ) as av, (
        select count(job) from wmbs_group_job_acquired where jobgroup=:group
        ) as ac, (
        select count(job) from wmbs_group_job_failed where jobgroup=:group
        ) as fa, (
        select count(job) from wmbs_group_job_complete where jobgroup=:group
        ) as cm
    """

    def format(self, result):
        result = MySQLBase.format(self, result)
        return result[0]
        
    def execute(self, group=None, conn = None, transaction = False):
        binds = self.getBinds(group=group)
        self.logger.debug('JobGroup.Status sql: %s' % self.sql)
        self.logger.debug('JobGroup.Status binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds)
        return self.format(result)