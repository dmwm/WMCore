#!/usr/bin/env python
"""
_Active_
Oracle implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""

__all__ = []
__revision__ = "$Id: Active.py,v 1.4 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobMySQL

class Active(ActiveJobMySQL):
    sql = ActiveJobMySQL.sql
