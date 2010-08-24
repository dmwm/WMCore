#!/usr/bin/env python
"""
_Active_
MySQL implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Active.py,v 1.2 2008/11/24 21:51:42 sryu Exp $"
__version__ = "$Revision: 1.2 $"

    
from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobMySQL

class Active(ActiveJobMySQL):
    
    sql = ActiveJobMySQL.sql