#!/usr/bin/env python
"""
_Active_
Oracle implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Active.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

    
from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobMySQL

class Active(ActiveJobMySQL):
    
    sql = ActiveJobMySQL.sql