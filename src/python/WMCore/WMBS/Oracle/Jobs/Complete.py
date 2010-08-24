#!/usr/bin/env python
"""
_Complete_
Oracle implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.2 2008/11/24 21:51:38 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobMySQL

class Complete(CompleteJobMySQL):
    
    sql = ClearStatucJobMySQL.sql