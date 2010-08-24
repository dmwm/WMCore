#!/usr/bin/env python
"""
_Complete_
Oracle implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Complete.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobMySQL

class Complete(CompleteJobMySQL):
    
    sql = CompleteJobMySQL.sql