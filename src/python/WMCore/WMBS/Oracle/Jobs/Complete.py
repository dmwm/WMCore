#!/usr/bin/env python
"""
_Complete_
Oracle implementation of Jobs.Complete

move file into wmbs_group_job_acquired
"""

__all__ = []
__revision__ = "$Id: Complete.py,v 1.4 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobMySQL

class Complete(CompleteJobMySQL):
    sql = CompleteJobMySQL.sql
