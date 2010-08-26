#!/usr/bin/env python
"""
_New_

Oracle implementation of JobGroup.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2009/01/11 17:51:39 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import time

from WMCore.WMBS.MySQL.JobGroup.New import New as NewJobGroupMySQL

class New(NewJobGroupMySQL):
    sql = """INSERT INTO wmbs_jobgroup (id, subscription, guid, output,
             last_update) VALUES (wmbs_jobgroup_SEQ.nextval, :subscription,
             :guid, :output, %d)""" % time.time()
