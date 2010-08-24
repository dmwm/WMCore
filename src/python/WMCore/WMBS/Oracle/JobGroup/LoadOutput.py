#!/usr/bin/env python
"""
_LoadOutput_

MySQL implementation of JobGroup.LoadOutput
"""

__all__ = []
__revision__ = "$Id: LoadOutput.py,v 1.1 2008/11/24 21:51:44 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadOutput import LoadOutput as LoadOutputJobGroupMySQL

class LoadOutput(LoadOutputJobGroupMySQL):
    sql = LoadOutputJobGroupMySQL.sql