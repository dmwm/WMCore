#!/usr/bin/env python
"""
_LoadOutput_

Oracle implementation of JobGroup.LoadOutput
"""

__all__ = []
__revision__ = "$Id: LoadOutput.py,v 1.2 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.JobGroup.LoadOutput import LoadOutput as LoadOutputJobGroupMySQL

class LoadOutput(LoadOutputJobGroupMySQL):
    sql = LoadOutputJobGroupMySQL.sql