#!/usr/bin/env python
"""
_LoadOutput_

Oracle implementation of Workflow.LoadOutput
"""

__all__ = []
__revision__ = "$Id: LoadOutput.py,v 1.1 2009/04/01 18:47:28 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.LoadOutput import LoadOutput as LoadOutputMySQL

class LoadOutput(LoadOutputMySQL):
    sql = LoadOutputMySQL.sql 
