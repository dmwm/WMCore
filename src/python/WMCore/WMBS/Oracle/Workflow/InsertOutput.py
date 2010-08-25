#!/usr/bin/env python
"""
_InsertOutput_

Oracle implementation of Workflow.InsertOutput
"""

__all__ = []
__revision__ = "$Id: InsertOutput.py,v 1.1 2009/04/01 18:47:28 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.InsertOutput import InsertOutput as InsertOutputMySQL

class InsertOutput(InsertOutputMySQL):
    sql = InsertOutputMySQL.sql
    
