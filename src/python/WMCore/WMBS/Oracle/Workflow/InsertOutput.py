#!/usr/bin/env python
"""
_InsertOutput_

Oracle implementation of Workflow.InsertOutput
"""

__revision__ = "$Id: InsertOutput.py,v 1.2 2009/12/04 21:27:55 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.InsertOutput import InsertOutput as InsertOutputMySQL

class InsertOutput(InsertOutputMySQL):
    pass
    
