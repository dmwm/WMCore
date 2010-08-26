#!/usr/bin/env python
"""
_LoadOutput_

Oracle implementation of Workflow.LoadOutput
"""

__revision__ = "$Id: LoadOutput.py,v 1.2 2009/12/04 21:27:55 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadOutput import LoadOutput as LoadOutputMySQL

class LoadOutput(LoadOutputMySQL):
    pass
