#!/usr/bin/env python
"""
_NewFilesAction_

Add a (list of) new file(s) to WMBS
"""

__revision__ = "$Id: New.py,v 1.1 2008/06/10 17:04:46 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class NewFileAction(BaseAction):
    name = "Files.Add"
        
    def execute(self, files=None, size=0, events=0, run=0, lumi=0, dbinterface=None):
        """
        Add a (list of) new file(s) to WMBS
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(files, size, events, run, lumi)
        except:
            return False