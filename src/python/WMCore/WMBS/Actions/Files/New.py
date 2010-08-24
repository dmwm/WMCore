#!/usr/bin/env python
"""
_NewFilesAction_

Add a (list of) new file(s) to WMBS
"""

__revision__ = "$Id: New.py,v 1.3 2008/06/19 11:48:14 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.Actions.Action import BoundAction
from WMCore.DAOFactory import DAOFactory

class NewFileAction(BoundAction):
    name = "Files.Add"
    
    def execute(self, files=None, size=0, events=0, run=0, lumi=0, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(files, size, events, run, lumi)
        except Exception, e:
            self.logger.exception(e)
            return False