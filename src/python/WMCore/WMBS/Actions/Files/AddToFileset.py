#!/usr/bin/env python
"""
_AddFileToFilesetAction_

Add a (list of) new file(s) to WMBS
"""

__revision__ = "$Id: AddToFileset.py,v 1.1 2008/06/10 17:04:46 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class AddFileToFilesetAction(BaseAction):
    name = "Files.AddToFileset"
        
    def execute(self, file=None, fileset=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(file, fileset)
        except Exception, e:
            self.logger.exception(e)
            return False