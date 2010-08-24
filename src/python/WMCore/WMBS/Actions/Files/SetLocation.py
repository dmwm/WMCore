#!/usr/bin/env python
"""
_SetFileLocationAction_

Remove a file from WMBS
"""

__revision__ = "$Id: SetLocation.py,v 1.1 2008/06/10 17:04:45 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class SetFileLocationAction(BaseAction):
    name = "Files.SetLocation"
        
    def execute(self, file=None, sename=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(file, sename)
        except Exception, e:
            self.logger.exception(e)
            return False