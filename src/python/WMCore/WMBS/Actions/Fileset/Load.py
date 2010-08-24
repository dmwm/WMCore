#!/usr/bin/env python
"""
_LoadFilesetAction_

Read a fileset from WMBS
"""

__revision__ = "$Id: Load.py,v 1.1 2008/06/09 16:13:43 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class LoadFilesetAction(BaseAction):
    name = "Fileset.Load"
    
    def execute(self, name=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(fileset = name)
        except Exception, e:
            self.logger.exception(e)
            return False