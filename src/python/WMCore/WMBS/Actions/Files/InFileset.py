#!/usr/bin/env python
"""
_InFilesetAction_

list of files in a WMBS fileset
"""

__revision__ = "$Id: InFileset.py,v 1.1 2008/06/10 17:04:45 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class InFilesetAction(BaseAction):
    name = "Files.InFileset"
    
    def execute(self, fileset=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(fileset)
        except Exception, e:
            self.logger.exception(e)
            return False