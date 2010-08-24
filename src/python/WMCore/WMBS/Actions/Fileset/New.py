#!/usr/bin/env python
"""
_NewFilesetAction_

Add a fileset to WMBS
"""

__revision__ = "$Id: New.py,v 1.1 2008/06/09 16:13:43 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class NewFilesetAction(BaseAction):
    name = "Fileset.New"
        
    def execute(self, name = None, dbinterface = None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        self.logger.debug("Adding %s" % name)    
        action = myclass(self.logger, dbinterface)
        try:
            action.execute(name)
            return True
        except Exception, e:
            self.logger.exception(e)
            return False