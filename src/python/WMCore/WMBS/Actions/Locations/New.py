#!/usr/bin/env python
"""
_NewLocationAction_

Add a location to WMBS
"""

__revision__ = "$Id: New.py,v 1.1 2008/06/10 11:55:58 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class NewLocationAction(BaseAction):
    name = "Locations.New"
    
    def execute(self, sename=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        self.logger.debug("Adding location: %s" % sename)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(sename)
        except Exception, e:
            self.logger.exception(e)
            return False