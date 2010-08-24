#!/usr/bin/env python
"""
_NewFilesetAction_

Add a fileset to WMBS
"""

__revision__ = "$Id: New.py,v 1.3 2008/06/19 11:30:58 swakef Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.Actions.Action import BaseAction

class NewFilesetAction(BaseAction):
    name = "Fileset.New"
        
    def execute(self, filesetname=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        self.logger.debug("Adding Fileset: %s" % filesetname)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(filesetname)
        except Exception, e:
            self.logger.exception(e)
            return False