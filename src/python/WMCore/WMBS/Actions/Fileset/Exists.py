#!/usr/bin/env python
"""
_FilesetExistsAction_

See is a fileset exists in WMBS
"""

__revision__ = "$Id: Exists.py,v 1.1 2008/06/09 16:13:43 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class FilesetExistsAction(BaseAction):
    name = "Fileset.Exists"
    
    def execute(self, name = None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(name=name)
        except Exception, e:
            self.logger.exception(e)
            return False