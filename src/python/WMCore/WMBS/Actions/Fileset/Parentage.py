#!/usr/bin/env python
"""
_FilesetParentageAction_

Assign parentage to two filesets in WMBS

"""

__revision__ = "$Id: Parentage.py,v 1.1 2008/06/09 16:13:43 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction
from WMCore.WMBS.Fileset import Fileset

class FilesetParentageAction(BaseAction):
    name = "Fileset.Parentage"
    
    def execute(self, dbinterface = None, child=None, parent=None):
        """
        import the approriate SQL object and execute it
        """ 
        
        if  not isinstance(child, Fileset) and not isinstance(parent, Fileset):
            raise TypeError, "Parent or Child not a WMBS Fileset object"
        
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(child.id, parent.id)
        except:
            self.logger.exception(e)
            return False