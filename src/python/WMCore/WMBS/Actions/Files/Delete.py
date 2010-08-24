#!/usr/bin/env python
"""
_DeleteFilesAction_

Remove a file from WMBS
"""

__revision__ = "$Id: Delete.py,v 1.1 2008/06/10 17:04:46 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class DeleteFileAction(BaseAction):
    name = "Files.Delete"
    
    def execute(self, files=None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        self.logger.debug("Removing files: %s" % files)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(files)
        except Exception, e:
            self.logger.exception(e)
            return False