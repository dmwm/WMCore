#!/usr/bin/env python
"""
_DeleteFilesetAction_

Delete a fileset from WMBS
"""

__revision__ = "$Id: Delete.py,v 1.1 2008/06/09 16:13:43 metson Exp $"
__version__ = "$Revision: 1.1 $"
from WMCore.WMBS.Actions.Action import BaseAction
"""
Issue the delete sql. 

Currently does no checking for success beyond exception 
catching. Could use FilesetExistsAction to check that 
the Fileset exists before the delete and doesn't after
but that is quite a lot of DB roundtrips. 
"""
class DeleteFilesetAction(BaseAction):
    name = "Fileset.Delete"
        
    def execute(self, name = None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            result = action.execute(name=name)
            return True
        except Exception, e:
            self.logger.exception(e)
            return False