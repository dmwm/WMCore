#!/usr/bin/env python
"""
_DeleteWorkflowAction_

Delete a workflow

"""
__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2008/06/09 16:15:56 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction
"""
Issue the delete sql. 

Currently does no checking for success beyond exception 
catching. Could use WorkflowExistsAction to check that 
the Workflow exists before the delete and doesn't after
but that is quite a lot of DB roundtrips. 
"""
class DeleteWorkflowAction(BaseAction):
    name = "Workflow.Delete"
        
    def execute(self, spec=None, owner=None, name = None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            result = action.execute(spec=spec, owner=owner, name=name)
            return True
        except Exception, e:
            self.logger.exception(e)
            return False