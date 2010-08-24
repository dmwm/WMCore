#!/usr/bin/env python
"""
_WorkflowExistsAction_

See if a workflow exists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/06/09 16:15:56 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.Actions.Action import BaseAction

class WorkflowExistsAction(BaseAction):
    name = "Workflow.Exists"
    
    def execute(self, spec=None, owner=None, name = None, dbinterface=None):
        """
        import the approriate SQL object and execute it
        """ 
        myclass = self.loadDialect(self.name, dbinterface)
        action = myclass(self.logger, dbinterface)
        try:
            return action.execute(spec=spec, owner=owner, name=name)
        except Exception, e:
            self.logger.exception(e)
            return False