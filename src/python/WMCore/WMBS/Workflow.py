#!/usr/bin/env python
"""
_Workflow_

A simple object representing a Workflow in WMBS.

A workflow has an owner (e.g. PA instance, CRAB server user) and
a specification. The specification describes how jobs should be 
created and what the jobs are supposed to do. This description 
is held external to WMBS, WMBS just stores a pointer (url) to 
the specification file. A workflow can be used with many 
filesets and subscriptions (e.g. repeating the same task on a 
bunch of data).

workflow + fileset = subscription

"""

__revision__ = "$Id: Workflow.py,v 1.6 2008/06/09 12:38:51 metson Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.Actions.WorkflowExists import WorkflowExistsAction
from WMCore.WMBS.Actions.NewWorkflow import NewWorkflowAction
from WMCore.WMBS.Actions.DeleteWorkflow import DeleteWorkflowAction

class Workflow(object):
    """
    A simple object representing a Workflow in WMBS.

    A workflow has an owner (e.g. PA instance, CRAB server user) and
    a specification. The specification describes how jobs should be 
    created and what the jobs are supposed to do. This description 
    is held external to WMBS, WMBS just stores a pointer (url) to 
    the specification file. A workflow can be used with many 
    filesets and subscriptions (e.g. repeating the same task on a 
    bunch of data).
    
    workflow + fileset = subscription
    """

    def __init__(self, spec=None, owner=None, name=None, logger=None, dbfactory=None):
        self.wmbs = wmbs
        #TODO: define a url-like scheme for spec's and enforce it here
        self.spec = spec
        self.name = name
        self.owner = owner
        self.name = name
        self.dbfactory = dbfactory
        self.logger = logger
        
    def exists(self):
        """
        Does a workflow exist with this spec and owner
        """
        conn = dbfactory.connect()
        action = WorkflowExistsAction(self.logger)
        return action.execute(spec=self.spec, owner=self.owner, name=self.name,
                               dbinterface=conn)
    
    def create(self):
        """
        Write a workflow to the database
        """
        conn = dbfactory.connect()
        action = NewWorkflowAction(self.logger)
        action.execute(spec=self.spec, owner=self.owner, name=self.name,
                               dbinterface=conn)

    def delete(self):
        """
        Remove this workflow from WMBS
        """
        self.logger.warning('You are removing the following workflow from WMBS %s (%s) owned by %s'
                                 % (self.name, self.spec, self.owner))
        conn = dbfactory.connect()
        action = DeleteWorkflowAction(self.testlogger)
        return action.execute(spec=self.spec, owner=self.owner, name=self.name,
                               dbinterface=conn)
        
        
        