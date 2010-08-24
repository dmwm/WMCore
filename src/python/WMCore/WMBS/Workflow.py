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

__revision__ = "$Id: Workflow.py,v 1.19 2009/03/09 13:15:12 sfoulkes Exp $"
__version__ = "$Revision: 1.19 $"

from WMCore.WMBS.WMBSBase import WMBSBase
from WMCore.DataStructs.Workflow import Workflow as WMWorkflow

class Workflow(WMBSBase, WMWorkflow):
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
    def __init__(self, spec = None, owner = None, name = None, id = -1):
        WMBSBase.__init__(self)
        WMWorkflow.__init__(self, spec = spec, owner = owner, name = name)

        self.id = id
        return
        
    def exists(self):
        """
        Does a workflow exist with this spec and owner, return the id
        """
        action = self.daofactory(classname = "Workflow.Exists")
        return action.execute(spec = self.spec, owner = self.owner,
                              name = self.name, conn = self.getReadDBConn(),
                              transaction = self.existingTransaction())
    
    def create(self):
        """
        Write a workflow to the database
        """
        self.id = self.exists()
        if self.id != False:
            self.load()
            return
        
        action = self.daofactory(classname = "Workflow.New")
        action.execute(spec = self.spec, owner = self.owner, name = self.name,
                       conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())
        
        self.id = self.exists()
        self.commitIfNew()
        return
    
    def delete(self):
        """
        Remove this workflow from WMBS
        """
        action = self.daofactory(classname = "Workflow.Delete")
        action.execute(id = self.id, conn = self.getWriteDBConn(),
                       transaction = self.existingTransaction())

        self.commitIfNew()
        return
        
    def load(self):
        """
        Load a workflow from WMBS
        """
        if self.id > 0:
            action = self.daofactory(classname = "Workflow.LoadFromID")
            result = action.execute(workflow = self.id,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        elif self.name != None:
            action = self.daofactory(classname = "Workflow.LoadFromName")
            result = action.execute(workflow = self.name,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "Workflow.LoadFromSpecOwner")
            result = action.execute(spec = self.spec, owner = self.owner,
                                    conn = self.getReadDBConn(),
                                    transaction = self.existingTransaction())

        self.id = result["id"]
        self.spec = result["spec"]
        self.name = result["name"]
        self.owner = result["owner"]
        return
