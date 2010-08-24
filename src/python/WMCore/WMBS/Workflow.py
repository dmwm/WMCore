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

__revision__ = "$Id: Workflow.py,v 1.3 2008/05/02 17:29:24 metson Exp $"
__version__ = "$Revision: 1.3 $"

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
    def __init__(self, spec=None, owner=None, wmbs=None):
        self.wmbs = wmbs
        #TODO: define a url-like scheme for spec's and enforce it here
        self.spec = spec
        self.owner = owner
        
    def exists(self):
        """
        Does a workflow exist with this spec and owner
        """
        result = -1
        for f in self.wmbs.workflowExists(self.spec, self.owner):
            for i in f.fetchall():
                result = i[0]
        if result > 0:
            return True
        else:
            return False
    
    def create(self):
        """
        Write a workflow to the database
        """
        try:
            self.wmbs.newWorkflow(self.spec, self.owner)
        except Exception, e:
            self.wmbs.logger.exception('Workflow %s:%s exists' % (self.spec, self.owner))
            raise e
    
    def delete(self):
        """
        Remove this workflow from WMBS
        """
        self.wmbs.logger.warning('you are removing the following workflow from WMBS %s %s'
                                 % (self.spec, self.owner))
        self.wmbs.deleteWorkflow(self.spec, self.owner)
    