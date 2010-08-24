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

__revision__ = "$Id: Workflow.py,v 1.8 2008/06/20 12:34:21 metson Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.BusinessObject import BusinessObject

class Workflow(BusinessObject):
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
        BusinessObject.__init__(logger=logger, dbfactory=dbfactory)
        #TODO: define a url-like scheme for spec's and enforce it here
        self.spec = spec
        self.name = name
        self.owner = owner
        self.name = name
        self.dbfactory = dbfactory
        self.logger = logger
        self.daofactory = DAOFactory(package='WMCore.WMBS', 
                                     logger=self.logger, 
                                     dbinterface=self.dbfactory.connect())
        
        
    def exists(self):
        """
        Does a workflow exist with this spec and owner
        """
        action = self.daofactory(classname='Workflow.Exists')
        return action.execute(spec=self.spec, 
                              owner=self.owner, 
                              name=self.name)
    
    def create(self):
        """
        Write a workflow to the database
        """
        action = self.daofactory(classname='Workflow.New')
        action.execute(spec=self.spec, 
                       owner=self.owner, 
                       name=self.name)

    def delete(self):
        """
        Remove this workflow from WMBS
        """
        self.logger.warning('You are removing the following workflow from WMBS %s (%s) owned by %s'
                                 % (self.name, self.spec, self.owner))
        action = self.daofactory(classname='Workflow.Delete')
        action.execute(spec=self.spec, 
                       owner=self.owner, 
                       name=self.name)
        
        
        