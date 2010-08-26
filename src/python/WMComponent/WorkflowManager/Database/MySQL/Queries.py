#!/usr/bin/env python
#pylint: disable-msg=E1103,W0222
"""
_Queries_

This module implements the mysql backend for the 
WorkflowManager
"""

__revision__ = "$Id: Queries.py,v 1.7 2009/10/04 14:44:48 riahi Exp $"
__version__ = "$Revision: 1.7 $"
__author__ = "james.jackson@cern.ch"

import threading
from WMCore.Database.DBFormatter import DBFormatter

class Queries(DBFormatter):
    """
    _Queries_
    This module implements the MySQL backend for the WorkflowManager
    """
    
    def __init__(self):
        myThread = threading.currentThread()
        DBFormatter.__init__(self, myThread.logger, myThread.dbi)
        
    def addManagedWorkflow(self, workflowId, filesetMatch, splitAlgo, wfType):
        """
        Adds a workflow to be managed
        """
        sqlStr = """INSERT INTO wm_managed_workflow
                        (workflow, fileset_match, split_algo, type)
                    VALUES (:workflow, :fileset_match, :split_algo, :type)
                    """

        self.execute(sqlStr, {'workflow' : workflowId, \
                              'fileset_match' : filesetMatch, \
                              'split_algo' : splitAlgo, \
                              'type' : wfType})

    def removeManagedWorkflow(self, workflowId, filesetMatch):
        """
        Removes a workflow from being managed
        """
        sqlStr = """DELETE FROM wm_managed_workflow
                    WHERE workflow = :workflow
                    AND fileset_match = :fileset_match
                    """
        self.execute(sqlStr, {'workflow' : workflowId, \
                              'fileset_match' : filesetMatch})
    
    def getManagedWorkflows(self):
        """
        Returns all workflows and fileset patterns that are currently being
        managed
        """
        sqlStr = """SELECT id, workflow, fileset_match, split_algo, type
                    FROM wm_managed_workflow
                    """
        result = self.execute(sqlStr, {})
        return self.formatDict(result)
    
    def getUnsubscribedFilesets(self):
        """
        Returns all filesets that do not have a subscription
        """
        sqlStr = """SELECT wmbs_fileset.id, wmbs_fileset.name 
                    FROM wmbs_fileset 
                    WHERE NOT EXISTS (SELECT 1 FROM wmbs_subscription WHERE 
                                   wmbs_subscription.fileset = wmbs_fileset.id)
                    """
        result = self.execute(sqlStr, {})
        return self.formatDict(result)

    def getUnsubscribedWorkflows(self):
        """
        Returns all workflows  that do not have a subscription
        """
        sqlStr = """SELECT id, workflow, fileset_match, split_algo, type
                    FROM wm_managed_workflow 
                    WHERE workflow NOT in (SELECT workflow from wmbs_subscription)
                    """
        result = self.execute(sqlStr, {})
        return self.formatDict(result)

    def getAllFilesets(self):
        """
        Returns all filesets 
        """
        sqlStr = """SELECT wmbs_fileset.id, wmbs_fileset.name 
                    FROM wmbs_fileset 
                    """
        result = self.execute(sqlStr, {})
        return self.formatDict(result)
    
    def getLocations(self, managedWorkflowId):
        """
        Returns all marked locations for a watched workflow / fileset match
        """
        sqlStr = """
          SELECT wmbs_location.site_name, wm_managed_workflow_location.valid
          FROM wmbs_location, wm_managed_workflow_location
          WHERE wmbs_location.id = wm_managed_workflow_location.location
          AND wm_managed_workflow_location.managed_workflow = :managed_workflow;
          """
        result = self.execute(sqlStr, {'managed_workflow' : managedWorkflowId})
        return self.formatDict(result)

    def markLocation(self, workflowId, filesetMatch, location, valid):
        """
        Adds a location to the created subscription white / black lists
        """
        sqlStr = """
           INSERT INTO wm_managed_workflow_location (managed_workflow, location,
                                                      valid) 
           VALUES ((SELECT id from wm_managed_workflow
                     WHERE workflow = :workflow AND fileset_match = :fsmatch),
                   (SELECT id FROM wmbs_location WHERE site_name = :location),
                   :valid);
           """
        self.execute(sqlStr, {'workflow' : workflowId, \
                              'fsmatch' : filesetMatch, \
                              'location' : location, \
                              'valid' : valid})

    def unmarkLocation(self, workflowId, filesetMatch, location):
        """
        Removes a location from the created subscription white / black lists
        """
        sqlStr = """DELETE FROM wm_managed_workflow_location
            WHERE managed_workflow = (SELECT id FROM wm_managed_workflow
                                       WHERE workflow = :workflow
                                       AND fileset_match = :fsmatch)
            AND location = (SELECT id FROM wmbs_location
                             WHERE site_name = :location)
            """
        self.execute(sqlStr, {'workflow' : workflowId, \
                              'fsmatch' : filesetMatch, \
                              'location' : location})

    def execute(self, sqlStr, args):
        """"
        Executes the queries by getting the current transaction
        and dbinterface object that is stored in the reserved words of
        the thread it operates in.
        """
        myThread = threading.currentThread()
        currentTransaction = myThread.transaction
        return currentTransaction.processData(sqlStr, args)
