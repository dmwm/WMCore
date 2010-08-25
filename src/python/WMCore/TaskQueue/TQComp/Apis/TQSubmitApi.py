#!/usr/bin/env python
"""
API to insert new tasks into the TQ queue.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQSubmitApi.py,v 1.4 2009/09/29 12:23:02 delgadop Exp $"
__version__ = "$Revision: 1.4 $"

import logging
import threading
import time
import xml.dom.minidom

from TQComp.Apis.TQApi import TQApi
from TQComp.Apis.TQApiData import validateTask
from TQComp.Constants import taskStates


class TQSubmitApi(TQApi):
    """
    API to insert new tasks into the TQ queue.

    Example to insert a task using this class, from a WMCore component.

      myThread = threading.currentThread()
      tqApi = TQSubmitApi(myThread.logger, self.config, myThread.transaction)

      id = 'some-id-34'
      spec = '/pool/TaskQueue/playground/specs/some-sec-file.xml'
      sandbox = '/pool/TaskQueue/playground/sandbox/some-sandbox.tar.gz'
      mydict = {'id': id, 'spec': spec, 'wkflow': 'TEST' , 'type': 5, \
                'sandbox': sandbox, 'reqs' = None, 'req_se': None}
      task = TQComp.Apis.TQApiData.Task(mydict)

      tqApi.insertTask(task)
    """

    def __init__(self, logger, tqRef, dbIface = None):
        """
        Constructor.
        """
        # Call our parent to set everything up
        TQApi.__init__(self, logger, tqRef, dbIface)


    def insertTaskBulk(self, taskList):
        """
        Insert a bunch of tasks.
        The 'taskList' must be a list of dicts as defined in
        TQComp.Apis.TQApiData.Task.
        """
        where = 0
        todel = []
        for task in taskList:
            try:
                validateTask(task)
            except ValueError, inst:
                self.logger.warning('%s' % inst)
                todel.insert(0,where)
            where += 1

        for i in todel:
            taskList.pop(i)
           
        self.transaction.begin()
        self.queries.addTasksBulk(taskList)
        self.transaction.commit()

    
    def insertTask(self, task):
        """
        Insert a task in the queue.
        The 'task' must be a dict as defined in TQComp.Apis.TQApiData.Task.
        """

#        self.logger.debug('Inserting task: %s, %s, %s, %s' %\
#                     (spec, sandbox, wkflow, type))

        # This may raise a ValueError exception if not compliant
        validateTask(task)
            
        # Insert job and its characteristics into the database
        self.transaction.begin()
        self.queries.addTask(task)
        self.transaction.commit()


    def removeOneTask(self, taskid):
        """
        Remove a task from the queue.
        The 'taskid' must be an existing task ID (otherwise, nothing is done).
        """
        self.transaction.begin()
        self.queries.removeOneTask(taskid)
        self.transaction.commit()


    def removeTasks(self, filter, deleteAll = False):
        """
        Remove matching tasks from the queue (filter will be used in the 
        predicate of WHERE clause).
        
        NOTE: If filter is empty, no deletion will be performed. To delete
        all tasks, set the deleteAll flag to True (the filter will be ignored).
        """
        if (not filter) and (not deleteAll):
            self.logger.warning("No 'filter' provided and 'deleteAll' flag \
not set. Doing nothing.")
            return

        if deleteAll:
            filter = {}
        
        self.transaction.begin()
        self.queries.removeTasks(filter)
        self.transaction.commit()


    def updateTasks(self, idList, keys, vals):
        """
        Updates tasks whose id is in 'idList' with the keys in
        'keys' and the values in 'vals' (if empty, then nothing is done). 

        For same values to all updates (applied to different ids), 'vals'
        should be a list of the same length than 'keys' (otherwise, an
        exception is raised).
        
        For different bindings for each id, 'vals' should be a list
        of lists. The length of the external list must be the same of 
        'idList' and the list of each member list must be as that of
        'keys' (otherwise, an exception is raised).
        """
        self.transaction.begin()
        self.queries.updateTasks(idList, keys, vals)
        self.transaction.commit()


    def updatePilot(self, pilotId, vars):
        """
        Updates pilot with given id, using the fields in 'vars'.
        If the field last_heartbeat is included with a value 
        of 0, the CURRENT_TIMESTAMP is used for it.
        """
        self.transaction.begin()
        self.queries.updatePilot(pilotId, vars)
        self.transaction.commit()


    def killTasks(self, taskIds):
        """
        Kill all tasks whose Id is included in the 'taskIds' list
        (and exist in the queue, others are just ignored).

        Actions:
          NOT YET DONE For those running, tell the pilot to kill the real job.
          For all tasks, change state to "Killed" and 
            archive them (eliminate them from the queue).
        """
        if not taskIds:
            return 
        
        self.transaction.begin()
        # TODO: for each running task, prepare a message for its pilot, so that
        #       it is told to kill the real job (on reply of next hbeat message
        self.queries.updateTasks(taskIds, ['state'], [taskStates['Killed']])
        self.queries.archiveTasksById(taskIds)
        self.queries.removeTasksById(taskIds)
        self.transaction.commit()


