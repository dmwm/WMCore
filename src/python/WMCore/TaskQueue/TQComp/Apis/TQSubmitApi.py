#!/usr/bin/env python
"""
API to insert new tasks into the TQ queue.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQSubmitApi.py,v 1.5 2009/12/16 18:09:05 delgadop Exp $"
__version__ = "$Revision: 1.5 $"

import logging
import threading
import time
import xml.dom.minidom as dom

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

    

    def insertTaskDeps(self, task):
        """
        Insert a task in the queue previous analysis of its dependencies
        (as expressed in its spec file), and possible modification of its
        'reqs' field as a result of it.

        The 'task' must be a dict as defined in TQComp.Apis.TQApiData.Task.
        """

#        self.logger.debug('Inserting task: %s, %s, %s, %s' %\
#                     (spec, sandbox, wkflow, type))

        # This may raise a ValueError exception if not compliant
        validateTask(task)

        # Inspect deps
        inputFiles = self.__parseInputFiles(task['spec'])
        if inputFiles:
            for file in inputFiles:
            # For each file we include a dependency that is significant only 
            # for 10 seconds (after that, it is just: 'and True') but is still
            # counted in the ranking (more points for this task if the pilot
            # holds the file). 
            # We might change this when we make out our mind about how we want
            # data dependencies to work (if we want it to be like this, we'd 
            # be probably better off by having a configurable ranking 
            #expression, instead using QTIME's trick).
                task['reqs'] += " and (('%s' in cache) or (QTIME()>10))" % (file)
            
        # Insert job and its characteristics into the database
        self.transaction.begin()
        self.queries.addTask(task)
        self.transaction.commit()


    def insertTaskBulkDeps(self, taskList):
        """
        Insert a bunch of tasks in the queue previous analysis of its 
        dependencies (as expressed in its spec file), and possible modification
        of its 'reqs' field as a result of it.
        
        The 'taskList' must be a list of dicts as defined in
        TQComp.Apis.TQApiData.Task.
        """
        where = 0
        todel = []
        for task in taskList:
            try:
                validateTask(task)
                # Inspect deps
                inputFiles = self.__parseInputFiles(task['spec'])
                if inputFiles:
                    for file in inputFiles:
                        task['reqs'] += " and (('%s' in cache) or (QTIME()>10))" % (file)
            except ValueError, inst:
                self.logger.warning('%s' % inst)
                todel.insert(0,where)
            where += 1

        for i in todel:
            taskList.pop(i)
           
        self.transaction.begin()
        self.queries.addTasksBulk(taskList)
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
        #       it is told to kill the real job (on reply of next hbeat message)
        self.queries.updateTasks(taskIds, ['state'], [taskStates['Killed']])
        self.queries.archiveTasksById(taskIds)
        self.queries.removeTasksById(taskIds)
        self.transaction.commit()


    def __parseInputFiles(self, xmlFile):
        """
        Utility to parse the specified XML file looking for InputFile elements
        under CMSSWConfig/Source. It returns a list of the textual values of the 
        composing File elements. If the InputFile element is not found, None is 
        returned. If it is found, but empty, an empty list is returned.
        """
        files = []
        xmldoc = dom.parse(xmlFile)  

        anchor = xmldoc
        # The following would make a stricter (but slower and less flexible) 
        # element chain verification (worth it?)
#        try:
#            anchor = anchor.getElementsByTagName('CMSSWConfig')[0]
#            anchor = anchor.getElementsByTagName('Source')[0]
#        except Exception, inst:
#            messg = "Parsing invalid job spec file (%s): %s " % (xmlFile, inst)
#            self.logger.warning(messg)
#            return None

        try:
            anchor = anchor.getElementsByTagName('InputFiles')[0]
        except:
            return None
            
        for elem in anchor.getElementsByTagName('File'):
            files.append(elem.childNodes[0].nodeValue.strip())

        return files


