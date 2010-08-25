#!/usr/bin/env python
"""
API to insert new tasks into the TQ queue.

It inherits the ability to connect to the TQ database 
from TQComp.Apis.TQApi.
"""

__all__ = []
__revision__ = "$Id: TQSubmitApi.py,v 1.1 2009/04/27 07:52:26 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading
import time
import xml.dom.minidom

from TQComp.Apis.TQApi import TQApi
from TQComp.Apis.TQApiData import validateTask


class TQSubmitApi(TQApi):
    """
    API to insert new tasks into the TQ queue.

    Example to insert a task using this class, from a WMCore component.

      myThread = threading.currentThread()
      tqApi = TQSubmitApi(myThread.logger, self.config, myThread.transaction)

      spec = '/pool/TaskQueue/playground/specs/some-sec-file.xml'
      sandbox = '/pool/TaskQueue/playground/sandbox/some-sandbox.tar.gz'
      mydict = {'spec': spec, 'wkflow': 'TEST' , 'type': 5, 'sandbox': sandbox}
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
        Insert a bunch of task.
        The 'taskList' must be a list of dicts as defined in
        TQComp.Apis.TQApiData.Task.
        """
        where = 0
        todel = []
        for task in taskList:
            try:
                validateTask()
            except ValueError, inst:
                self.logger.warning('%s' % inst)
                todel.insert(0,where)
            where += 1

        for i in todel:
            taskList.pop(i)
           
        self.transaction.begin()
        self.queries.addBulk(taskList)
        self.transaction.commit()

    
    def insertTask(self, task):
        """
        Insert a task.
        The 'task' must be a dict as defined in TQComp.Apis.TQApiData.Task.
        """

#        self.logger.debug('Inserting task: %s, %s, %s, %s' %\
#                     (spec, sandbox, wkflow, type))

        # This may raise a ValueError exception if not compliant
        validateTask(task)
            
        # Insert job and its characteristics into the database
        self.transaction.begin()
        self.queries.add(task)
        self.transaction.commit()

