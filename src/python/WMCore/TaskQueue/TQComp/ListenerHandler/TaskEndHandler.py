#!/usr/bin/env python
"""
Base handler for taskEnd.
"""
__all__ = []
__revision__ = "$Id: TaskEndHandler.py,v 1.1 2009/04/27 07:52:27 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

#from WMCore.Agent.BaseHandler import BaseHandler
#from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.WMFactory import WMFactory

from TQComp.CommonUtil import buildXmlResponse                             
from TQComp.Constants import taskStates
from traceback import extract_tb
import sys

import threading

class TaskEndHandler(object):
    """
    Handler for pilot's taskEnd message.
    """
   
    def __init__(self, params = None):
        """
        Constructor. The params argument can be used as a dict for any
        parameter (if needed). Basic things can be obtained from currentThread.
        """
        myThread = threading.currentThread()
        factory = WMFactory("default", \
                  "TQComp.Database."+myThread.dialect)
        self.queries = factory.loadObject("Queries")
        self.logger = myThread.logger
    
    
    # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload.
        """
#        # as we defined a threadpool we can enqueue our item
#        # and move to the next.
#        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload})
        
        # load queries for backend.
        myThread = threading.currentThread()
   
        # Now handle the message
        fields = {}
        if event in ['taskEnd']:
   
            self.logger.debug('TaskEndHandler:TaskEnd:payload: %s' % payload)
            
            try:
                # Extract the task attributes
                # Here we should check that all arguments are given correctly...
                pilotId = payload['pilotId']
                taskId = payload['taskId']
              
                # Mark the task as done in the DB
                myThread.transaction.begin()
#                vars = {'state': taskStates['Done'], 'pilot': None}
                vars = {'state': taskStates['Done']}
                self.queries.updateTask(taskId, vars)
                self.logger.debug("Task updated as Done.")
              
                # Commit
                myThread.transaction.commit()
              
                # Give the result back
                fields['TaskId'] = taskId
                fields['Info'] = 'Task updated as Done.'
                result = 'TaskEndACK'
      
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in TaskEnd, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
        
