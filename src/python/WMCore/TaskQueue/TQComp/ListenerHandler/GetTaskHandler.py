#!/usr/bin/env python
"""
Base handler for getTask.
"""
__all__ = []
__revision__ = "$Id: GetTaskHandler.py,v 1.2 2009/04/30 09:00:23 delgadop Exp $"
__version__ = "$Revision: 1.2 $"

#from WMCore.Agent.BaseHandler import BaseHandler
#from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException
from traceback import extract_tb
import sys


from TQComp.Constants import sandboxUrlDir, specUrlDir, taskStates, staticRoot

import threading

class GetTaskHandler(object):
    """
    Handler for pilot's getTask message.
    """
 
    def __init__(self, params = None):
        """
        Constructor. The params argument can be used as a dict for any
        parameter (if needed). Basic things can be obtained from currentThread.

        The required params are as follow:
           downloadBaseUrl, sandboxBasePath, specBasePath
        """
        myThread = threading.currentThread()
        factory = WMFactory("default", \
                  "TQComp.Database."+myThread.dialect)
        self.queries = factory.loadObject("Queries")
        self.logger = myThread.logger

        required = ["downloadBaseUrl", "sandboxBasePath", "specBasePath"]
        for param in required:
            if not param in params:
                messg = "GetTaskHandler object requires params['%s']" % param
                # TODO: What number?
                numb = 0
                raise WMException(messg, numb)

        self.downloadBaseUrl = params["downloadBaseUrl"]
        self.sandboxBasePath = params["sandboxBasePath"]
        self.specBasePath = params["specBasePath"]


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
        if event in ['getTask']:
    
            self.logger.debug('GetTaskHandler:GetTask:payload: %s' % payload)
           
            try:
                # Extract the task attributes
                # Here we should check that all arguments are given correctly...
                pilotId = payload['pilotId']
                host = payload['host']
             
                # Select a task to assign to this pilot 
#                res = self.queries.getTaskAtState(taskStates['Queued'])[0].fetchone()
                res = self.queries.getTaskAtState(taskStates['Queued'])
#                self.logger.debug("From SELECT query: %s" % res)
             
                if not res:
                    result = 'NoTaskAvailable'
                    return {'msgType': result, 'payload': fields}
             
                [taskId, taskSpecFile, taskSandbox] = res[0]
#                taskId = res['id']
#                specFile = res['spec']
#                sandbox = res['sandbox']
#                messg = "taskId, taskSpecFile, taskSandbox:"
#                messg += " %s, %s, %s" %(taskId,taskSpecFile,taskSandbox)
#                self.logger.debug(messg)
             
                # Update task table with this assignment
                myThread.transaction.begin()
                vars = {'pilot': pilotId, 'state': taskStates['Assigned']}
                self.logger.debug("updateTask vars: %s" % (vars))
                self.queries.updateTask(taskId, vars)
                
                # Update pilot table with his host
                vars = {'host': host}
                self.logger.debug("updatePilot vars: %s" % (vars))
                self.queries.updatePilot(pilotId, vars)
             
                self.logger.info("Assigning task %s to pilot %s" % (taskId, pilotId))
                
                # Give the result back
                fields['TaskId'] = taskId
                fields['SandboxUrl'] = self.__buildSandboxUrl(taskSandbox)
                fields['SpecUrl'] = self.__buildSpecUrl(taskSpecFile)
                result = 'TaskAssigned'

                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Could not assign task, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}

        else:
            # unexpected message, scream?
            pass
       


    def __buildSpecUrl(self, specFile):
        path = specFile.replace(self.specBasePath, specUrlDir+'/')
        return self.downloadBaseUrl+'/'+staticRoot+'/'+path
        
    def __buildSandboxUrl(self, sandbox):
        path = sandbox.replace(self.sandboxBasePath, sandboxUrlDir+'/')
        return self.downloadBaseUrl+'/'+staticRoot+'/'+path
