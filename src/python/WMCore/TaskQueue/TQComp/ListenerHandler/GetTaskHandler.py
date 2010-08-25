#!/usr/bin/env python
"""
Base handler for getTask.
"""
__all__ = []
__revision__ = "$Id: GetTaskHandler.py,v 1.5 2009/08/11 14:09:27 delgadop Exp $"
__version__ = "$Revision: 1.5 $"

#from WMCore.Agent.BaseHandler import BaseHandler
#from WMCore.ThreadPool.ThreadPool import ThreadPool
from WMCore.WMFactory import WMFactory
from WMCore.WMException import WMException

from TQComp.Constants import sandboxUrlDir, specUrlDir, taskStates, \
                             staticRoot, reportUrlDir, uploadRoot

from traceback import extract_tb
import sys
import threading


class GetTaskHandler(object):
    """
    Handler for pilot's getTask message.
    """
 
    def __init__(self, params = None):
        """
        Constructor. The 'params' argument can be used as a dict for any
        parameter (if needed). Basic things can be obtained from currentThread.

        The required params are as follow:
           downloadBaseUrl, sandboxBasePath, specBasePath, matcherPlugin
        """
        myThread = threading.currentThread()
#        factory = WMFactory("default", \
#                  "TQComp.Database."+myThread.dialect)
        factory = WMFactory("default")
#        self.queries = factory.loadObject("Queries")
        self.queries = factory.loadObject("TQComp.Database."\
                                  +myThread.dialect+".Queries")
        self.logger = myThread.logger

        required = ["downloadBaseUrl", "sandboxBasePath", "specBasePath",\
                    "uploadBaseUrl", "matcherPlugin", "maxThreads"]
        for param in required:
            if not param in params:
                messg = "GetTaskHandler object requires params['%s']" % param
                # TODO: What number?
                numb = 0
                raise WMException(messg, numb)

        self.downloadBaseUrl = params["downloadBaseUrl"]
        self.sandboxBasePath = params["sandboxBasePath"]
        self.specBasePath = params["specBasePath"]
        self.uploadBaseUrl = params["uploadBaseUrl"]
        self.maxThreads = params["maxThreads"]
        try:
            self.matcher = factory.loadObject(params["matcherPlugin"],\
                           {'queries': self.queries, 'logger': self.logger})
        except:
            ttype, val, tb = sys.exc_info()
#            myThread.transaction.rollback()
            messg = 'Could not load matcher plugin, due to: %s - %s '% (ttype, val)
            # TODO: What number?
            numb = 0
            raise WMException(messg, numb)


    # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload.
        """
        # load queries for backend.
        myThread = threading.currentThread()
    
        # Now handle the message
        fields = {}
        if event in ['getTask']:
    
            self.logger.debug('GetTaskHandler:GetTask:payload: %s' % payload)
           
            # Extract message attributes
            required = ['pilotId']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': "errorReport message requires \
'%s' field in payload" % param}
#                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

            pilotId = payload['pilotId']


            try:
                myThread.transaction.begin()

                # Get pilot info from DB (check that it's registered)
                res = self.queries.getPilotsWithFilter({'id': pilotId}, \
                                    ['host', 'se'], None, asDict = True)
                if not res:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

                # If the pilot did not pass required info, use DB values
                if not ('host' in payload):
                   payload['host'] = res[0]['host']
                if not ('se' in payload):
                   payload['se'] = res[0]['se']

                if not 'cache' in payload:
                   payload['cache'] = self.queries.getCacheAtHost(\
                                    payload['host'], payload['se'])
             
                # Update heartbeat of the pilot (in any case)
                vars = {'last_heartbeat': None}
#               self.logger.debug("updatePilot vars: %s" % (vars))
                self.queries.updatePilot(pilotId, vars)

                # Select a task to assign to this pilot 
                res = self.matcher.matchTask(payload, self.maxThreads)
#                self.logger.debug("From SELECT query: %s" % res)
             
                if not res:
                    result = 'NoTaskAvailable'
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

                # Commit
                myThread.transaction.commit()
              
            except:
                ttype, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Could not assign task, due to: %s - %s '% (ttype, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
                return {'msgType': result, 'payload': fields}

             
            # Now try with each of the returned tasks in order
            # Find one that has not been assigned yet
            taskId = None
            for t in res:
                taskId = t['id']
                taskSpecFile = t['spec']
                taskSandbox = t['sandbox']
                taskWkflow = t['wkflow']
#                    messg = "taskId, taskSpecFile, taskSandbox:"
#                    messg += " %s, %s, %s" %(taskId,taskSpecFile,taskSandbox)
#                    self.logger.debug(messg)
             
                try:
                    myThread.transaction.begin()

                    # Check & lock the state of the task (not yet running)
                    resLock = self.queries.lockTask(taskId)
                    if resLock[0]['state'] != taskStates['Queued']:
                        raise Exception('Task not in Queued state')
                    
                    # Update task table with this assignment
                    vars = {'pilot': pilotId, 'state': taskStates['Running']}
                    self.logger.debug("update task %s, vars: %s" %(taskId,vars))
                    self.queries.updateOneTask(taskId, vars)

                    # Commit
                    myThread.transaction.commit()
                    break
                  
                except:
                    ttype, val, tb = sys.exc_info()
                    myThread.transaction.rollback()
                    messg = 'Not assigning task %s to pilot %s, due to:' % \
                            (taskId, pilotId)
                    self.logger.warning('%s %s - %s'% (messg, ttype, val))
                    taskId = None



            # See if we found a task
            if not taskId:
                result = 'NoTaskAvailable'
                return {'msgType': result, 'payload': fields}

            # Take note of assignment in the logs
            try:
                myThread.transaction.begin()
                
                # Log in the tq_pilot_log table
                self.queries.logPilotEvent(pilotId, 'GetTask', None, taskId)
                self.logger.info("Assigning task %s to pilot %s" % (taskId, pilotId))

                # Commit
                myThread.transaction.commit()
              
            except:
                ttype, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error when writing pilot %s log' % (pilotId)
                self.logger.warning('%s %s - %s'% (messg, ttype, val))
               
            # Give the result back
            fields['taskId'] = taskId
            fields['sandboxUrl'] = self.__buildSandboxUrl(taskSandbox)
            fields['specUrl'] = self.__buildSpecUrl(taskSpecFile)
            fields['workflowType'] = taskWkflow
            # (By now) store report in the same dir as spec (like PA)
            reportUrl = self.__buildReportUrl(taskSpecFile)
            fields['reportUrl'] = reportUrl 
            result = 'TaskAssigned'

              
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
        
    def __buildReportUrl(self, thefile):
        # TODO: maybe need to change the file name/location for error reports?
        path = thefile.replace(self.specBasePath, reportUrlDir+'/')
        path = path.replace(thefile.split('/')[-1], 'FrameworkJobReport.xml')
        return self.uploadBaseUrl+'/'+uploadRoot+'/'+path


#    def __doTheMatching(self, ):
        
