#!/usr/bin/env python
"""
Base handler for getTask.
"""
__all__ = []
__revision__ = "$Id: GetTaskHandler.py,v 1.4 2009/07/08 17:28:08 delgadop Exp $"
__version__ = "$Revision: 1.4 $"

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
                    "uploadBaseUrl", "matcherPlugin"]
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
        try:
            self.matcher = factory.loadObject(params["matcherPlugin"],\
                           {'queries': self.queries, 'logger': self.logger})
        except:
            ttype, val, tb = sys.exc_info()
            myThread.transaction.rollback()
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
           
            try:
                myThread.transaction.begin()

                # Extract the task attributes
                # Here we should check that all arguments are given correctly...
                pilotId = payload['pilotId']

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
             
                # Select a task to assign to this pilot 
#                self.logger.debug("From SELECT query: %s" % res)
#                res = self.queries.getTaskAtState(taskStates['Queued'])
                res = self.matcher.matchTask(payload)
             
                if not res:
                    result = 'NoTaskAvailable'
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}
             
                taskId = res['id']
                taskSpecFile = res['spec']
                taskSandbox = res['sandbox']
                taskWkflow = res['wkflow']
#                messg = "taskId, taskSpecFile, taskSandbox:"
#                messg += " %s, %s, %s" %(taskId,taskSpecFile,taskSandbox)
#                self.logger.debug(messg)
             
                # Update task table with this assignment
                vars = {'pilot': pilotId, 'state': taskStates['Running']}
#                vars = {'pilot': pilotId, 'state': taskStates['Assigned']}
                self.logger.debug("updateOneTask vars: %s" % (vars))
                self.queries.updateOneTask(taskId, vars)
               
                # In principle, pilot should have registered before 
                # (so no host update should occur here)
                # but we update the hearbeat
#                  # Update pilot table with his host
#                vars = {'host': host}
                vars = {'last_heartbeat': None}
#                self.logger.debug("updatePilot vars: %s" % (vars))
                self.queries.updatePilot(pilotId, vars)
             
                self.logger.info("Assigning task %s to pilot %s" % (taskId, pilotId))
                
                # Give the result back
                fields['taskId'] = taskId
                fields['sandboxUrl'] = self.__buildSandboxUrl(taskSandbox)
                fields['specUrl'] = self.__buildSpecUrl(taskSpecFile)
                fields['workflowType'] = taskWkflow
                # (By now) store report in the same dir as spec (like PA)
                reportUrl = self.__buildReportUrl(taskSpecFile)
                fields['reportUrl'] = reportUrl 
                result = 'TaskAssigned'

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
        
