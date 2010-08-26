#!/usr/bin/env python
"""
Base handler for errorReport message.
"""
__all__ = []
__revision__ = "$Id: ErrorReportHandler.py,v 1.2 2009/09/29 12:23:03 delgadop Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMFactory import WMFactory

from traceback import extract_tb
import sys
import threading
from time import strftime, localtime

from TQComp.Constants import pilotLogUrlDir, uploadRoot

class ErrorReportHandler(object):
    """
    Handler for pilot's errorReport message.
    """
   
    def __init__(self, params = None):
        """
        Constructor. The params argument can be used as a dict for any
        parameter (if needed). Basic things can be obtained from currentThread.

        The required params are as follow:
           (none)
        """
        required = ['pilotErrorLogPath', "uploadBaseUrl"]
        for param in required:
            if not param in params:
                messg = "ErrorReportHandler object requires params['%s']" % param
                # TODO: What number?
                numb = 0
                raise WMException(messg, numb)
        self.pilotErrorLogPath = params["pilotErrorLogPath"]
        self.uploadBaseUrl = params["uploadBaseUrl"]

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
        
        # load queries for backend.
        myThread = threading.currentThread()
   
        # Now handle the message
        fields = {}
        if event in ['errorReport']:
   
            self.logger.debug('ErrorReportHandler:ErrorReport:payload: %s' % payload)
            
            # Extract message attributes
            required = ['pilotId', 'error', 'errorCode']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': "errorReport message requires \
'%s' field in payload" % param}
#                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

            pilotId = payload['pilotId']
            error = payload['error']
            errorCode = payload['errorCode']
            if 'taskId' in payload:
                taskId = payload['taskId']
            else:
                taskId = None


            try:
                myThread.transaction.begin()
              
                # Log the pilot error
                self.queries.logPilotEvent(pilotId, 'ErrorReport', error, \
                    taskId, errorCode)
                
                # Give the result back
                fields['errorLogUrl'] = self.__buildErrorLogUrl(pilotId)
                result = 'errorReportAck'
      
                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in ErrorReport, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass



    def __buildErrorLogUrl(self, pilotId):
#        path = thefile.replace(self.pilotErrorLogPath, reportUrlDir+'/')
#        path = path.replace(thefile.split('/')[-1], 'FrameworkJobReport.xml')
#        return 
        baseurl = self.uploadBaseUrl+'/'+uploadRoot+'/'+pilotLogUrlDir
        tstamp = strftime('%Y%m%d_%H%M%S', localtime())

        result = "%s/%s_%s.tgz" % (baseurl, pilotId, tstamp)
        return result
