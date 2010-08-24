#!/usr/bin/env python
"""
Base handler for pilotShutdown.
"""
__all__ = []



from WMCore.WMFactory import WMFactory

from traceback import extract_tb
import sys
import threading

from TQComp.TQRoutines import finishPilot

class PilotShutdownHandler(object):
    """
    Handler for pilot's pilotShutdown message.
    """
   
    def __init__(self, params = None):
        """
        Constructor. The params argument can be used as a dict for any
        parameter (if needed). Basic things can be obtained from currentThread.

        The required params are as follow:
           (none)
        """
        required = []
        for param in required:
            if not param in params:
                messg = "PilotShutdownHandler object requires params['%s']" % param
                # TODO: What number?
                numb = 0
                raise WMException(messg, numb)

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
        if event in ['pilotShutdown']:
   
            self.logger.debug('PilotShutdownHandler:PilotShutdown:payload: %s' % payload)
            
            # Extract message attributes
            required = ['pilotId']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': "pilotShutdown message requires \
'%s' field in payload" % param}
                    return {'msgType': result, 'payload': fields}
            pilotId = payload['pilotId']
            if 'reason' in payload:
                reason = payload['reason']
            else:
                reason = None

            finalRes = finishPilot(self, myThread.transaction, pilotId, reason)
              
            return finalRes
   
        else:
            # unexpected message, scream?
            pass
    
    
