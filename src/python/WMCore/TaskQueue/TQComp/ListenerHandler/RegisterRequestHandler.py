#!/usr/bin/env python
"""
Base handler for registerRequest.
"""
__all__ = []
__revision__ = "$Id: RegisterRequestHandler.py,v 1.3 2009/08/11 14:09:27 delgadop Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMFactory import WMFactory


from traceback import extract_tb
import sys
import threading


class RegisterRequestHandler(object):
    """
    Handler for pilot's registerRequest message.
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
                messg = "RegisterRequestHandler object requires params['%s']" % param
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
        if event in ['registerRequest']:
   
            self.logger.debug('RegisterRequestHandler:RegisterRequest:payload: %s' % payload)
            
            # Extract the pilot attributes
            required = ['host', 'se', 'cacheDir', 'ttl']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': 'registerRequest messsage requires \
%s field in payload' % param}
#                        myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}
            host = payload['host']
            se = payload['se']
            cacheDir = payload['cacheDir']
            ttl = payload['ttl']
            if 'filesystem' in payload:
                filesystem = payload['filesystem']

            try:
                myThread.transaction.begin()
              
                # Get the list of pilots in the same host
                pilotList = self.queries.getPilotsAtHost(host, se, True)
                self.logger.debug("PilotList: %s" % pilotList)

                # Register new pilot, retrieving assigned id
                pilotId = self.queries.addPilot({'host': host, 'se': se, \
                       'cacheDir': cacheDir, 'ttl': ttl, 'ttl_time': None})
                self.logger.debug("PilotId: %s" % pilotId)
                if (not pilotId) and (pilotId != 0):
                    result = 'Error'
                    fields = {'Error': 'Could not register pilot!'}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

                # Log in the tq_pilot_log table
                self.queries.logPilotEvent(pilotId, 'RegisterRequest', \
                  'Host: %s, SE: %s' % (host, se))

                # Give the result back
                fields['registerStatus'] = 'RegisterDone'
                fields['pilotId'] = pilotId 
                fields['otherPilots'] = pilotList
                result = 'registerResponse'
      
                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in RegisterRequest, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
    
