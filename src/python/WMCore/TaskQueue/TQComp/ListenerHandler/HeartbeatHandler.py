#!/usr/bin/env python
"""
Base handler for heartbeat.
"""
__all__ = []
__revision__ = "$Id: HeartbeatHandler.py,v 1.1 2009/06/01 09:57:09 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMFactory import WMFactory

from traceback import extract_tb
import sys
import threading


class HeartbeatHandler(object):
    """
    Handler for pilot's heartbeat message.
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
                messg = "HeartbeatHandler object requires params['%s']" % param
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
        if event in ['heartbeat']:
   
            self.logger.debug('HeartbeatHandler:Heartbeat:payload: %s' % payload)
            
            try:
                myThread.transaction.begin()
              
                # Message attributes
                required = ['pilotId', 'ttl']
                for param in required:
                    if not param in payload:
                        result = 'Error'
                        fields = {'Error': "heartbeat message requires \
'%s' field in payload" % param}
                        myThread.transaction.rollback()
                        return {'msgType': result, 'payload': fields}
                pilotId = payload['pilotId']
                ttl = payload['ttl']


                # Get pilot info from DB (check that it's registered)
                res = self.queries.getPilotsWithFilter({'id': pilotId}, \
                                    ['id'], None, asDict = False)
                if not res:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}


                # Update TTL, TTL time, and last heartbeat values
                self.queries.updatePilot(pilotId, {'ttl': ttl, \
                         'ttl_time': None,   'last_heartbeat': None})
               
                # Look for extra optional parameters 

                # cacheUpdate
                if 'cacheUpdate' in payload:
                    # TODO: Update cache if required based on this info
                    pass

                # others?

                # Give the result back
                fields['info'] = 'Done'
                result = 'heartbeatAck'
      
                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in Heartbeat, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
    
