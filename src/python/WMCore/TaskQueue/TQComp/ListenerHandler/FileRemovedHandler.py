#!/usr/bin/env python
"""
Base handler for fileRemoved.
"""
__all__ = []
__revision__ = "$Id: FileRemovedHandler.py,v 1.1 2009/06/01 09:57:09 delgadop Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMFactory import WMFactory


from traceback import extract_tb
import sys
import threading


class FileRemovedHandler(object):
    """
    Handler for pilot's fileRemoved message.
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
                messg = "FileRemovedHandler object requires params['%s']" % param
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
        if event in ['fileRemoved']:
   
            self.logger.debug('FileRemovedHandler:FileRemoved:payload: %s' % payload)
            
            try:
                myThread.transaction.begin()
              
                # Extract the pilot attributes
                required = ['pilotId', 'fileguid']
                for param in required:
                    if not param in payload:
                        result = 'Error'
                        fields = {'Error': 'fileRemoved message requires \
%s field in payload' % param}
                        myThread.transaction.rollback()
                        return {'msgType': result, 'payload': fields}

                pilotId = payload['pilotId']
                fileguid = payload['fileguid']

                # Get pilot info from DB (check that it's registered)
                res = self.queries.getPilotsWithFilter({'id': pilotId}, \
                                    ['id'], None, asDict = True)
                if not res:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

                # Delete the file from the hostdata association
                self.queries.removeFileHost(fileguid, pilotId)

                # Update last heartbeat
                self.queries.updatePilot(pilotId, {'last_heartbeat': None})

                # Give the result back
                fields['info'] = 'Done'
                result = 'fileRemovedAck'

                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in FileRemoved, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
    
