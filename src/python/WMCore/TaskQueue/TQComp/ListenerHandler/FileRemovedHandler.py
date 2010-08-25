#!/usr/bin/env python
"""
Base handler for fileRemoved.
"""
__all__ = []
__revision__ = "$Id: FileRemovedHandler.py,v 1.5 2009/09/29 14:22:46 delgadop Exp $"
__version__ = "$Revision: 1.5 $"

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
            
            # Extract the pilot attributes
#                required = ['pilotId', 'fileguid']
            required = ['pilotId', 'fileList']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': 'fileRemoved message requires \
%s field in payload' % param}
#                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

            pilotId = payload['pilotId']
            files = payload['fileList']

            try:
                myThread.transaction.begin()
              
                # Get pilot info from DB (check that it's registered)
#                res = self.queries.getPilotsWithFilter({'id': pilotId}, \
#                                    ['id'], None, asDict = True)
                res = self.queries.selectWithFilter('tq_pilots', \
                            {'id': pilotId}, ['id'], None, asDict = True)
                if not res:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

                # Remove the files from the DB
                for guid in files:
                    if not (isinstance(guid, str) or isinstance(guid, unicode)): 
                        self.logger.debug('Type of guid: %s' % type(guid))
                        result = 'Error'
                        msg = 'Members of "fileList" should be strings (GUIDs)'
                        fields = {'Error': msg}
                        myThread.transaction.rollback()
                        return {'msgType': result, 'payload': fields}

                for guid in files:
                    # TODO: Is there a better way to do this in bulk mode?
                    # Delete the file from the hostdata association
                    self.queries.removeFileHost(guid, pilotId)

                # Now, if the data was not at other host,
                # remove it also from the tq_data table
                self.queries.removeLooseData()


                # Update last heartbeat
                self.queries.updatePilot(pilotId, {'last_heartbeat': None})

                # Give the result back
                fields['info'] = 'Done'
                result = 'fileRemovedAck'

                # Commit
                myThread.transaction.commit()
              
            except:
                etype, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in FileRemoved, due to: %s - %s '% (etype, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
    
