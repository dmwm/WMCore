#!/usr/bin/env python
"""
Base handler for addFile.
"""
__all__ = []
__revision__ = "$Id: AddFileHandler.py,v 1.2 2009/07/08 17:28:08 delgadop Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMFactory import WMFactory

from traceback import extract_tb
import sys
import threading


class AddFileHandler(object):
    """
    Handler for pilot's addFile message.
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
                messg = "AddFileHandler object requires params['%s']" % param
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
        if event in ['addFile']:
   
            self.logger.debug('AddFileHandler:AddFile:payload: %s' % payload)
            
            try:
                myThread.transaction.begin()
              
                # Message attributes
#                required = ['pilotId', 'fileguid', 'filesize', 'filetype']
                required = ['pilotId', 'fileList']
                for param in required:
                    if not param in payload:
                        result = 'Error'
                        fields = {'Error': "addFile message requires \
'%s' field in payload" % param}
                        myThread.transaction.rollback()
                        return {'msgType': result, 'payload': fields}
                pilotId= payload['pilotId']
                files = []
                for i in payload['fileList']:
                    file = {}
                    file['guid'] = i['fileGuid']
                    file['size'] = i['fileSize']
                    file['type'] = i['fileType']
#                    self.logger.debug('fileId: %s' % file['guid'])
                    files.append(file)

                # Get pilot info from DB (check that it's registered)
                res = self.queries.getPilotsWithFilter({'id': pilotId}, \
                                    ['id'], None, asDict = False)
                if not res:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}


                # Add the files to the DB
                # TODO: Is there a better way to do this in bulk mode?
                for file in files:

                    # Add file to data table (or retrieve it, if already there)
                    self.queries.addFile({'guid': file['guid'], 'type': \
                                         file['type'], 'size': file['size']})
                    self.logger.debug('addFile: %s' % file['guid'])
                    
                    # Register file with pilot's host (if not already there)
                    self.queries.addFileHost(pilotId, file['guid'])

                # Update last heartbeat
                self.queries.updatePilot(pilotId, {'last_heartbeat': None})

                # Give the result back
                fields['info'] = 'Done'
                result = 'addFileAck'
      
                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in AddFile, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
    
