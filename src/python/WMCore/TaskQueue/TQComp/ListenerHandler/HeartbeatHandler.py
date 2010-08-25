#!/usr/bin/env python
"""
Base handler for heartbeat.
"""
__all__ = []



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
            
            # Message attributes
            required = ['pilotId', 'ttl']
            for param in required:
                if not param in payload:
                    result = 'Error'
                    fields = {'Error': "heartbeat message requires \
'%s' field in payload" % param}
#                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}
            pilotId = payload['pilotId']
            ttl = payload['ttl']

            # Look for extra optional parameters 

            # Cache Update: Just try to add all known files (not delete 
            # any). If known, it will be skipped.           
            if 'cache' in payload:
                files = []
                guids = []
                try:
                    for i in payload['cache']:
                        file = {}
                        file['guid'] = i['fileGuid']
                        file['size'] = i['fileSize']
                        file['type'] = i['fileType']
    #                        self.logger.debug('fileId: %s' % file['guid'])
                        files.append(file)
                        guids.append({'guid': i['fileGuid']})
                except:
                    messg = "Malformed cache. Each file requires:"
                    messg += "'fileGuid', 'fileSize', 'fileType'"
                    return {'msgType': 'Error', 'payload': {'Error': messg}}


            # Any other optional field?

            try:
                myThread.transaction.begin()
              
                # Get pilot info from DB (check that it's registered)
#                res = self.queries.getPilotsWithFilter({'id': pilotId}, \
#                                    ['id'], None, asDict = False)
                res = self.queries.selectWithFilter('tq_pilots', \
                   {'id': pilotId}, ['id', 'host', 'se'], None, asDict = True)
                if not res:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}
                host = res[0]['host']
                se = res[0]['se']


                # Update TTL, TTL time, and last heartbeat values
                self.queries.updatePilot(pilotId, {'ttl': ttl, \
                         'ttl_time': None,   'last_heartbeat': None})
               
                # Cache Update: Just try to add all known files (not delete 
                # any). If known, it will be skipped.           
                if 'cache' in payload:

                    # Add the files
                    self.logger.debug('addFilesBulk: %s' % files)
                    self.queries.addFilesBulk(files)
                        
# TODO: This will go away when we move to cache per host
#       Instead we get the commented code below
                    # Register files with pilot (if not already there)
                    self.queries.addFilePilotBulk(pilotId, guids)

#                    # Register files with pilot's host (if not already there)
#                    self.queries.addFileHostBulk(pilotId, guids)
# TODO: End of cache per host
                

                # Get the list of pilots in the same host for the response
                pilotList = self.queries.getPilotsAtHost(host, se, True)
                for i in pilotList:
                    if i['pilotid'] == pilotId: 
                        pilotList.remove(i)
                        break

                self.logger.debug("PilotList: %s" % pilotList)

                # Give the result back, including the list of pilots in the host
                fields['info'] = 'Done'
                fields['otherPilots'] = pilotList
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
    
