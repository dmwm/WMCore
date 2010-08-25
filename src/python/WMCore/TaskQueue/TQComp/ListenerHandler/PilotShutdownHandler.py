#!/usr/bin/env python
"""
Base handler for pilotShutdown.
"""
__all__ = []
__revision__ = "$Id: PilotShutdownHandler.py,v 1.2 2009/07/08 17:28:08 delgadop Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMFactory import WMFactory

from traceback import extract_tb
import sys
import threading

# This is for the reschedule method
# TODO: If this is used by someone else, factorize to an external lib module
from TQComp.Constants import taskStates


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
            
            try:
                myThread.transaction.begin()
              
                # Extract message attributes
                required = ['pilotId']
                for param in required:
                    if not param in payload:
                        result = 'Error'
                        fields = {'Error': "pilotShutdown message requires \
'%s' field in payload" % param}
                        myThread.transaction.rollback()
                        return {'msgType': result, 'payload': fields}
                pilotId= payload['pilotId']
                if 'reason' in payload:
                    reason = payload['reason']

                # See if this was the last pilot on its own host
                # and if so, remove its cache from that host
                res = self.queries.countPilotMates(pilotId)
                self.logger.debug("countPilotMates: %s" % res)

                if res == 0:
                    result = 'Error'
                    fields = {'Error': 'Not registered pilot', \
                              'PilotId': pilotId}
                    myThread.transaction.rollback()
                    return {'msgType': result, 'payload': fields}

                if res == 1:
                    self.queries.removeFileHost(None, pilotId)

                # Now, if the data was not at other host,
                # remove it also from the tq_data table
                self.queries.removeLooseData()

                # Check if there are tasks with this pilot
                # and if so, run the procedure for rescheduling/abort
                res = self.queries.getTasksWithFilter( \
                          {'pilot': pilotId}, asDict = True)
                if res: 
                    self.rescheduleTasks(res)

                # Finally, delete the pilot from the DB
                self.queries.removePilot(pilotId)

#                self.logger.debug('fileId: %s' % fileId)
                
                # Give the result back
                fields['info'] = 'Done'
                result = 'pilotShutdownAck'
      
                # Commit
                myThread.transaction.commit()
              
            except:
                type, val, tb = sys.exc_info()
                myThread.transaction.rollback()
                messg = 'Error in PilotShutdown, due to: %s - %s '% (type, val)
                self.logger.warning(messg + "Trace: %s"% extract_tb(tb,limit=5))
                result = 'Error'
                fields = {'Error': messg}
              
            return {'msgType': result, 'payload': fields}
   
        else:
            # unexpected message, scream?
            pass
    
    # TODO: If this is used by someone else, factorize to an external lib module
    def rescheduleTasks(self, taskList):

        # Extract ids
        idList = map(lambda x: x['id'], taskList)
            
    # TODO: Need a more sophisticated logic for this?
    #       This is just changing state of all tasks to queued (never abort?)

        # Update all tasks to be queued
        self.queries.updateTasks(idList, ['state', 'pilot'], \
                                 [taskStates['Queued'], None])

